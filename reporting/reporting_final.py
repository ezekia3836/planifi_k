from .adaptiveChunck import AdaptiveChunkManager
from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
from config.konticrea import connect_kit
from datetime import datetime,date,timedelta
import time, math, requests, psutil
from sqlalchemy import text
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import logging
import os
import gc

BATCH_ADV_SIZE = 5000
BATCH_CONTACTS = 2_000
BATCH_INSERT = 5_000
MAX_HTTP_WORKERS = 8

AGE_BINS = [0, 18, 25, 35, 45, 55, 65, 75, float("inf")]
AGE_LABELS = ["0-18", "18-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75+"]
NEED_COLS = [
    "database_id","id_routers","id_focus","adv_id","dwh_id",
    "segmentId","subject","event_type","date_event","tag_id",
    "brand","client_id","ListId","ListName","affiliate_id","ca","comment",
    "bounces","clickers","clicks","complaints","openers",
    "opens","sends","unsubs","date_schedule"
]
GROUP_COLS = [
    "database_id","adv_id","id_routers","segmentId","tag_id","date_event"    
]

COLUMNS_FINAL = [
    "database_id", "dwh_id",
    "country", "segmentId", "subject", "brand", "tag_id",
    "adv_id", "id_routers","id_focus", "affiliate_id", "ListId","ListName", "zipcode", "dep",
    "sends", "opens", "openers", "clicks", "clickers", "unsubs","complaints",
    "bounces","age_range", "gender", "main_isp", "age_gender_isp", "ca",
    "date_schedule", "date_event", "optimized","comment","date_schedule_max", "updated_at",
]
INT_COLS = [
    "database_id", "segmentId", "tag_id", "adv_id", "sends", "opens",
    "openers", "clicks", "clickers", "unsubs","complaints","bounces","ListId",
    "affiliate_id", "country","id_focus"
]
STR_COLS = [
    "id_routers", "age_range", "gender", "main_isp",
    "optimized", "subject", "zipcode", "dep","comment"
]
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("reporting")
class reporting:
    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.pg = PgConfig().get_client()
        self.table = "prod_reporting"
        self.adaptive_chunk = AdaptiveChunkManager()
        self.batch_adv_size = BATCH_ADV_SIZE
        self.konticrea = connect_kit()
        self.cache_contacts = {}
        self.ktk_id_cache = {}
        self.optimize_cache = {}

    def resilient_call(self, func, *args, max_retry=5, sleep_sec=5, backoff=True, **kwargs):
        attempt, wait = 1, sleep_sec
        while attempt <= max_retry:
            try:
                return func(*args, **kwargs)
            except (requests.ConnectionError, requests.Timeout, Exception) as e:
                print(f"Tentative {attempt}/{max_retry} échouée : {e}")
                self.notifier_erreur(f"Tentative {attempt}/{max_retry} échouée : {e}")
                if attempt == max_retry:
                    raise
                time.sleep(wait)
                if backoff:
                    wait *= 2
                attempt += 1

    def notifier_info(self, message):   
        logger.info(message)
    
    def notifier_erreur(self, message): 
        logger.error(message)

    def safe(self, value):
        try:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return 0
            f = float(value)
            return int(f) if math.isfinite(f) else 0
        except (ValueError, TypeError):
            return 0

    def clean_adv_ids(self, adv_ids):
        return list({str(x).strip() for x in adv_ids if str(x).strip().isdigit()})

    def recupere_pg(self, date_start, date_end, batch=1000):

        query = text(f"""
            SELECT vd.id AS id_focus,
                pa.caeur AS ca,
                vd.comment,
                COALESCE(json_agg(DISTINCT vd.date_shedule), '[]'::json) AS date_schedule,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('idsendout', idsendouts.idsendout, 'is_direct', idsendouts.is_direct)),
                    '[]'::json
                ) AS id_routers
            FROM visu.v2_data vd
            JOIN visu.v2_status st ON st.id = vd.status
            LEFT JOIN visu.v2_payoutinfo pa ON pa.id_data = vd.id
            LEFT JOIN LATERAL (
                SELECT vd1.idsendout, vd1.is_direct FROM (
                    SELECT vd.idsendout, TRUE AS is_direct
                    UNION
                    SELECT vd2.idsendout, FALSE AS is_direct
                    FROM visu.v2_data vd2
                    WHERE vd2.id = ANY (
                        SELECT vdr2.id_reuse FROM visu.v2_data_reuse vdr2
                        WHERE vdr2.id_v2 = vd.id
                    )
                    AND vd2.idsendout IS NOT NULL
                ) AS vd1
            ) AS idsendouts ON TRUE
            WHERE st.id = 5
            AND vd.date_shedule BETWEEN '{date_start}' AND '{date_end}'
            GROUP BY vd.id, pa.caeur, vd.comment
        """)

        pg_map = {}

        try:
            with self.pg.connect() as conn:
                result = conn.execution_options(stream_results=True).execute(query)

                while True:
                    rows = result.fetchmany(batch)
                    if not rows:
                        break

                    for row in rows:
                        id_focus, ca, comment, date_schedule, id_routers_list = row

                        if not id_routers_list:
                            continue

                        for item in id_routers_list:
                            if not isinstance(item, dict):
                                continue

                            id_r = item.get("idsendout")
                            is_direct = item.get("is_direct", False)

                            if id_r is None:
                                continue

                            key = str(id_r)
                            existing = pg_map.get(key)
                            if existing is None or (is_direct and not existing.get("is_direct")):
                                pg_map[key] = {
                                    "id_focus": id_focus,
                                    "ca": ca,
                                    "comment": comment,
                                    "date_schedule": date_schedule or [],
                                    "is_direct": is_direct,
                                }

        except Exception as e:
            self.notifier_erreur(f"Erreur Focus : {e}")
            return {}

        return pg_map

    def recupere_events(self, id_routers_focus, date_start, date_end):

        if not id_routers_focus:
            return

        query = f"""
       SELECT
        database_id,
        MessageId AS id_routers,
        adv_id,
        dwh_id,
        SegmentId AS segmentId,
        MessageSubject AS subject,
        event_type,
        Date AS date_event,
        tag AS tag_id,
        brand,
        client_id,
        ListId,
        ListName,
        affiliate_id
    FROM prod_events
    WHERE
        MessageId IN ({",".join(map(str, id_routers_focus))})
        AND adv_id != 0
        AND Date BETWEEN '{date_start}' AND '{date_end}'
        """
        try:
            r = self.resilient_call(self.clk.query, query)
            for row in r.result_rows:
                yield dict(zip(r.column_names, row))

        except Exception as e:
            self.notifier_erreur(f"Erreur events: {e}")
   
    def recupere_contacts(self, dwh_ids, batch_size=BATCH_CONTACTS):
        if not dwh_ids:
            return {}
        contacts_map = {}
        for i in range(0, len(dwh_ids), batch_size):
            batch_str = ",".join(f"'{x}'" for x in dwh_ids[i:i + batch_size])
            if not batch_str:
                continue
            query = f"""
                SELECT dwh_id,
                    argMax(age, updated_at) AS age,
                    argMax(gender, updated_at) AS gender,
                    argMax(main_isp, updated_at) AS main_isp,
                    argMax(zipcode, updated_at) AS zipcode,
                    argMax(dep,  updated_at) AS dep
                FROM prod_contacts
                WHERE dwh_id IN ({batch_str})
                GROUP BY dwh_id
            """
            try:
                r = self.resilient_call(self.clk.query, query)
                for row in r.result_rows:
                    contacts_map[str(row[0])] = dict(zip(r.column_names[1:], row[1:]))
            except Exception as e:
                self.notifier_erreur(f"Erreur contacts batch : {e}")
        return contacts_map
    
    def get_contacts_cached(self, dwh_ids):
        missing_dwh_ids = [dwh_id for dwh_id in dwh_ids if dwh_id not in self.cache_contacts]
        if missing_dwh_ids:
            try:
                new_contacts = self.recupere_contacts(missing_dwh_ids)
                for k, v in new_contacts.items():
                    self.cache_contacts[k] = v if isinstance(v, dict) else {"age": None, "gender": "O_gender", "main_isp": "O_isp", "zipcode": "zipcode_vide", "dep": "dep_vide"}
            except Exception as e:
                self.notifier_erreur(f"Erreur lors de la récupération des contacts : {e}")
        return {
            k: self.cache_contacts.get(k) if isinstance(self.cache_contacts.get(k), dict) else {}
            for k in dwh_ids
        }
   
    def recupere_ktk_id(self, database_ids):
        if not database_ids:
            return {}
        try:
            ids   = ",".join(str(x) for x in database_ids)
            if not ids:
                return {}
            query = f"""
                SELECT id AS database_id, ktk_id, basename, country
                FROM databases
                WHERE id IN ({ids})
            """
            r = self.clk.query(query)
            return {
                str(row[0]): {
                    "ktk_id":   row[1] or "ktk_vide",
                    "basename": row[2] or "base_vide",
                    "country":  row[3] or 0,
                }
                for row in r.result_rows
            }
        except Exception as e:
            self.notifier_erreur(f"Erreur ktk_id : {e}")
            return {}

    def get_ktk_id_cached(self, database_ids):
        missing_db_ids = [db_id for db_id in database_ids if db_id not in self.ktk_id_cache]
        if missing_db_ids:
            try:
                new_mappings = self.recupere_ktk_id(missing_db_ids)
                for k, v in new_mappings.items():
                    self.ktk_id_cache[k] = v if isinstance(v, dict) else {"ktk_id": v, "basename": "base_vide", "country": 0}   
            except Exception as e:
                self.notifier_erreur(f"Erreur lors de la récupération des ktk_id : {e}")
        return {
            k: self.ktk_id_cache.get(k) if isinstance(self.ktk_id_cache.get(k), dict) else {}
            for k in database_ids
        }
    
    def recuper_optimize(self, rows_list, chunck=100):
        if not self.konticrea:
            return {}
        cursor = self.konticrea.cursor()
        keys = list({
            (str(self.safe(r.get("id_focus"))),
            str(self.safe(r.get("ktk_id"))))
            for r in rows_list
            if self.safe(r.get("id_focus")) and self.safe(r.get("ktk_id"))})
        optimized_map = {k: "optimize_vide" for k in keys}
        if not keys:
            return optimized_map
        for i in range(0, len(keys), chunck):
            batch = keys[i:i + chunck]
            if not batch:
                continue
            values = ",".join(f"('{f}','{k}')" for f, k in batch)
            query = f"""
                SELECT focus_id, base_id, optimized
                FROM creativities
                WHERE (focus_id, base_id) IN ({values})
            """
            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                for focus_id,base_id,optimized in rows:
                    key=(str(focus_id),str(base_id))
                    if key in optimized_map and optimized:
                        optimized_map[key]=optimized
            except Exception as e:
                self.notifier_erreur(f"Erreur optimize chunk {i // chunck + 1} : {e}")
        cursor.close()
        return optimized_map
    
    def get_optimize_cached(self, rows_list):
        keys = list({
            (str(self.safe(r.get("id_focus"))),
             str(self.safe(r.get("ktk_id"))))
            for r in rows_list
            if self.safe(r.get("id_focus")) and self.safe(r.get("ktk_id"))
        })
        missing = [k for k in keys if k not in self.optimize_cache]
        if missing:
            try:
                new_map = self.resilient_call(
                    self.recuper_optimize,
                    [{"id_focus": k[0], "ktk_id": k[1]} for k in missing]
                ) or {}
                for k, v in new_map.items():
                    self.optimize_cache[k] = v if isinstance(v, str) else "optimized_vide"
            except Exception as e:
                logger.warning(f"Optimize indisponible: {e}")

        return {k: self.optimize_cache.get(k, "optimized_vide") for k in keys} 
    
    def get_month_ranges(self, start_month, end_month, year):
        start = date(year, start_month, 1)
        end = date(year, end_month, 1)

        ranges = []
        current = start

        while current <= end:
            month_start = current
            month_end = (current + relativedelta(months=1)) - timedelta(days=1)

            ranges.append((
                month_start.strftime("%Y-%m-%d"),
                month_end.strftime("%Y-%m-%d"),
                month_start.strftime("%Y%m")
            ))

            current += relativedelta(months=1)

        return ranges
    
    def clean_cache(self,max_size=500_000):
        if len(self.cache_contacts) > max_size:
            self.cache_contacts.clear()
        if len(self.ktk_id_cache) > max_size:
            self.ktk_id_cache.clear()
        if len(self.optimize_cache) > max_size:
            self.optimize_cache.clear()
   
    def report(self):
        self.clean_cache()
        def check_initialization():
            file = "etat.txt"
            if not os.path.exists(file):
                return False
            try:
                with open(file, "r") as f:
                    return "initialized=1" in f.read().strip()
            except Exception as e:
                logger.error(f"Erreur lecture état : {e}")
                return False

        def chunk_list(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i:i + size]

        initialized = check_initialization()
        n_months = 2 if initialized else 3
        logger.info(f"Lancement {'régulier' if initialized else 'initial'} — {n_months} mois")
        months = self.get_month_ranges(9,12,2025)
        success = True
        try:
            for date_start, date_end, partition in months:
                logger.info(f"Traitement {date_start} à {date_end} (partition {partition})")
                if initialized:
                    try:
                        query = f"ALTER TABLE {self.table} DROP PARTITION IF EXISTS {partition}"
                        self.clk.query(query)
                        logger.info(f"Partition supprimée : {partition}")
                    except Exception as e:
                        logger.warning(f"Erreur DROP partition {partition} : {e}")
                data = self.recupere_pg(date_start, date_end)
                if not data:
                    logger.warning(f"Pas de données pour {partition}")
                    continue
                focus_map = {str(k): v for k, v in data.items()}
                router_ids = list(focus_map.keys())
                rows=[]
                for id_chunk in chunk_list(router_ids, 20):
                        for row in self.recupere_events(id_chunk, date_start, date_end):
                            focus = focus_map.get(str(row["id_routers"]))
                            if not focus:
                                continue
                            merged = {**row, **focus}
                            rows.append(merged)
                            if len(rows) >= 50_000:
                                self._process_batch(rows)
                                self.clean_cache(max_size=300_000)
                                rows.clear()
                                gc.collect()
                if rows:
                    self._process_batch(rows)
                    rows.clear()
                    gc.collect()
                del focus_map, router_ids, data
                gc.collect()
        except Exception as e:
            success = False
            logger.error(f"Erreur pipeline : {e}", exc_info=True)
        if success and not initialized:
            try:
                with open("etat.txt", "w") as f:
                    f.write("initialized=1")
                logger.info("État initialisé")
            except Exception as e:
                logger.error(f"Erreur écriture état : {e}")
    
    def _process_batch(self, rows_batch, database_id=None):

        if len(rows_batch) > 50_000:
            logger.warning(f"Batch trop gros: {len(rows_batch)}")
            return
        df = pd.DataFrame(rows_batch, columns=NEED_COLS)
        if df.empty:
            return

        if database_id is not None:
            df = df[df["database_id"] == database_id]

        if df.empty:
            return
        df.to_csv("rows.csv",index=False,sep=';')
        df["dwh_id"] = df["dwh_id"].astype("string").fillna("")
        df["database_id"] = df["database_id"].astype(str)
        event = df["event_type"].to_numpy()
        df["sends"] = (event == "Sends").astype("int8")
        df["opens"] = (event == "Opens").astype("int8")
        df["clicks"] = (event == "Clicks").astype("int8")
        df["unsubs"] = (event == "Removals").astype("int8")
        df["complaints"] = (event == "Complaints").astype("int8")
        df["bounces"] = (event == "Bounces").astype("int8")
        key_cols = ["adv_id", "id_routers", "dwh_id"]

        df["clickers"] = 0
        mask = event == "Clicks"
        if mask.any():
            df.loc[mask, "clickers"] = (~df.loc[mask].duplicated(key_cols)).astype("int8")

        df["openers"] = 0
        mask = event == "Opens"
        if mask.any():
            df.loc[mask, "openers"] = (~df.loc[mask].duplicated(key_cols)).astype("int8")

        dwh_ids = df["dwh_id"].unique().tolist()
        contacts_map = self.get_contacts_cached(dwh_ids)

        df["age"] = df["dwh_id"].map(lambda x: contacts_map.get(x, {}).get("age"))
        df["gender"] = df["dwh_id"].map(lambda x: contacts_map.get(x, {}).get("gender"))
        df["main_isp"] = df["dwh_id"].map(lambda x: contacts_map.get(x, {}).get("main_isp"))
        df["zipcode"] = df["dwh_id"].map(lambda x: contacts_map.get(x, {}).get("zipcode"))
        df["dep"] = df["dwh_id"].map(lambda x: contacts_map.get(x, {}).get("dep"))
        df["gender"] = df["gender"].fillna("O_gender").replace({"O": "O_gender"})
        df["main_isp"] = df["main_isp"].fillna("O_isp").replace({"Other": "O_isp"})
        df["zipcode"] = df["zipcode"].fillna("zipcode_vide")
        df["dep"] = df["dep"].fillna("dep_vide")

        age = pd.to_numeric(df["age"], errors="coerce")

        df["age_range"] = np.select(
            [
                age < 18,
                age < 25,
                age < 35,
                age < 45,
                age < 55,
                age < 65,
                age < 75,
            ],
            AGE_LABELS[:-1],
            default="75+"
        )

        df["age_gender_isp"] = (
            df["age_range"].astype(str) + "_" +
            df["gender"].astype(str) + "_" +
            df["main_isp"].astype(str)
        )

        database_ids = df["database_id"].unique().tolist()
        db_map = self.get_ktk_id_cached(database_ids)

        df["ktk_id"] = df["database_id"].map(lambda x: db_map.get(x, {}).get("ktk_id"))
        df["basename"] = df["database_id"].map(lambda x: db_map.get(x, {}).get("basename"))
        df["country"] = df["database_id"].map(lambda x: db_map.get(x, {}).get("country"))

        df["ktk_id"] = df["ktk_id"].fillna("ktk_vide")
        df["basename"] = df["basename"].fillna("base_vide")
        df["country"] = df["country"].fillna(0)

        df["optimized"] = "optimized_vide"

        try:
            optimize_params = df[["id_focus", "ktk_id"]].drop_duplicates()

            optimized_map = self.get_optimize_cached(
                optimize_params.to_dict("records")
            ) or {}

            df["optimized"] = [
                optimized_map.get((str(f), str(k)), "optimized_vide")
                for f, k in zip(df["id_focus"], df["ktk_id"])
            ]

        except Exception as e:
            logger.warning(f"Optimize indisponible: {e}")

        if "date_schedule" not in df.columns:
            df["date_schedule"] = [[] for _ in range(len(df))]

        df["date_event"] = pd.to_datetime(df["date_event"], errors="coerce")

        df["date_schedule_max"] = df["date_schedule"].map(
                lambda x: max(x) if isinstance(x, list) and x else None
            )
        df["date_schedule_max"] = pd.to_datetime(df["date_schedule_max"], errors="coerce")
        df["updated_at"] = datetime.now()
        for col in COLUMNS_FINAL:
            if col not in df.columns:
                df[col] = None

        df = df[COLUMNS_FINAL]

        for col in INT_COLS:
            if col in df:
                df[col] = (
                    pd.to_numeric(df[col], errors="coerce")
                    .fillna(0)
                    .astype(np.int32)
                )

        for col in STR_COLS:
            if col in df:
                df[col] = df[col].astype("string").fillna("")

        df["dwh_id"] = df["dwh_id"].astype("string")
        df["brand"] = df["brand"].astype("string").fillna("brand_vide")
        df["subject"] = df["subject"].fillna("O_objet").astype("string")
        df["ca"] = df["ca"].fillna(0.0).astype(float)
        df["date_event"] = pd.to_datetime(df["date_event"], errors="coerce").fillna(datetime.now())
        self.clk.insert_df("prod_reporting", df)
        #df.to_csv("temps.csv",index=False,sep=';')
        logger.info(f"Données traitées : {len(df)} lignes")

        del df
        gc.collect()