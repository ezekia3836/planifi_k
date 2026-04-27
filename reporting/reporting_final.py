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
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        self.table = "prod_reporting_test"
        self.adaptive_chunk = AdaptiveChunkManager()
        self.batch_adv_size = BATCH_ADV_SIZE
        self.konticrea = connect_kit()
        self.cache_contacts = {}
        self.ktk_id_cache = {}
        self.optimize_cache = {}
  

    def recupere_pg_optimized(self, date_start, date_end, batch=1000, max_workers=4):
        start = datetime.strptime(date_start, "%Y-%m-%d")
        end = datetime.strptime(date_end, "%Y-%m-%d")

        final_map = {}

        def process_day(day):
            day_start = day.strftime("%Y-%m-%d")
            day_end = (day + timedelta(days=1)).strftime("%Y-%m-%d")
            return self._recupere_pg_single(day_start, day_end, batch)

        days = []
        current = start
        while current <= end:
            days.append(current)
            current += timedelta(days=1)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_day, d): d for d in days}

            for future in as_completed(futures):
                day = futures[future]
                try:
                    pg_map = future.result()
                    if not pg_map:
                        continue

                    # 🔥 merge optimisé
                    for key, value in pg_map.items():
                        existing = final_map.get(key)

                        if existing is None or (value["is_direct"] and not existing.get("is_direct")):
                            final_map[key] = value

                except Exception as e:
                    self.notifier_erreur(f"Erreur jour {day} : {e}")

        return final_map

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
        query = text("""
            WITH base AS (
                SELECT
                    vd.id          AS id_focus,
                    vd.advertiser,
                    vd.date_shedule,
                    pa.caeur       AS ca,
                    vd.comment
                FROM visu.v2_data vd
                JOIN visu.v2_status st ON st.id = vd.status
                LEFT JOIN visu.v2_payoutinfo pa ON pa.id_data = vd.id
                WHERE st.id = 5
                AND vd.date_shedule BETWEEN :date_start AND :date_end
            ),
            routers AS (
                -- ── directs : idsendout du focus lui-même
                SELECT
                    vd.id     AS id_focus,
                    vd.idsendout,
                    TRUE      AS is_direct
                FROM visu.v2_data vd
                -- ── filtre sur les focus de la base seulement
                WHERE vd.id IN (SELECT id_focus FROM base)
                AND vd.idsendout IS NOT NULL

                UNION ALL

                -- ── reuses : idsendout des campagnes réutilisées
                SELECT
                    vd.id     AS id_focus,
                    vd2.idsendout,
                    FALSE     AS is_direct
                FROM visu.v2_data vd
                JOIN visu.v2_data_reuse r  ON r.id_v2   = vd.id
                JOIN visu.v2_data      vd2 ON vd2.id    = r.id_reuse
                WHERE vd.id IN (SELECT id_focus FROM base)
                AND vd2.idsendout IS NOT NULL
            )
            SELECT
                b.id_focus,
                b.advertiser,
                MAX(b.ca)       AS ca,
                MAX(b.comment)  AS comment,
                json_agg(DISTINCT b.date_shedule ORDER BY b.date_shedule) AS date_schedule,
                json_agg(DISTINCT jsonb_build_object(
                    'idsendout', r.idsendout,
                    'is_direct', r.is_direct
                )) FILTER (WHERE r.idsendout IS NOT NULL) AS id_routers
            FROM base b
            LEFT JOIN routers r ON r.id_focus = b.id_focus
            GROUP BY b.id_focus, b.advertiser
        """)

        focus_cache = {}
        pg_map = []

        try:
            with self.pg.connect() as conn:
                result = conn.execution_options(
                    stream_results=True,
                    yield_per=batch,
                ).execute(query,
                {"date_start":date_start, "date_end":date_end}
                )

                while True:
                    rows = result.fetchmany(batch)
                    if not rows:
                        break

                    for row in rows:
                        id_focus, advertiser, ca, comment, date_schedule, id_routers_list = row

                        # =========================
                        # focus cache (stable)
                        # =========================
                        if id_focus not in focus_cache:
                            focus_cache[id_focus] = {
                                "advertiser": advertiser,
                                "ca": ca,
                                "comment": comment,
                                "date_schedule": date_schedule if isinstance(date_schedule, list) else [],
                            }

                        # =========================
                        # 🔥 IMPORTANT : PAS DE GROUPING PAR id_router
                        # =========================
                        if not id_routers_list:
                            continue

                        for item in id_routers_list:
                            id_r = item.get("idsendout")
                            if id_r is None:
                                continue

                            pg_map.append({
                                "id_router": id_r,
                                "id_focus": id_focus,
                                "is_direct": item.get("is_direct", False)
                            })

        except Exception as e:
            self.notifier_erreur(f"Erreur Focus : {e}")
            return {}

        # =========================
        # 🔥 RESULT FINAL = LIGNES DISTINCTES
        # =========================
        result = {}

        for row in pg_map:
            id_r = row["id_router"]
            id_focus = row["id_focus"]

            focus = focus_cache.get(id_focus)
            if not focus:
                continue

            # clé unique = combinaison
            key = (id_r, id_focus)

            result[key] = {
                "id_router": id_r,
                "id_focus": id_focus,
                "advertiser": focus["advertiser"],
                "ca": focus["ca"],
                "comment": focus["comment"],
                "date_schedule": focus["date_schedule"],
                "is_direct": row["is_direct"],
            }

        logger.info(f"Focus : {len(result)} rows")
        return result

    def recupere_events(self, id_routers_focus,date_start,date_end):
        if not id_routers_focus:
            return

        end_dt = datetime.strptime(date_end, "%Y-%m-%d")
        extended_end = (end_dt + timedelta(days=7)).strftime("%Y-%m-%d")

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
        FROM prod_events_2
        WHERE
            MessageId IN ({",".join(map(str, id_routers_focus))})
            AND adv_id != 0 AND Date BETWEEN '{date_start}' AND '{extended_end}'
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

        months = self.get_month_ranges(1, 1, 2026)

        success = True

        try:
            for date_start, date_end, partition in months:

                if initialized:
                    try:
                        query = f"ALTER TABLE {self.table} DROP PARTITION {partition}"
                        self.clk.command(query)
                        logger.info(f"Partition supprimée : {partition}")
                    except Exception as e:
                        logger.warning(f"Erreur DROP partition {partition} : {e}")

                logger.info(f"Traitement {date_start} à {date_end}")

                # =========================
                # 🔥 1. DATA FOCUS
                # =========================
                data = self.recupere_pg(date_start, date_end)

                if not data:
                    logger.warning(f"Pas de données pour {partition}")
                    continue

                focus_map = data  # id_router → focus
                router_ids = list(focus_map.keys())

                rows = []

                for chunk in chunk_list(router_ids, 5):

                    for row in self.recupere_events(chunk, date_start, date_end):

                        # ✔ clé correcte = id_router
                        focus = focus_map.get(row["id_router"])
                        if not focus:
                            continue

                        row["id_focus"] = focus["id_focus"]
                        row["ca"] = focus["ca"]
                        row["comment"] = focus["comment"]
                        row["date_schedule"] = focus["date_schedule"]
                        row["is_direct"] = focus["is_direct"]

                        rows.append(row)

                        if len(rows) >= 10_000:
                            self._process_batch(rows)
                            rows.clear()
                            self.clean_cache(max_size=200_000)
                            gc.collect()

                if rows:
                    self._process_batch(rows)
                    rows.clear()
                    gc.collect()

                logger.info(f"[SUCCESS] Traitement terminé pour: {date_start} - {date_end}")

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
        if len(rows_batch) > 70_000:
            logger.warning(f"Batch trop gros: {len(rows_batch)}")
            return
        df = pd.DataFrame(rows_batch)
        for col in NEED_COLS:
            if col not in df.columns:
                df[col] = None
        if df.empty:
            return

        if database_id is not None:
            df = df[df["database_id"] == database_id]
            if df.empty:
                return
        df["database_id"] = df["database_id"].astype(str)
        df["dwh_id"] = df["dwh_id"].astype("string").fillna("")
        df["event_type"] = df["event_type"].astype("string")

        event = df["event_type"]

        df["sends"] = event.eq("Sends").astype("int8")
        df["opens"] = event.eq("Opens").astype("int8")
        df["clicks"] = event.eq("Clicks").astype("int8")
        df["unsubs"] = event.eq("Removals").astype("int8")
        df["complaints"] = event.eq("Complaints").astype("int8")
        df["bounces"] = event.eq("Bounces").astype("int8")

        key_cols = ["adv_id", "id_routers", "dwh_id"]

        click_mask = event.eq("Clicks")
        open_mask = event.eq("Opens")

        df["clickers"] = 0
        df["openers"] = 0

        if click_mask.any():
            tmp = df.loc[click_mask, key_cols]
            df.loc[click_mask, "clickers"] = (~tmp.duplicated()).astype("int8").to_numpy()

        if open_mask.any():
            tmp = df.loc[open_mask, key_cols]
            df.loc[open_mask, "openers"] = (~tmp.duplicated()).astype("int8").to_numpy()

        dwh_ids = df["dwh_id"].unique()
        contacts_map = self.get_contacts_cached(dwh_ids)

        if contacts_map:
            contact_df = pd.DataFrame.from_dict(contacts_map, orient="index")
            contact_df.index.name = "dwh_id"
            contact_df = contact_df.reset_index()
            df = df.merge(contact_df, on="dwh_id", how="left")


        for col, default in [("age", None), ("gender", "O_gender"),
                            ("main_isp", "O_isp"), ("zipcode", "zipcode_vide"), ("dep", "dep_vide")]:
            if col not in df.columns:
                df[col] = default

        df["gender"]   = df["gender"].fillna("O_gender").replace({"O": "O_gender"})
        df["main_isp"] = df["main_isp"].fillna("O_isp").replace({"Other": "O_isp"})
        df["zipcode"]  = df["zipcode"].fillna("zipcode_vide")
        df["dep"]      = df["dep"].fillna("dep_vide")
        df["age_range"] = (
            pd.cut(pd.to_numeric(df["age"], errors="coerce"),
                bins=AGE_BINS,
                labels=AGE_LABELS,
                right=False)
            .astype("string")
            .fillna("O_age")
        )
        df["age_gender_isp"] = (
            df["age_range"].astype(str)
            + "_"
            + df["gender"].astype(str)
            + "_"
            + df["main_isp"].astype(str)
        )
        database_ids = df["database_id"].unique()
        db_map = self.get_ktk_id_cached(database_ids)

        if db_map:
            db_df = pd.DataFrame.from_dict(db_map, orient="index").reset_index()
            db_df.rename(columns={"index": "database_id"}, inplace=True)
            df = df.merge(db_df, on="database_id", how="left")

        for col, default in [("ktk_id", "ktk_vide"), ("basename", "base_vide"), ("country", 0)]:
            if col not in df.columns:
                df[col] = default

        df["ktk_id"]  = df["ktk_id"].fillna("ktk_vide")
        df["basename"]= df["basename"].fillna("base_vide")
        df["country"] = df["country"].fillna(0)

        try:
            optimize_params = df[["id_focus", "ktk_id"]].drop_duplicates()

            optimized_map = self.get_optimize_cached(
                optimize_params.to_dict("records")
            ) or {}

            keys = list(zip(df["id_focus"].astype(str), df["ktk_id"].astype(str)))

            df["optimized"] = [
                optimized_map.get(k, "optimized_vide")
                for k in keys
            ]

        except Exception as e:
            logger.warning(f"Optimize indisponible: {e}")
            df["optimized"] = "optimized_vide"

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

        df = df.reindex(columns=COLUMNS_FINAL, fill_value=None)

        for col in INT_COLS:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(np.int32)

        for col in STR_COLS:
            if col in df:
                df[col] = df[col].astype("string").fillna("")

        df["ca"] = df["ca"].fillna(0.0).astype(float)

        df["date_event"] = pd.to_datetime(df["date_event"], errors="coerce").fillna(datetime.now())


        batch_size = 5000
        n = len(df)

        for start in range(0, n, batch_size):
            end = start + batch_size
            chunk = df[start:end]
            try:
                self.clk.insert_df(self.table, chunk)
                # file_path = "temps.csv" 
                # chunk.to_csv( file_path, mode='a', index=False, sep=';', header=not os.path.exists(file_path))
                logger.info(f"[SUCCESS]")
            except Exception as e:
                logger.error(f"[ERROR] Insert {start}-{end}: {e}")

        del df
        gc.collect()