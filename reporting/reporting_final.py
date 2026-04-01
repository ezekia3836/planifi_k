from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
from config.konticrea import connect_kit
from datetime import datetime,date
import time, math, requests, psutil
from sqlalchemy import text
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import logging

BATCH_ADV_SIZE = 200
BATCH_CONTACTS = 2_000
BATCH_INSERT = 5_000
MAX_HTTP_WORKERS = 8

AGE_BINS   = [0, 18, 25, 35, 45, 55, 65, 75, float("inf")]
AGE_LABELS = ["0-18", "18-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75+"]
GROUP_COLS = [
    "id_focus","database_id","adv_id","id_routers","segmentId","tag_id","affiliate_id","date_event","age_range","gender","main_isp","age_gender_isp"    
]
"""GROUP_COLS = [
    "database_id", "segmentId", "subject", "adv_id", "id_routers",
    "tag_id", "brand", "date_event", "age_range", "gender", "main_isp",
    "age_gender_isp", "optimized", "country", "ListId", "zipcode",
    "dep", "affiliate_id","comment"
]"""
COLUMNS_FINAL = [
    "database_id", "dwh_id",
    "country", "segmentId", "subject", "brand", "tag_id",
    "adv_id", "id_routers", "affiliate_id", "ListId", "zipcode", "dep",
    "sends", "opens", "openers", "clicks", "clickers", "unsubs","complaints",
    "bounces","age_range", "gender", "main_isp", "age_gender_isp", "ca",
    "date_schedule", "date_event", "optimized","comment","date_schedule_max", "updated_at",
]
INT_COLS = [
    "database_id", "segmentId", "tag_id", "adv_id", "sends", "opens",
    "openers", "clicks", "clickers", "unsubs","complaints","bounces", "ca", "ListId",
    "affiliate_id", "country",
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
        today = datetime.today()
        year_start = 2025
        #self.date_start=date(year_start,1,1)
        self.date_end = today.date()
        self.date_start = (today - relativedelta(months=1)).date()
        self.batch_adv_size = BATCH_ADV_SIZE
        self.konticrea = connect_kit()
        self.seen_openers= set()
        self.seen_clickers =set()

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

    def recupere_pg(self, batch=1000):
        query = text(f"""
            SELECT vd.id AS id_focus,
                   pa.caeur AS ca,
                   vd.comment,
                   COALESCE(json_agg(DISTINCT vd.date_shedule), '[]'::json) AS date_schedule,
                   COALESCE(json_agg(DISTINCT idsendouts.idsendout), '[]'::json) AS id_routers
            FROM visu.v2_data vd
            JOIN visu.v2_status st ON st.id = vd.status
            LEFT JOIN visu.v2_payoutinfo pa ON pa.id_data = vd.id
            LEFT JOIN LATERAL (
                SELECT vd1.idsendout FROM (
                    SELECT vd.idsendout
                    UNION
                    SELECT vd2.idsendout FROM visu.v2_data vd2
                    WHERE vd2.id = ANY (
                        SELECT vdr2.id_reuse FROM visu.v2_data_reuse vdr2
                        WHERE vdr2.id_v2 = vd.id
                    ) AND vd2.idsendout IS NOT NULL
                ) AS vd1
            ) AS idsendouts ON TRUE
            WHERE st.id = 5 AND vd.date_shedule BETWEEN '{self.date_start}' AND '{self.date_end}'
            GROUP BY pa.caeur, vd.id
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
                        id_focus, ca,comment,date_schedule, id_routers_list = row
                        if not id_routers_list or isinstance(id_routers_list, str):
                            continue
                        for id_r in id_routers_list:
                            if id_r is None:
                                continue
                            pg_map[str(id_r)] = {
                                "id_focus": str(id_focus),
                                "ca": ca,
                                "comment" : comment,
                                "date_schedule": date_schedule or [],
                            }
        except Exception as e:
            self.notifier_erreur(f"Erreur Focus : {e}")
            return {}
        return pg_map

    def recupere_events(self, id_routers_focus):
        if not id_routers_focus:
            return
        for i in range(0, len(id_routers_focus), self.batch_adv_size):
            batch     = id_routers_focus[i:i + self.batch_adv_size]
            batch_str = ",".join(str(x) for x in batch)
            if not batch_str:
                continue
            query = f"""
                SELECT e.database_id, e.MessageId AS id_routers, e.adv_id, e.dwh_id,
                       e.SegmentId AS segmentId, e.MessageSubject AS subject,
                       e.event_type, e.Date AS date_event, e.tag AS tag_id,
                       e.brand, e.client_id, e.ListId, e.affiliate_id
                FROM events_2 e
                INNER JOIN (
                    SELECT MessageId, event_type, max(run_id) AS max_run_id
                    FROM events_2
                    WHERE MessageId IN ({batch_str})
                      AND Date BETWEEN '{self.date_start}' AND '{self.date_end}'
                    GROUP BY MessageId, event_type, database_id
                ) m ON e.MessageId = m.MessageId
                     AND e.event_type = m.event_type
                     AND e.run_id = m.max_run_id
                WHERE e.MessageId IN ({batch_str})
                  AND e.Date BETWEEN '{self.date_start}' AND '{self.date_end}'
            """
            try:
                r = self.resilient_call(self.clk.query, query)
                for row in r.result_rows:
                    yield dict(zip(r.column_names, row))
            except Exception as e:
                self.notifier_erreur(f"Erreur events batch {batch_str} : {e}")
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
                       argMax(age,      updated_at) AS age,
                       argMax(gender,   updated_at) AS gender,
                       argMax(main_isp, updated_at) AS main_isp,
                       argMax(zipcode,  updated_at) AS zipcode,
                       argMax(dep,      updated_at) AS dep
                FROM prod_contacts
                WHERE dwh_id IN ({batch_str})
                GROUP BY dwh_id
            """
            try:
                r = self.resilient_call(self.clk.query, query)
                for row in r.result_rows:
                    contacts_map[str(row[0])] = dict(zip(r.column_names, row))
            except Exception as e:
                self.notifier_erreur(f"Erreur contacts batch : {e}")
        return contacts_map

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
    def report(self):
        logger.info("LANCEMENT.........")
        self.seen_openers.clear()
        self.seen_clickers.clear()
        logger.info("recup FOCUS")
        focus_map = self.recupere_pg()
        id_routers = list(focus_map.keys())
        temp_rows = []
        batch_size_events = 500
        logger.info("recup EVENTS")
        for row in self.recupere_events(id_routers):
            focus_data = focus_map.get(str(row.get("id_routers")))
            if not focus_data:
                continue
            row.update(focus_data)
            ev = row.get("event_type")
            key = (row.get("adv_id"), row.get("id_routers"), row.get("dwh_id"))
            row["sends"] = int(ev == "Sends")
            row["opens"] = int(ev == "Opens")
            row["clicks"] = int(ev == "Clicks")
            row["unsubs"] = int(ev == "Removals")
            row["complaints"] = int(ev == "Complaints")
            row["bounces"] = int(ev == "Bounces")
            if ev == "Opens" and key not in self.seen_openers:
                row["openers"] = 1
                self.seen_openers.add(key)
            else:
                row["openers"] = 0
            if ev == "Clicks" and key not in self.seen_clickers:
                row["clickers"] = 1
                self.seen_clickers.add(key)
            else:
                row["clickers"] = 0
            temp_rows.append(row)
            if len(temp_rows) >= batch_size_events:
                self._process_batch(temp_rows)
                temp_rows = []
        if temp_rows:
            self._process_batch(temp_rows)

    def _process_batch(self, rows_batch, database_id=None):
        import gc

        if len(rows_batch) > 10000:
            logger.warning(f"Batch trop gros: {len(rows_batch)} → skip")
            return

        df = pd.DataFrame.from_records(rows_batch)

        needed_cols = [
            "database_id","id_routers","adv_id","dwh_id","segmentId",
            "subject","event_type","date_event","tag_id","brand",
            "client_id","ListId","affiliate_id","ca","comment",'bounces', 
            'clickers','clicks', 'complaints', 'openers', 'opens', 'sends', 'unsubs'
        ]

        df = df.reindex(columns=needed_cols)

        if database_id is not None:
            df = df[df["database_id"] == database_id]

        if df.empty:
            return

        df["dwh_id"] = df["dwh_id"].astype("string").fillna("")

        dwh_ids = df["dwh_id"].unique().tolist()

        contacts_map = {}
        try:
            contacts_map = self.resilient_call(self.recupere_contacts, dwh_ids) or {}
        except Exception as e:
            logger.warning(f"Contacts indisponible: {e}")

        if contacts_map:
            contacts_df = (
                pd.DataFrame.from_dict(contacts_map, orient="index")
                .reset_index()
                .rename(columns={"index": "dwh_id"})
            )
            if "dwh_id" in contacts_df.columns:
                contacts_df = contacts_df.loc[:, ~contacts_df.columns.duplicated()]
            contacts_df["dwh_id"] = contacts_df["dwh_id"].astype("string")
            df = df.merge(contacts_df, on="dwh_id", how="left")
            del contacts_df
            gc.collect()

        if "age" in df.columns:
            df["age"] = pd.to_numeric(df["age"], errors="coerce")
            df["age_range"] = pd.cut(
                df["age"],
                bins=AGE_BINS,
                labels=AGE_LABELS,
                right=False
            ).astype("string").fillna("O_age")
        else:
            df["age_range"] = "O_age"

        df["gender"] = df.get("gender").fillna("O_gender").replace({"O": "O_gender"})
        df["main_isp"] = df.get("main_isp").fillna("O_isp").replace({"Other": "O_isp"})
        df["zipcode"] = df.get("zipcode").fillna("zipcode_vide")
        df["dep"] = df.get("dep").fillna("dep_vide")

        df["age_gender_isp"] = (
            df["age_range"].astype(str) + "_" +
            df["gender"].astype(str) + "_" +
            df["main_isp"].astype(str)
        )

        database_ids = df["database_id"].dropna().unique().tolist()

        db_map = {}
        try:
            db_map = self.resilient_call(self.recupere_ktk_id, database_ids) or {}
        except Exception as e:
            logger.warning(f"DB mapping indisponible: {e}")

        if db_map:
            db_df = (
                pd.DataFrame.from_dict(db_map, orient="index")
                .reset_index()
                .rename(columns={"index": "database_id"})
            )

            df["database_id"] = pd.to_numeric(df["database_id"], errors="coerce")
            db_df["database_id"] = pd.to_numeric(db_df["database_id"], errors="coerce")

            df = df.merge(db_df, on="database_id", how="left")

            del db_df
            gc.collect()

        df["ktk_id"] = df.get("ktk_id", "ktk_vide").fillna("ktk_vide")
        df["basename"] = df.get("basename", "base_vide").fillna("base_vide")
        df["country"] = df.get("country", 0).fillna(0)

        df["optimized"] = "optimized_vide"

        try:
            optimize_params = df[["id_focus", "ktk_id"]].drop_duplicates()

            optimized_map = self.resilient_call(
                self.recuper_optimize,
                optimize_params.to_dict("records")
            ) or {}

            if optimized_map:
                opt_df = pd.DataFrame(
                    [(k[0], k[1], v) for k, v in optimized_map.items()],
                    columns=["id_focus", "ktk_id", "optimized"]
                )

                df["id_focus"] = df["id_focus"].astype(str)
                df["ktk_id"] = df["ktk_id"].astype(str)

                df = df.merge(opt_df, on=["id_focus", "ktk_id"], how="left")

                df["optimized"] = df["optimized"].fillna("optimized_vide")

                del opt_df
                gc.collect()

        except Exception as e:
            logger.warning(f"Optimize indisponible: {e}")
        defaults = {
            "subject": "O_objet",
            "brand": "brand_vide",
            "zipcode": "zipcode_vide",
            "dep": "dep_vide",
            "comment": "",
            "ListId": 0,
            "country": 0,
            "optimized": "optimized_vide"
        }

        for c, v in defaults.items():
            df[c] = df.get(c, v).fillna(v)

        if "date_schedule" not in df.columns:
            df["date_schedule"] = [[] for _ in range(len(df))]

        df["date_event"] = pd.to_datetime(df["date_event"], errors="coerce")

        df = df.groupby(GROUP_COLS, dropna=False).agg(
            sends=("sends", "sum"),
            opens=("opens", "sum"),
            openers=("openers", "max"),
            clicks=("clicks", "sum"),
            clickers=("clickers", "max"),
            unsubs=("unsubs", "sum"),
            complaints=("complaints", "sum"),
            bounces=("bounces", "sum"),
            ca=("ca", "max"),
            dwh_id=("dwh_id", "first"),
            subject=("subject", "first"),
            brand=("brand", "first"),
            zipcode=("zipcode", "first"),
            dep=("dep", "first"),
            comment=("comment", "first"),
            optimized=("optimized", "first"),
            country=("country", "max"),
            ListId=("ListId", "max"),
            date_schedule=("date_schedule", lambda x: sorted({
                d for sub in x if isinstance(sub, list) for d in sub
            }))
        ).reset_index()

        if df.empty:
            return

        df["date_schedule_max"] = (
            df["date_schedule"]
            .explode()
            .pipe(pd.to_datetime, errors="coerce")
            .groupby(level=0)
            .max()
        )

        df["updated_at"] = datetime.now()

        for col in INT_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(np.int32)

        for col in STR_COLS:
            if col in df.columns:
                df[col] = df[col].astype("string").fillna("")

        df["dwh_id"] = df["dwh_id"].astype("string")
        df["brand"] = df["brand"].astype("string").fillna("brand_vide")
        df["subject"] = df["subject"].fillna("O_objet")

        df["updated_at"] = pd.to_datetime(df["updated_at"])
        df["date_event"] = pd.to_datetime(df["date_event"], errors="coerce").fillna(datetime.now())
        total = len(df)

        for start in range(0, total, BATCH_INSERT):
            chunk = df.iloc[start:start + BATCH_INSERT]
            if not chunk.empty:
                self.clk.insert_df(self.table, chunk)

        logger.info(f"Données insérées : {total} lignes")

        del df
        gc.collect()