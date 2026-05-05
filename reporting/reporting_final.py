from .adaptiveChunck import AdaptiveChunkManager
from threading import Lock
from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
from config.konticrea import connect_kit
from datetime import datetime,date,timedelta
import time, math, requests
from sqlalchemy import text
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from queue import Queue
from contextlib import contextmanager
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
    "brand","client_id","ListId","ListName","affiliate_id","client_id","ca","payvalue","clicks_val","leads_val","cpm_val","model","comment",
    "bounces","clickers","clicks","complaints","openers",
    "opens","sends","unsubs","date_schedule"
]
GROUP_COLS = [
    "database_id","adv_id","id_routers","segmentId","tag_id","date_event"    
]

COLUMNS_FINAL = [
    "database_id", "dwh_id",
    "country", "segmentId", "subject", "brand", "tag_id",
    "adv_id", "id_routers","id_focus", "affiliate_id","client_id", "ListId","ListName", "zipcode", "dep",
    "sends", "opens", "openers", "clicks", "clickers", "unsubs","complaints",
    "bounces","age_range", "gender", "main_isp", "age_gender_isp", "ca","payvalue","clicks_val","leads_val","cpm_val","model",
    "date_schedule", "date_event", "optimized","comment","date_schedule_max", "updated_at",
]
INT_COLS = [
    "database_id", "segmentId", "tag_id", "adv_id", "sends", "opens",
    "openers", "clicks", "clickers", "unsubs","complaints","bounces","ListId",
    "affiliate_id","client_id", "country","id_focus","clicks_val","leads_val","cpm_val"
]
STR_COLS = [
    "id_routers", "age_range", "gender", "main_isp",
    "optimized", "subject", "zipcode", "dep","comment","model"
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
        self.init_clk_pool()

    def init_clk_pool(self, size=4):
        self.clk_pool = Queue(maxsize=size)
        for _ in range(size):
            self.clk_pool.put(ClickHouseConfig().getClient_prod())

    @contextmanager
    def get_clk(self):
        clk = self.clk_pool.get()
        try:
            yield clk
        finally:
            self.clk_pool.put(clk)

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
                    vd.id  AS id_focus,
                    vd.base  AS database_id,
                    vd.date_shedule,
                    pa.caeur  AS ca,
                    pa.clickval    AS clicks_val,
                    pa.leadsval AS leads_val,
                    pa.cpmval AS cpm_val,
                    pa.p_comment AS comment,
                    pa.leadsval    AS leadsval,
                    pa.cpmval  AS cpmval,
                    pa.payvalue AS payvalue,
                    mo.name AS model
                FROM visu.v2_data vd
                JOIN visu.v2_status st ON st.id = vd.status
                LEFT JOIN visu.v2_payoutinfo pa ON pa.id_data = vd.id
                LEFT JOIN visu.model mo ON pa.model=mo.id
                WHERE st.id = 5
                AND vd.date_shedule BETWEEN :date_start AND :date_end
            ),
            routers AS (
                SELECT
                    vd.id     AS id_focus,
                    vd.idsendout,
                    TRUE      AS is_direct
                FROM visu.v2_data vd
                WHERE vd.id IN (SELECT id_focus FROM base)
                AND vd.idsendout IS NOT NULL

                UNION ALL

                SELECT
                    vd.id     AS id_focus,
                    vd2.idsendout,
                    FALSE     AS is_direct
                FROM visu.v2_data vd
                JOIN visu.v2_data_reuse r  ON r.id_v2  = vd.id
                JOIN visu.v2_data      vd2 ON vd2.id   = r.id_reuse
                WHERE vd.id IN (SELECT id_focus FROM base)
                AND vd2.idsendout IS NOT NULL
            )
            SELECT
                b.id_focus,
                b.database_id,
                MAX(b.ca)         AS ca,
                MAX(b.payvalue)  AS payvalue,
                MAX(b.clicks_val) AS clicks_val,
                MAX(b.leads_val) AS leads_val,
                MAX(b.cpm_val) AS cpm_val,
                MAX(b.comment)    AS comment,
                MAX(b.model) AS model,
                json_agg(DISTINCT b.date_shedule ORDER BY b.date_shedule) AS date_schedule,
                json_agg(DISTINCT jsonb_build_object(
                    'idsendout', r.idsendout,
                    'is_direct', r.is_direct
                )) FILTER (WHERE r.idsendout IS NOT NULL) AS id_routers
            FROM base b
            LEFT JOIN routers r ON r.id_focus = b.id_focus
            GROUP BY b.id_focus, b.database_id
        """)
        result: dict = {}

        try:
            with self.pg.connect() as conn:
                rows_result = conn.execution_options(
                    stream_results=True,
                    yield_per=batch,
                ).execute(query, {"date_start": date_start, "date_end": date_end})

                while True:
                    rows = rows_result.fetchmany(batch)
                    if not rows:
                        break

                    for row in rows:
                        (id_focus, database_id, ca,payvalue,clicks_val,leads_val,cpm_val,comment,model,date_schedule, id_routers_list) = row

                        if not id_routers_list:
                            continue

                        focus_data = {
                            "id_focus":      id_focus,
                            "database_id":   database_id,
                            "ca":            ca           or 0,
                            "payvalue":      payvalue     or 0,
                            "clicks_val":  clicks_val or 0,
                            "leads_val":   leads_val or 0,
                            "cpm_val": cpm_val or 0,
                            "comment":       comment       or "",
                            "model":        model or "",
                            "date_schedule": date_schedule if isinstance(date_schedule, list) else [],
                        }

                        for item in id_routers_list:
                            id_router = item.get("idsendout")
                            if id_router is None:
                                continue

                            is_direct = item.get("is_direct", False)

                            key = (int(id_router), int(database_id))
                            existing = result.get(key)

                            if existing is None or (is_direct and not existing["is_direct"]):
                                result[key] = {**focus_data, "is_direct": is_direct}

        except Exception as e:
            self.notifier_erreur(f"Erreur Focus : {e}")
            return {}

        logger.info(f"Focus : {len(result)} entrées (id_router, database_id)")
        return result
    def recupere_events(self, id_routers_focus,date_start,date_end):
        if not id_routers_focus:
            return

        end_dt = datetime.strptime(date_end, "%Y-%m-%d")
        extended_end = (end_dt + timedelta(days=7)).strftime("%Y-%m-%d")

        query = f"""
            SELECT
            p.database_id,
            p.MessageId AS id_routers,
            p.adv_id,
            p.dwh_id,
            p.SegmentId AS segmentId,
            p.MessageSubject AS subject,
            p.event_type,
            p.Date AS date_event,
            p.tag AS tag_id,
            p.brand,
            p.client_id,
            p.ListId,
            p.ListName,
            p.affiliate_id,
            d.stats_id
        FROM prod_events_2 p
        LEFT JOIN databases d ON d.id = p.database_id
        PREWHERE p.MessageId IN ({",".join(map(str, id_routers_focus))})
        WHERE p.Date BETWEEN '{date_start}' AND '{extended_end}'
        AND p.adv_id != 0
"""

        try:
            with self.get_clk() as clk:
                r = clk.query(query)

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
                with self.get_clk() as clk:
                    r = clk.query(query)
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

        ids = ",".join(str(x) for x in database_ids)

        query = f"""
        SELECT id, ktk_id, basename, country
        FROM databases
        WHERE id IN ({ids})
        """

        try:
            with self.get_clk() as clk:
                r = clk.query(query)

                return {
                    str(row[0]): {
                        "ktk_id": row[1] or "ktk_vide",
                        "basename": row[2] or "base_vide",
                        "country": row[3] or 0,
                    }
                    for row in r.result_rows
                }

        except Exception as e:
            self.notifier_erreur(f"Erreur ktk_id: {e}")
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
        seen_openers:  set = set()
        seen_clickers: set = set()
        seen_unsubs: set = set()
        seen_lock = Lock()

        def read_state():
            file = "etat.txt"
            state = {"initialized": False, "processed": set()}
            if not os.path.exists(file):
                return state
            try:
                with open(file, "r") as f:
                    content = f.read().strip()
                for line in content.splitlines():
                    if line.startswith("initialized="):
                        state["initialized"] = line.split("=")[1] == "1"
                    elif line.startswith("processed="):
                        months = line.split("=")[1]
                        if months:
                            state["processed"] = set(months.split(","))
            except Exception as e:
                logger.error(f"Erreur lecture état : {e}")
            return state

        def write_state(state):
            try:
                with open("etat.txt", "w") as f:
                    f.write(f"initialized={'1' if state['initialized'] else '0'}\n")
                    f.write(f"processed={','.join(sorted(state['processed']))}")
            except Exception as e:
                logger.error(f"Erreur écriture état : {e}")

        def chunk_list(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i:i + size]

        # 🔹 lecture état
        state = read_state()
        initialized = state["initialized"]
        processed_months = state["processed"]

        n_months = 2 if initialized else 3
        logger.info(f"Lancement {'régulier' if initialized else 'initial'} — {n_months} mois")

        months = self.get_month_ranges(1, 1, 2026)

        MAX_WORKERS_PROCESS = 4

        try:
            for date_start, date_end, partition in months:

                seen_openers.clear()
                seen_clickers.clear()
                if partition in processed_months:
                    logger.info(f"[SKIP] Déjà traité : {partition}")
                    continue

                logger.info(f"Traitement {date_start} à {date_end}")
                if initialized:
                    try:
                        query = f"ALTER TABLE {self.table} DROP PARTITION {partition}"
                        self.clk.query(query)
                        logger.info(f"Partition supprimée : {partition}")
                    except Exception as e:
                        logger.warning(f"Erreur DROP partition {partition} : {e}")
                data = self.recupere_pg(date_start, date_end)
                if not data:
                    logger.warning(f"Pas de données pour {partition}")
                    continue

                focus_map = data
                router_ids = list({int(id_r) for (id_r, db_id) in data.keys()})
                del data

                rows = []
                futures = []

                from concurrent.futures import ThreadPoolExecutor, as_completed

                with ThreadPoolExecutor(max_workers=MAX_WORKERS_PROCESS) as executor:

                    for id_routers in chunk_list(router_ids, 5):

                        for row in self.recupere_events(id_routers, date_start, date_end):

                            try:
                                id_r = int(row.get("id_routers", 0))
                                db = int(row.get("stats_id", 0))
                            except (ValueError, TypeError):
                                continue

                            key = (id_r, db)
                            focus = focus_map.get(key)
                            if not focus:
                                continue
                            row.update({
                                "id_focus": focus["id_focus"],
                                "ca": focus["ca"],
                                "payvalue": focus["payvalue"],
                                "clicks_val": focus["clicks_val"],
                                "leads_val": focus["leads_val"],
                                "cpm_val": focus["cpm_val"],
                                "comment": focus["comment"],
                                "model": focus["model"],
                                "date_schedule": focus["date_schedule"],
                                "is_direct": focus["is_direct"],
                            })

                            ev  = row.get("event_type")
                            dedup_key = (
                                row.get("adv_id"),
                                row.get("id_routers"),
                                row.get("dwh_id"),
                            )
                            with seen_lock:
                                if ev == "Opens":
                                    row["openers"] = int(dedup_key not in seen_openers)
                                    if row["openers"]:
                                        seen_openers.add(dedup_key)
                                else:
                                    row["openers"] = 0

                                if ev == "Clicks":
                                    row["clickers"] = int(dedup_key not in seen_clickers)
                                    if row["clickers"]:
                                        seen_clickers.add(dedup_key)
                                else:
                                    row["clickers"] = 0
                                
                                if ev == "Removals":
                                    row["unsubs"] = int(dedup_key not in seen_unsubs)
                                    if row["unsubs"]:
                                        seen_unsubs.add(dedup_key)
                                else:
                                    row["unsubs"] = 0

                            rows.append(row)
                            if len(rows) >= 8000:
                                futures.append(
                                    executor.submit(self._process_batch, rows.copy())
                                )
                                rows.clear()

                            if len(futures) >= MAX_WORKERS_PROCESS * 2:
                                for f in as_completed(futures[:2]):
                                    f.result()
                                futures = futures[2:]

                    if rows:
                        futures.append(executor.submit(self._process_batch, rows.copy()))
                        rows.clear()

                    for f in as_completed(futures):
                        f.result()
                del focus_map, router_ids
                gc.collect()

                state["processed"].add(partition)
                write_state(state)

                logger.info(f"[SUCCESS] {date_start} - {date_end}")
            if not initialized:
                state["initialized"] = True
                write_state(state)
                logger.info("État initialisé")

        except Exception as e:
            logger.error(f"Erreur pipeline : {e}", exc_info=True)
    
    def _process_batch(self, rows_batch, database_id=None):
        clk = ClickHouseConfig().getClient_prod()
        if not rows_batch:
            return

        df = pd.DataFrame(rows_batch)

        if df.empty:
            return

        # 🔹 filtre optionnel
        if database_id is not None:
            df = df[df["database_id"] == database_id]
            if df.empty:
                return

        # 🔹 colonnes minimales (évite boucle NEED_COLS)
        missing_cols = set(NEED_COLS) - set(df.columns)
        for col in missing_cols:
            df[col] = None

        df["database_id"] = df["database_id"].astype(str)
        df["dwh_id"] = df["dwh_id"].astype("string").fillna("")
        df["event_type"] = df["event_type"].astype("string")

        event = df["event_type"].values

        df["sends"] = (event == "Sends").astype(np.int8)
        df["opens"] = (event == "Opens").astype(np.int8)
        df["clicks"] = (event == "Clicks").astype(np.int8)
        df["complaints"] = (event == "Complaints").astype(np.int8)
        df["bounces"] = (event == "Bounces").astype(np.int8)

        if "openers" not in df.columns:
            df["openers"]  = 0
        if "clickers" not in df.columns:
            df["clickers"] = 0
        if "unsubs" not in df.columns:
            df["unsubs"] = 0
        
        df["openers"]  = pd.to_numeric(df["openers"],  errors="coerce").fillna(0).astype(np.int8)
        df["clickers"] = pd.to_numeric(df["clickers"], errors="coerce").fillna(0).astype(np.int8)
        df["unsubs"]   = pd.to_numeric(df["unsubs"],   errors="coerce").fillna(0).astype(np.int8)

        dwh_ids = df["dwh_id"].unique()
        contacts_map = self.get_contacts_cached(dwh_ids)

        if contacts_map:
            df["age"] = df["dwh_id"].map(lambda x: contacts_map.get(x, {}).get("age"))
            df["gender"] = df["dwh_id"].map(lambda x: contacts_map.get(x, {}).get("gender", "O_gender"))
            df["main_isp"] = df["dwh_id"].map(lambda x: contacts_map.get(x, {}).get("main_isp", "O_isp"))
            df["zipcode"] = df["dwh_id"].map(lambda x: contacts_map.get(x, {}).get("zipcode", "zipcode_vide"))
            df["dep"] = df["dwh_id"].map(lambda x: contacts_map.get(x, {}).get("dep", "dep_vide"))

        df["gender"] = df["gender"].fillna("O_gender").replace({"O": "O_gender"})
        df["main_isp"] = df["main_isp"].fillna("O_isp").replace({"Other": "O_isp"})
        df["zipcode"] = df["zipcode"].fillna("zipcode_vide")
        df["dep"] = df["dep"].fillna("dep_vide")

        age = pd.to_numeric(df["age"], errors="coerce").values

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

        database_ids = df["database_id"].unique()
        db_map = self.get_ktk_id_cached(database_ids)

        if db_map:
            df["ktk_id"] = df["database_id"].map(lambda x: db_map.get(x, {}).get("ktk_id", "ktk_vide"))
            df["basename"] = df["database_id"].map(lambda x: db_map.get(x, {}).get("basename", "base_vide"))
            df["country"] = df["database_id"].map(lambda x: db_map.get(x, {}).get("country", 0))
        try:
            keys = list(zip(df["id_focus"].astype(str), df["ktk_id"].astype(str)))
            optimized_map = self.get_optimize_cached(
                [{"id_focus": k[0], "ktk_id": k[1]} for k in set(keys)]
            ) or {}

            df["optimized"] = pd.Series(keys).map(optimized_map).fillna("optimized_vide")

        except Exception as e:
            logger.warning(f"Optimize indisponible: {e}")
            df["optimized"] = "optimized_vide"

        df["date_event"] = pd.to_datetime(df["date_event"], errors="coerce")

        if "date_schedule" not in df:
            df["date_schedule"] = [[] for _ in range(len(df))]

        df["date_schedule_max"] = df["date_schedule"].map(
            lambda x: max(x) if isinstance(x, list) and x else None
        )

        df["date_schedule_max"] = pd.to_datetime(df["date_schedule_max"], errors="coerce")
        df["updated_at"] = datetime.now()
        df = df.reindex(columns=COLUMNS_FINAL, fill_value=None)

        df[INT_COLS] = df[INT_COLS].apply(pd.to_numeric, errors="coerce").fillna(0).astype(np.int32)
        df[STR_COLS] = df[STR_COLS].astype("string").fillna("")

        df["ca"] = df["ca"].fillna(0.0).astype(float)
        df["payvalue"] = df["payvalue"].fillna(0.0).astype(float)

        df["date_event"] = df["date_event"].fillna(datetime.now())
        with self.get_clk() as clk:

            for i in range(0, len(df), 20000):
                chunk = df.iloc[i:i + 20000]

                try:
                    clk.insert_df(self.table, chunk)
                    logger.info('[SUCCESS]')
                except Exception as e:
                    logger.error(f"Insert error: {e}")

        del df
        gc.collect()