from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time, math, requests, psutil
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text
import pandas as pd
import numpy as np

BATCH_ADV_SIZE   = 50
BATCH_CONTACTS   = 2_000
BATCH_INSERT     = 5_000   
MAX_HTTP_WORKERS = 8        

AGE_BINS   = [0, 18, 25, 35, 45, 55, 65, 75, float("inf")]
AGE_LABELS = ["0-18", "18-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75+"]

GROUP_COLS = [
    "database_id", "segmentId", "subject", "adv_id", "id_routers",
    "tag_id", "brand", "date_event", "age_range", "gender", "main_isp",
    "age_gender_isp", "optimized", "country", "ListId", "zipcode",
    "dep", "affiliate_id",
]
COLUMNS_FINAL = [
    "database_id", "country", "segmentId", "subject", "brand", "tag_id",
    "adv_id", "id_routers", "affiliate_id", "ListId", "zipcode", "dep",
    "sends", "opens", "openers", "clicks", "clickers", "unsubs",
    "age_range", "gender", "main_isp", "age_gender_isp", "ca",
    "date_schedule", "date_event", "optimized", "updated_at",
]
INT_COLS = [
    "database_id", "segmentId", "tag_id", "adv_id", "sends", "opens",
    "openers", "clicks", "clickers", "unsubs", "ca", "ListId",
    "affiliate_id", "country",
]
STR_COLS = [
    "id_routers", "age_range", "gender", "main_isp",
    "optimized", "subject", "zipcode", "dep",
]

class reporting:

    def __init__(self):
        self.clk   = ClickHouseConfig().getClient_prod()
        self.pg    = PgConfig().get_client()
        self.table = "dev_reporting_agg"
        today      = datetime.today()
        self.date_end   = today.date()
        self.date_start = datetime(year=today.year - 1, month=7, day=1).date()
        self.batch_adv_size = BATCH_ADV_SIZE
        self._session = requests.Session()
        retry = Retry(
            total=3, backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        self._session.mount(
            "https://",
            HTTPAdapter(max_retries=retry,
                        pool_connections=MAX_HTTP_WORKERS,
                        pool_maxsize=MAX_HTTP_WORKERS),
        )
        self._optimize_url = "https://konticreav2.kontikimedia.fr:5009/api/creativities/filter-plannifik"


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

    def notifier_info(self, message):  print(f"Succès : {message}")
    def notifier_erreur(self, message): print(f"Erreur : {message}")

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
        query = text("""
            SELECT vd.id AS id_focus,
                   pa.caeur AS ca,
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
            WHERE st.id = 5
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
                        id_focus, ca, date_schedule, id_routers_list = row
                        if not id_routers_list or isinstance(id_routers_list, str):
                            continue
                        for id_r in id_routers_list:
                            if id_r is None:
                                continue
                            pg_map[str(id_r)] = {
                                "id_focus":      str(id_focus),
                                "ca":            ca,
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
            query = f"""
                SELECT e.database_id, e.MessageId AS id_routers, e.adv_id, e.dwh_id,
                       e.SegmentId AS segmentId, e.MessageSubject AS subject,
                       e.event_type, e.Date AS date_event, e.tag AS tag_id,
                       e.brand, e.client_id, e.ListId, e.affiliate_id
                FROM events e
                INNER JOIN (
                    SELECT MessageId, event_type, max(run_id) AS max_run_id
                    FROM events
                    WHERE MessageId IN ({batch_str})
                      AND Date BETWEEN '{self.date_start}' AND '{self.date_end}'
                    GROUP BY MessageId, event_type, database_id
                    ORDER BY database_id ASC
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
                self.notifier_erreur(f"Erreur recup events batch {batch_str} : {e}")

    def recupere_contacts(self, dwh_ids, batch_size=BATCH_CONTACTS):
        if not dwh_ids:
            return {}
        contacts_map = {}
        for i in range(0, len(dwh_ids), batch_size):
            batch_str = ",".join(f"'{x}'" for x in dwh_ids[i:i + batch_size])
            query = f"""
                SELECT database_id,
                       argMax(age,      updated_at) AS age,
                       argMax(gender,   updated_at) AS gender,
                       argMax(main_isp, updated_at) AS main_isp,
                       argMax(zipcode,  updated_at) AS zipcode,
                       argMax(dep,      updated_at) AS dep
                FROM prod_contacts
                WHERE database_id IN ({batch_str})
                GROUP BY database_id
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
            query = f"SELECT id AS database_id, ktk_id, basename, country FROM databases WHERE id IN ({ids})"
            r     = self.clk.query(query)
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

    def _fetch_one_optimize(self, id_routers, id_focus, ktk_id) -> tuple:
        key = (str(id_routers), str(id_focus), str(ktk_id))
        if not (id_routers and id_focus and ktk_id):
            return key, "url_vide"
        try:
            resp = self._session.post(
                self._optimize_url,
                params={"focus_id": id_focus, "base_id": ktk_id, "router_id": id_routers},
                timeout=20,
            )
            data = resp.json() if resp.status_code == 200 else {}
            opt  = next(
                (x.get("optimized") for x in data.get("data", []) if x.get("optimized")),
                "url_vide",
            )
        except Exception as e:
            self.notifier_erreur(f"Optimize erreur {key} : {e}")
            opt = "url_vide"
        return key, opt

    def recuper_optimize(self, rows_list, chunck=50):
        unique_keys = {
            (str(self.safe(r.get("id_routers"))),
             str(self.safe(r.get("id_focus"))),
             str(self.safe(r.get("ktk_id"))))
            for r in rows_list
        }
        optimized_map = {}
        with ThreadPoolExecutor(max_workers=MAX_HTTP_WORKERS) as ex:
            futures = {ex.submit(self._fetch_one_optimize, *k): k for k in unique_keys}
            for f in as_completed(futures):
                key, opt = f.result()
                optimized_map[key] = opt
        return optimized_map

    def report(self):
        print("Récupération Focus")
        focus_map = self.recupere_pg()
        id_routers = list(focus_map.keys())
        batch_size_events = 10_000
        seen_openers:  set = set()
        seen_clickers: set = set()
        temp_rows = []
        print("Récupération events")
        for row in self.recupere_events(id_routers):
            focus_data = focus_map.get(str(row.get("id_routers")))
            if not focus_data:
                continue
            row.update(focus_data)
            ev  = row.get("event_type")
            key = (row.get("adv_id"), row.get("id_routers"), row.get("dwh_id"))
            row["sends"]      = 1 if ev == "Sends"      else 0
            row["opens"]      = 1 if ev == "Opens"      else 0
            row["clicks"]     = 1 if ev == "Clicks"     else 0
            row["unsubs"]     = 1 if ev == "Removals"   else 0
            row["complaints"] = 1 if ev == "Complaints" else 0
            row["bounces"]    = 1 if ev == "Bounces"    else 0
            row["openers"]    = 1 if ev == "Opens"  and key not in seen_openers  else 0
            row["clickers"]   = 1 if ev == "Clicks" and key not in seen_clickers else 0
            if row["openers"]:  seen_openers.add(key)
            if row["clickers"]: seen_clickers.add(key)
            temp_rows.append(row)
            if len(temp_rows) >= batch_size_events:
                start_time = time.time()
                self._process_batch(temp_rows)
                elapsed   = time.time() - start_time
                mem_ratio = psutil.virtual_memory().available / psutil.virtual_memory().total
                if elapsed > 10 or mem_ratio < 0.2:
                    batch_size_events = max(1_000, int(batch_size_events * 0.7))
                elif elapsed < 5 and mem_ratio > 0.5:
                    batch_size_events = min(50_000, int(batch_size_events * 1.2))
                print(f"Batch traité en {elapsed:.1f}s, batch_size: {batch_size_events}")
                temp_rows = []
        if temp_rows:
            self._process_batch(temp_rows)

    def _process_batch(self, rows_batch, database_id=None):

        df = pd.DataFrame(rows_batch)
        if database_id is not None:
            df = df[df["database_id"] == database_id]
        if df.empty:
            return
        dwh_ids= df["database_id"].dropna().unique().tolist()
        contacts_map = self.resilient_call(self.recupere_contacts, dwh_ids)
        ages = df["dwh_id"].map(lambda x: contacts_map.get(str(x), {}).get("age", None))
        df["age_range"] = (
            pd.cut(pd.to_numeric(ages, errors="coerce"),
                   bins=AGE_BINS, labels=AGE_LABELS, right=False)
            .astype(str)
            .replace("nan", "O_age"))
        df["gender"]         = df["dwh_id"].map(lambda x: contacts_map.get(str(x), {}).get("gender")   or "O_gender")
        df["main_isp"]       = df["dwh_id"].map(lambda x: contacts_map.get(str(x), {}).get("main_isp") or "O_isp")
        df["zipcode"]        = df["dwh_id"].map(lambda x: contacts_map.get(str(x), {}).get("zipcode")  or "zipcode_vide")
        df["dep"]            = df["dwh_id"].map(lambda x: contacts_map.get(str(x), {}).get("dep")      or "dep_vide")
        df["age_gender_isp"] = df["age_range"] + "_" + df["gender"] + "_" + df["main_isp"]
        del contacts_map  
        database_ids = df["database_id"].dropna().unique().tolist()
        db_map       = self.resilient_call(self.recupere_ktk_id, database_ids)
        df["ktk_id"]  = df["database_id"].map(lambda x: db_map.get(str(x), {}).get("ktk_id",   "ktk_vide"))
        df["basename"]= df["database_id"].map(lambda x: db_map.get(str(x), {}).get("basename", "base_vide"))
        df["country"] = df["database_id"].map(lambda x: db_map.get(str(x), {}).get("country",  0))
        del db_map
        optimize_params = (
            df[["id_routers", "id_focus", "ktk_id"]]
            .drop_duplicates()
            .to_dict("records")
        )
        optimized_map = self.resilient_call(self.recuper_optimize, optimize_params)
        df["optimized"] = df.apply(
            lambda r: optimized_map.get(
                (str(self.safe(r["id_routers"])),
                 str(self.safe(r["id_focus"])),
                 str(self.safe(r["ktk_id"]))),
                "url_vide",
            ), axis=1,
        )
        del optimized_map
        df_grouped = df.groupby(GROUP_COLS, observed=True).agg(
            sends         = ("sends",         "sum"),
            opens         = ("opens",         "sum"),
            openers       = ("openers",        "max"),
            clicks        = ("clicks",        "sum"),
            clickers      = ("clickers",       "max"),
            unsubs        = ("unsubs",        "sum"),
            complaints    = ("complaints",    "sum"),
            bounces       = ("bounces",       "sum"),
            ca            = ("ca",            "max"),
            date_schedule = ("date_schedule",
                             lambda x: sorted({d for sub in x if isinstance(sub, list) for d in sub})),
        ).reset_index()
        del df 
        df_grouped["updated_at"] = datetime.now()
        df_grouped = df_grouped[COLUMNS_FINAL]
        for col in INT_COLS:
            df_grouped[col] = pd.to_numeric(df_grouped[col], errors="coerce").fillna(0).astype(np.uint32)
        for col in STR_COLS:
            df_grouped[col] = df_grouped[col].astype("string").fillna("")
        df_grouped["brand"]= df_grouped["brand"].astype("string").fillna("brand_vide")
        df_grouped["subject"]    = df_grouped["subject"].fillna("O_objet")
        df_grouped["updated_at"] = pd.to_datetime(df_grouped["updated_at"])
        df_grouped["date_event"] = pd.to_datetime(df_grouped["date_event"]).fillna(datetime.now())
        total = len(df_grouped)
        for start in range(0, total, BATCH_INSERT):
            self.clk.insert_df(self.table, df_grouped.iloc[start:start + BATCH_INSERT])

        print(f"Données insérées : {total} lignes")
        del df_grouped
