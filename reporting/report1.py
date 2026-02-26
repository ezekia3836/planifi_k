from enum import Flag

from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
from datetime import datetime, timedelta
import time, math, requests, psutil
from winotify import Notification
from requests.adapters import HTTPAdapter, Retry
from sqlalchemy import text
import pandas as pd

class reporting1:
    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.pg = PgConfig().get_client()
        self.table = "reporting"
        today = datetime.today()
        self.date_end = today.date()
        self.date_start = datetime(year=today.year-1, month=7, day=1).date()
        self.batch_adv_size = 50
    def convert_types_for_clickhouse(self, df):
        import pandas as pd
        int_cols = [
            "database_id","ktk_id","segmentId","adv_id","id_focus","tag_id",
            "client_id","ListId","sends","opens","openers","clicks",
            "clickers","removals","complaints","bounces"
        ]

        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

        if "ca" in df.columns:
            df["ca"] = pd.to_numeric(df["ca"], errors="coerce").fillna(0.0).astype("float64")

        str_cols = [
            "dwh_id","basename","subject","id_routers","brand",
            "zipcode","dep","age_range","gender","main_isp",
            "age_civilite_isp","optimized"
        ]

        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].fillna("").astype("string")

        datetime_cols = ["date_event", "updated_at"]
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        if "date_shedule" in df.columns:
            df["date_shedule"] = df["date_shedule"].apply(
                lambda arr: [str(x) for x in arr] if isinstance(arr, (list, tuple)) else []
            )

        if "dep" in df.columns:
            df["dep"] = df["dep"].replace("", None)

        if "optimized" in df.columns:
            df["optimized"] = df["optimized"].replace("", None)

        return df
    def resilient_call(self, func, *args, max_retry=5, sleep_sec=5, backoff=True, **kwargs):
        attempt = 1
        wait = sleep_sec
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
        Notification(app_id="Reporting Planifik", title="✅ Succès", msg=message, duration="short").show()

    def notifier_erreur(self, message):
        Notification(app_id="Reporting Planifik", title="❌ Erreur", msg=message, duration="long").show()

    def safe(self, value):
        try:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return 0
            f = float(value)
            if not math.isfinite(f):
                return 0
            return int(f)
        except (ValueError, TypeError):
            return 0

    def clean_adv_ids(self, adv_ids):
        return list(set([str(x).strip() for x in adv_ids if str(x).strip().isdigit()]))
    
    def recupere_pg(self,batch=50):
        last_id=0
        query = text(f"""
            SELECT vd.id AS id_focus, vd.campaingkind AS ca,
                   COALESCE(json_agg(DISTINCT vd.date_shedule), '[]'::json) AS date_shedule,
                   COALESCE(json_agg(DISTINCT idsendouts.idsendout), '[]'::json) AS id_routers
            FROM visu.v2_data vd
            JOIN visu.v2_status st ON st.id = vd.status
            LEFT JOIN LATERAL (
                SELECT vd1.idsendout
                FROM (SELECT vd.idsendout
                      UNION
                      SELECT vd2.idsendout
                      FROM visu.v2_data vd2
                      WHERE vd2.id = ANY (SELECT vdr2.id_reuse FROM visu.v2_data_reuse vdr2 WHERE vdr2.id_v2 = vd.id)
                      AND vd2.idsendout IS NOT NULL) AS vd1
            ) AS idsendouts ON TRUE
            WHERE st.id = 5 AND vd.id
            GROUP BY vd.campaingkind, vd.id
        """)
        try:
            with self.pg.connect() as conn:
                result = self.resilient_call(lambda: conn.execute(query).fetchall())
            pg_map = {}
            for row in result:
                id_focus, ca, date_shedule, id_routers_list = row
                for id_r in id_routers_list or []:
                    pg_map[str(id_r)] = {"id_focus": str(id_focus), "ca": ca, "date_shedule": date_shedule or []}
            return pg_map
        except Exception as e:
            self.notifier_erreur(f"Erreur Focus: {e}")
            return {}

    def recupere_events(self, id_routers_focus):
        if not id_routers_focus:
            return
        for i in range(0, len(id_routers_focus), self.batch_adv_size):
            batch = id_routers_focus[i:i+self.batch_adv_size]
            batch_str = ",".join(str(x) for x in batch)
            query = f"""
           SELECT e.database_id, e.MessageId AS id_routers, e.adv_id, e.dwh_id,
                e.SegmentId AS segmentId, e.MessageSubject AS subject,
                e.event_type, e.Date AS date_event, e.tag AS tag_id,
                e.brand, e.client_id, e.ListId
            FROM events_2 e
            INNER JOIN (
                SELECT MessageId, event_type, max(run_id) AS max_run_id
                FROM events_2
                WHERE MessageId IN ({batch_str})
                AND Date BETWEEN '{self.date_start}' AND '{self.date_end}'
                GROUP BY MessageId, event_type ORDER BY adv_id ASC
            ) m
            ON e.MessageId = m.MessageId 
            AND e.event_type = m.event_type 
            AND e.run_id = m.max_run_id
            WHERE e.MessageId IN ({batch_str})
            AND e.Date BETWEEN '{self.date_start}' AND '{self.date_end}' ORDER BY e.adv_id ASC
            """
            try:
                r = self.resilient_call(self.clk.query, query)
                for row in r.result_rows:
                    yield dict(zip(r.column_names, row))
            except Exception as e:
                self.notifier_erreur(f"Erreur recup events batch {batch_str}: {e}")

    def recupere_contacts(self, dwh_ids, batch_size=2000):
        if not dwh_ids:
            return {}
        contacts_map = {}
        for i in range(0, len(dwh_ids), batch_size):
            batch = dwh_ids[i:i+batch_size]
            batch_str = ",".join(f"'{x}'" for x in batch)
            query = f"""
                SELECT dwh_id, argMax(age, updated_at) AS age, argMax(gender, updated_at) AS gender,
                       argMax(main_isp, updated_at) AS main_isp, argMax(zipcode, updated_at) AS zipcode,
                       argMax(dep, updated_at) AS dep
                FROM contacts_2
                WHERE dwh_id IN ({batch_str})
                GROUP BY dwh_id
            """
            try:
                r = self.resilient_call(self.clk.query, query)
                for row in r.result_rows:
                    contacts_map[str(row[0])] = dict(zip(r.column_names, row))
            except Exception as e:
                self.notifier_erreur(f"Erreur contacts batch: {e}")
        return contacts_map

    def recupere_ktk_id(self, database_ids):
        if not database_ids:
            return {}
        try:
            ids = ",".join(str(x) for x in database_ids)
            query = f"SELECT id AS database_id, ktk_id, basename FROM databases WHERE id IN ({ids})"
            r = self.clk.query(query)
            db_map = {}
            for row in r.result_rows:
                db_id = str(row[0])
                db_map[db_id] = {"ktk_id": row[1] or "ktk_vide", "basename": row[2] or "base_vide"}
            return db_map
        except Exception as e:
            self.notifier_erreur(f"Erreur ktk_id : {e}")
            return {}

    def recuper_optimize_direct(self, rows_list, chunck=50):
        endpoint = "https://konticreav2.kontikimedia.fr:5009/api/creativities/filter-plannifik"
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))
        optimized_map = {}
        for i in range(0, len(rows_list), chunck):
            batch = rows_list[i:i+chunck]
            for row in batch:
                id_routers = self.safe(row.get("id_routers"))
                id_focus = self.safe(row.get("id_focus"))
                ktk_id = self.safe(row.get("ktk_id"))
                if not id_routers or not id_focus or not ktk_id:
                    optimized_map[(str(id_routers), str(id_focus), str(ktk_id))] = "url_vide"
                    continue
                try:
                    resp = self.resilient_call(session.post, endpoint, params={"focus_id": id_focus, "base_id": ktk_id, "router_id": id_routers}, timeout=20)
                    data = resp.json() if resp.status_code == 200 else {}
                    opt = next((x.get("optimized") for x in data.get("data", []) if x.get("optimized")), "url_vide")
                except Exception as e:
                    self.notifier_erreur(f"Optimized erreur: {e}")
                    opt = "url_vide"
                optimized_map[(str(id_routers), str(id_focus), str(ktk_id))] = opt
            time.sleep(0.05)
        return optimized_map
    def report(self):
        print("Récupération Focus")
        focus_map = self.recupere_pg()
        id_routers = list(focus_map.keys())
        batch_size_events = 20000
        seen_openers=set()
        seen_clickers=set()
        temp_rows=[]

        print("Récupération events")

        for row in self.recupere_events(id_routers):
            focus_data = focus_map.get(str(row.get("id_routers")))
            if not focus_data:
                continue  
            row.update(focus_data)
            ev=row.get('event_type')
            row["sends"] = 1 if ev == "Sends" else 0
            row["opens"] = 1 if ev == "Opens" else 0
            row["clicks"] = 1 if ev == "Clicks" else 0
            row["removals"] = 1 if ev == "Removals" else 0
            row["complaints"] = 1 if ev == "Complaints" else 0
            row["bounces"] = 1 if ev == "Bounces" else 0
            key = (row.get("adv_id"), row.get("id_routers"), row.get("dwh_id"))
            row["openers"] = 1 if ev=="Opens" and key not in seen_openers else 0
            row["clickers"] = 1 if ev=="Clicks" and key not in seen_clickers else 0

            if row["openers"]: seen_openers.add(key)
            if row["clickers"]: seen_clickers.add(key)

            temp_rows.append(row)

            if len(temp_rows) >= batch_size_events:
                start_time = time.time()
                self._process_batch(temp_rows)
                elapsed = time.time() - start_time
                mem_ratio = psutil.virtual_memory().available / psutil.virtual_memory().total
                if elapsed>10 or mem_ratio<0.2:
                    batch_size_events = max(1000, int(batch_size_events*0.7))
                elif elapsed<5 and mem_ratio>0.5:
                    batch_size_events = min(50000, int(batch_size_events*1.2))
                print(f"Batch traité en {elapsed:.1f}s, batch_size: {batch_size_events}")
                temp_rows = []

        if temp_rows:
            self._process_batch(temp_rows)

        print("Traitement terminé")
    def _process_batch(self, rows_batch):
        dwh_ids = list({r["dwh_id"] for r in rows_batch if r.get("dwh_id")})
        contacts_map = self.resilient_call(self.recupere_contacts, dwh_ids)
        bins = [0,18,25,35,45,55,65,75,float("inf")]
        labels = ['0-18','18-24','25-34','35-44','45-54','55-64','65-74','75+']
        def get_age_range(age):
            try: age=int(age); return next(labels[i] for i in range(len(bins)-1) if bins[i]<=age<bins[i+1])
            except: return "O_age"

        for row in rows_batch:
            contact = contacts_map.get(str(row.get("dwh_id")), {})
            row["age_range"] = get_age_range(contact.get("age"))
            row["gender"] = contact.get("gender") or "O_gender"
            row["main_isp"] = contact.get("main_isp") or "O_isp"
            row["zipcode"] = contact.get("zipcode","Inconnu")
            row["dep"] = contact.get("dep","Inconnu")
            row["age_civilite_isp"] = f"{row['age_range']}_{row['gender']}_{row['main_isp']}"

        database_ids = list({r.get("database_id") for r in rows_batch})
        db_map = self.resilient_call(self.recupere_ktk_id, database_ids)
        for row in rows_batch:
            row.update(db_map.get(str(row.get("database_id")), {"ktk_id":"ktk_vide","basename":"base_vide"}))
        optimize_keys = {(str(r.get("id_routers")), str(r.get("id_focus")), str(r.get("ktk_id"))) for r in rows_batch}
        optimize_payload = [{"id_routers":k[0], "id_focus":k[1], "ktk_id":k[2]} for k in optimize_keys]
        optimized_map = self.resilient_call(self.recuper_optimize_direct, optimize_payload, batch_size=500)
        for r in rows_batch:
            key = (str(r.get("id_routers")), str(r.get("id_focus")), str(r.get("ktk_id")))
            r["optimized"] = optimized_map.get(key, "url_vide")
            r["updated_at"] = datetime.now()

        df_final = pd.DataFrame(rows_batch)

        columns_order=[
            "dwh_id","database_id","basename","ktk_id","segmentId","adv_id",
            "id_focus","subject","id_routers","tag_id","brand","client_id","ListId",
            "zipcode","dep","age_range","gender","main_isp","age_civilite_isp",
            "date_event","sends","opens","openers","clicks","clickers",
            "removals","complaints","bounces","ca","date_shedule","updated_at","optimized"
        ]
        for col in columns_order:
            if col not in df_final.columns: df_final[col]=None
        df_final = df_final[columns_order]
        group_cols = [
            "dwh_id","database_id","basename","ktk_id","segmentId","adv_id","id_focus",
            "id_routers","tag_id","brand","client_id","ListId","zipcode","dep",
            "age_range","gender","main_isp","age_civilite_isp"
        ]
        def merge_dates(series):
            dates = set()
            for sub in series:
                if isinstance(sub, list):
                    dates.update(d for d in sub if d)
            return sorted(dates)

        df_final = df_final.groupby(group_cols, observed=True).agg(
            sends=("sends", "sum"),
            opens=("opens", "sum"),
            openers=("openers", "max"),
            clicks=("clicks", "sum"),
            clickers=("clickers", "max"),
            removals=("removals", "sum"),
            complaints=("complaints", "sum"),
            bounces=("bounces", "sum"),
            ca=("ca", "max"),
            subject=("subject", "first"),
            optimized=("optimized","first"),
            date_event=("date_event", "first"),
            date_shedule=("date_shedule", merge_dates),
            updated_at=("updated_at","max")
        ).reset_index()
        df_final = self.convert_types_for_clickhouse(df_final)
        self.clk.insert_df(self.table, df_final)
        print(f"Lignes à insérer: {len(df_final)}")