from enum import Flag
from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
from datetime import datetime, timedelta,date
import time, math, requests, psutil
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text
import pandas as pd
import json
import os
import numpy as np
class reporting2:
    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.pg = PgConfig().get_client()
        self.table = "dev_reporting_agg"
        today = datetime.today()
        self.date_end = today.date()
        self.date_start = datetime(year=today.year-1, month=7, day=1).date()
        self.batch_adv_size = 50
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
       print(f"Succès:{message}")

    def notifier_erreur(self, message):
        print(f"Erreur:{message}")

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
    
    def recupere_pg(self, batch=1000):
        query = text("""
            SELECT vd.id AS id_focus,
                vd.campaingkind AS ca,
                COALESCE(json_agg(DISTINCT vd.date_shedule), '[]'::json) AS date_shedule,
                COALESCE(json_agg(DISTINCT idsendouts.idsendout), '[]'::json) AS id_routers
            FROM visu.v2_data vd
            JOIN visu.v2_status st ON st.id = vd.status
            LEFT JOIN LATERAL (
                SELECT vd1.idsendout
                FROM (
                    SELECT vd.idsendout
                    UNION
                    SELECT vd2.idsendout
                    FROM visu.v2_data vd2
                    WHERE vd2.id = ANY (
                        SELECT vdr2.id_reuse
                        FROM visu.v2_data_reuse vdr2
                        WHERE vdr2.id_v2 = vd.id
                    )
                    AND vd2.idsendout IS NOT NULL
                ) AS vd1
            ) AS idsendouts ON TRUE
            WHERE st.id = 5
            GROUP BY vd.campaingkind, vd.id
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
                        id_focus, ca, date_shedule, id_routers_list = row
                        print(row)
                        if not id_routers_list:
                            continue

                        if isinstance(id_routers_list, str):
                            continue 

                        for id_r in id_routers_list:
                            if id_r is None:
                                continue

                            pg_map[str(id_r)] = {
                                "id_focus": str(id_focus),
                                "ca": ca,
                                "date_shedule": date_shedule or []
                            }
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
                e.brand, e.client_id, e.ListId,e.affiliate_id
            FROM events e
            INNER JOIN (
                SELECT MessageId, event_type, max(run_id) AS max_run_id
                FROM events
                WHERE MessageId IN ({batch_str})
                AND Date BETWEEN '{self.date_start}' AND '{self.date_end}'
                GROUP BY MessageId, event_type,database_id ORDER BY database_id ASC
            ) m
            ON e.MessageId = m.MessageId 
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
                FROM prod_contacts
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
            query = f"SELECT id AS database_id, ktk_id, basename,country FROM databases WHERE id IN ({ids})"
            r = self.clk.query(query)
            db_map = {}
            for row in r.result_rows:
                db_id = str(row[0])
                db_map[db_id] = {"ktk_id": row[1] or "ktk_vide", "basename": row[2] or "base_vide", "country":row[3] or "country_vide"}
            return db_map
        except Exception as e:
            self.notifier_erreur(f"Erreur ktk_id : {e}")
            return {}
    def recuper_optimize(self, rows_list, chunck=50):
        endpoint = "https://konticreav2.kontikimedia.fr:5009/api/creativities/filter-plannifik"

        session = requests.Session()
        retry_strategy = Retry(
            total=3, 
            backoff_factor=1, 
            status_forcelist=[429, 500, 502, 503, 504], 
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
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
                    resp = self.resilient_call(
                        session.post,
                        endpoint,
                        params={"focus_id": id_focus, "base_id": ktk_id, "router_id": id_routers},
                        timeout=20
                    )
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
        batch_size_events = 10000
        seen_openers = set()
        seen_clickers = set()
        temp_rows = []
        print("Récupération events")
        for row in self.recupere_events(id_routers):
            focus_data = focus_map.get(str(row.get("id_routers")))
            if not focus_data:
                continue  
            row.update(focus_data)
            ev = row.get('event_type')
            row["sends"] = 1 if ev == "Sends" else 0
            row["opens"] = 1 if ev == "Opens" else 0
            row["clicks"] = 1 if ev == "Clicks" else 0
            row["unsubs"] = 1 if ev == "Removals" else 0
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

    def _process_batch(self, rows_batch, database_id=19):
        df_final = pd.DataFrame(rows_batch)
        if database_id is not None:
            df_final = df_final[df_final["database_id"] == database_id]
        if df_final.empty:
            return
        filtered_rows = df_final.to_dict("records")
        dwh_ids = list({r["dwh_id"] for r in filtered_rows if r.get("dwh_id")})
        contacts_map = self.resilient_call(self.recupere_contacts, dwh_ids)
        bins = [0,18,25,35,45,55,65,75,float("inf")]
        labels = ['0-18','18-24','25-34','35-44','45-54','55-64','65-74','75+']
        def get_age_range(age):
            try:
                age = int(age)
                return next(labels[i] for i in range(len(bins)-1) if bins[i] <= age < bins[i+1])
            except:
                return "O_age"
        for row in filtered_rows:
            contact = contacts_map.get(str(row.get("dwh_id")), {})
            row["age_range"] = get_age_range(contact.get("age"))
            row["gender"] = contact.get("gender") or "O_gender"
            row["main_isp"] = contact.get("main_isp") or "O_isp"
            row["zipcode"]=contact.get("zipcode") or "zipcode_vide"
            row["dep"]=contact.get("dep") or "dep_vide"
            row["age_gender_isp"]=f"{row["age_range"]}_{row["gender"]}_{row["main_isp"]}"
        database_ids = list({r.get("database_id") for r in filtered_rows})
        db_map = self.resilient_call(self.recupere_ktk_id, database_ids)
        for row in filtered_rows:
            row.update(db_map.get(str(row.get("database_id")), {
                "ktk_id": "ktk_vide",
                "basename": "base_vide",
                "country":"country_vide"
            }))
        optimize_keys = {
            (str(r.get("id_routers")), str(r.get("id_focus")), str(r.get("ktk_id")))
            for r in filtered_rows
        }
        optimize_params = [
            {"id_routers": k[0], "id_focus": k[1], "ktk_id": k[2]}
            for k in optimize_keys
        ]
        optimized_map = self.resilient_call(self.recuper_optimize, optimize_params, chunck=50)
        for r in filtered_rows:
            key = (str(r.get("id_routers")), str(r.get("id_focus")), str(r.get("ktk_id")))
            r["optimized"] = optimized_map.get(key, "url_vide")
        df_final = pd.DataFrame(filtered_rows)
        group_cols = [
            "database_id","segmentId","subject","adv_id","id_routers","tag_id","brand","age_range","gender","main_isp","age_gender_isp","optimized","country","ListId","zipcode","dep","affiliate_id"
        ]
        df_grouped = df_final.groupby(group_cols, observed=True).agg(
            sends=("sends","sum"),
            opens=("opens","sum"),
            openers=("openers","max"),
            clicks=("clicks","sum"),
            clickers=("clickers","max"),
            unsubs=("unsubs","sum"),
            complaints=("complaints","sum"),
            bounces=("bounces","sum"),
            ca=("ca","max"),
            date_shedule=("date_shedule", lambda x: sorted({d for sub in x if isinstance(sub, list) for d in sub}))
        ).reset_index()
        df_grouped['updated_at']=datetime.now()
        columns_final = [
            "database_id","country","segmentId","subject","brand","tag_id","adv_id","id_routers","affiliate_id","ListId","zipcode","dep","sends","opens","openers",
            "clicks","clickers","unsubs","age_range","gender","main_isp","age_gender_isp","ca","date_shedule","optimized","updated_at"]
        df_grouped = df_grouped[columns_final]
        def prepare_for_clickhouse(df_grouped):
            df_grouped = df_grouped.copy()
            int_cols = [
                "database_id", "segmentId","tag_id", "adv_id","sends", "opens", "openers", "clicks", "clickers", "unsubs","ca","ListId","affiliate_id","country","affiliate_id"]
            for col in int_cols:
                df_grouped[col] = df_grouped[col].fillna(0).astype(np.uint32)
            df_grouped["brand"] = df_grouped["brand"].astype("string").fillna("brand_vide")
            df_grouped["subject"] = df_grouped["subject"].fillna("O_objet")
            str_cols = ["id_routers","age_range","gender","main_isp","optimized","subject","zipcode","dep"]
            for col in str_cols:
                df_grouped[col] = df_grouped[col].astype("string").fillna("")
            df_grouped["updated_at"] = pd.to_datetime(df_grouped["updated_at"]).fillna(datetime.now())
            return df_grouped
        df_grouped = prepare_for_clickhouse(df_grouped)
        chunk_size = 1000
        for start in range(0, len(df_grouped), chunk_size):
            end = start + chunk_size
            chunk_df = df_grouped.iloc[start:end]
            self.clk.insert_df(self.table, chunk_df)
        #df_grouped.to_csv( f"reports_{database_id}1.csv", mode="a", header=not os.path.exists(f"reports_{database_id}1.csv"), index=False, sep=";", encoding="utf-8" )
        #df_grouped.to_csv("testes.csv",index=False,sep=';')
        print(f"Données insérées : {len(df_grouped)} lignes")
        