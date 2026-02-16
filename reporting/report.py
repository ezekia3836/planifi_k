from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
from datetime import timedelta, datetime
import pandas as pd
import time
import os
from sqlalchemy import text
from models.Events import Events
import requests
import json
class reporting:

    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.pg = PgConfig().get_client()
        self.table = "reporting"
        self.date_end = pd.to_datetime("today").date()
        self.date_start = self.date_end - timedelta(days=90)
        self.batch_adv_size = 50
        self.adv_ids =[54] #Events().get_adv_ids()
    def recupere_ktk_id(self, databases_ids):
        if not databases_ids:
            return pd.DataFrame(columns=['database_id', 'ktk_id'])

        try:
            
            ids = ",".join(str(x) for x in databases_ids)
            query = f"""SELECT id AS database_id, ktk_id,basename FROM databases WHERE id IN ({ids})"""
            
            r = self.clk.query(query)
            
            if not r.result_rows:
                return pd.DataFrame(columns=['database_id', 'ktk_id','basename'])
            
            df = pd.DataFrame(r.result_rows, columns=r.column_names)
            return df

        except Exception as e:
            print("Erreur lors de la récupération ktk_id :", e)
            
            return pd.DataFrame(columns=['database_id', 'ktk_id','basename'])


    def recuper_optimize(self, df_unique, batch_size=10):
        
        endpoint = "https://konticreav2.kontikimedia.fr:5009/api/creativities/filter-plannifik"
        df_unique = df_unique.copy()
        for col in ["id_focus", "ktk_id", "id_routers"]:
            df_unique[col] = df_unique[col].astype(str).str.strip()

        optimized_list = []
        for i in range(0, len(df_unique), batch_size):
            batch = df_unique.iloc[i:i+batch_size]

            for _, row in batch.iterrows():
                params = [
                        ("focus_id", int(row["id_focus"])),
                        ("base_id", int(row["ktk_id"])),
                        ("router_id",int(row["id_routers"]))
                    ]
                
                try:
                    resp = requests.post(endpoint, params=params, timeout=30)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("data"):
                            opt_value = next(
                                (item.get("optimized") for item in data["data"] if item.get("optimized")),"url_vide")
                            optimized_list.append(opt_value)
                        else:
                            optimized_list.append("url_vide")
                    else:
                        print(f"Erreur API {resp.status_code}: {resp.text}")
                        optimized_list.append("url_vide")
                except Exception as e:
                    print(f"Erreur récupération optimize pour {row['id_focus']}, {row['ktk_id']}, {row['id_routers']}: {e}")
                    optimized_list.append("url_vide")

        df_unique["optimized"] = optimized_list
        return df_unique
    def clean_adv_ids(self, adv_ids):
        return list(set([str(x).strip() for x in adv_ids if str(x).strip().isdigit()]))
    def recupere_events(self, adv_ids):
        adv_ids_clean = self.clean_adv_ids(adv_ids)
        if not adv_ids_clean:
            return pd.DataFrame()
        df_all = []
        for i in range(0, len(adv_ids_clean), self.batch_adv_size):
            batch = adv_ids_clean[i:i+self.batch_adv_size]
            batch_str = ",".join(f"{x}" for x in batch)
            query = f"""
                       SELECT
                e.database_id,
                e.MessageId      AS id_routers,
                e.adv_id,
                e.dwh_id,
                e.SegmentId      AS segmentId,
                e.MessageSubject AS subject,
                e.event_type,
                e.Date           AS date_event,
                e.tag            AS tag_id,
                e.brand,
                e.client_id,
                e.ListId
            FROM events_2 e
            INNER JOIN (
                SELECT
                    MessageId,
                    event_type,
                    max(run_id) AS max_run_id
                FROM events_2
                WHERE adv_id IN ({batch_str})
                AND Date BETWEEN '{self.date_start}' AND '{self.date_end}'
                GROUP BY MessageId, event_type
            ) m
            ON e.MessageId = m.MessageId
            AND e.event_type = m.event_type
            AND e.run_id = m.max_run_id
            WHERE e.adv_id IN ({batch_str})
            AND e.Date BETWEEN '{self.date_start}' AND '{self.date_end}'
            """
            try:
                r = self.clk.query(query)
                df = pd.DataFrame(r.result_rows, columns=r.column_names)
                if not df.empty:
                    df_all.append(df)
            except Exception as e:
                print(f"Erreur ClickHouse events batch {batch_str}: {e}")

        if not df_all:
            return pd.DataFrame()
        df_events = pd.concat(df_all, ignore_index=True)
        return df_events

    def recupere_contacts(self, dwh_ids, max_retry=3, sleep_sec=2, batch_size=5000):
        cols = ["dwh_id", "age", "gender", "main_isp", "zipcode", "dep"]

        if not dwh_ids:
            return pd.DataFrame(columns=cols)

        all_dfs = []
        n = len(dwh_ids)

        for i in range(0, n, batch_size):
            batch_ids = dwh_ids[i:i + batch_size]
            batch_str = ",".join(f"'{str(x)}'" for x in batch_ids)
            last_exception = None

            for attempt in range(1, max_retry + 1):
                try:
                    query = f"""
                        SELECT
                            dwh_id,
                            age,
                            gender,
                            main_isp,
                            zipcode,
                            dep
                        FROM contacts_2
                        PREWHERE dwh_id IN ({batch_str})
                        ORDER BY updated_at DESC
                        LIMIT 1 BY dwh_id
                        SETTINGS optimize_read_in_order = 1
                    """
                    r = self.clk.query(query)
                    df = pd.DataFrame(r.result_rows, columns=r.column_names)

                    if not df.empty:
                        
                        for c in cols:
                            if c not in df.columns:
                                df[c] = None
                        df["dwh_id"] = df["dwh_id"].astype(str)
                        all_dfs.append(df)

                    break  

                except Exception as e:
                    last_exception = e
                    import traceback
                    print(f"Tentative {attempt}/{max_retry} échouée : {e}")
                    traceback.print_exc() 
                    time.sleep(sleep_sec)
                    print(f"Tentative {attempt}/{max_retry} échouée : {e}")
                    time.sleep(sleep_sec)

            else:
                raise RuntimeError(f"recupere_contacts a échoué pour le batch {batch_ids}") from last_exception

        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame(columns=cols)

    def recupere_pg(self, adv_ids):
        adv_ids_clean = self.clean_adv_ids(adv_ids)
        if not adv_ids_clean:
            return pd.DataFrame(columns=["id_routers","ca","date_shedule"])

        adv_ids_str = ",".join(f"'{x}'" for x in adv_ids_clean)

        query = text(f"""
            SELECT
                vd.id AS id_focus,
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
            AND vd.advertiser IN ({adv_ids_str})
            GROUP BY vd.campaingkind,vd.id
        """)
        try:
            with self.pg.connect() as conn:
                df = pd.read_sql(query, conn)
            df["date_shedule"] = df["date_shedule"].apply(lambda x: x or [])
            df["id_routers"] = df["id_routers"].apply(lambda x: x or [])
            df = df.explode("id_routers")
            df["id_routers"] = df["id_routers"].astype(str)
            return df[["id_focus","id_routers","ca","date_shedule"]]
        except Exception as e:
            print("Erreur Postgres:", e)
            return pd.DataFrame(columns=["id_focus","id_routers","ca","date_shedule"])

    def report(self,journal="journal.txt",batch_optimize=10):
        df_final_all = []
        if os.path.exists(journal):
            with open(journal,'r') as f:
                process_adv = set(int(line.strip()) for line in f.readlines())
        else:
            process_adv = set()

        for adv_id in self.adv_ids:
            if adv_id in process_adv:
                continue
            print(f"Traitement advertiser {adv_id} ........")
            df_events = self.recupere_events([adv_id])
            if df_events.empty:
                print(f"Aucun événement pour advertiser {adv_id}")
                continue
            event_types = ["Sends", "Opens", "Clicks", "Removals", "Complaints", "Bounces"]
            for ev in event_types:
                df_events[ev.lower()] = (df_events["event_type"] == ev).astype(int)
            df_opens = df_events[df_events['event_type'] == 'Opens'][['adv_id', 'id_routers', 'dwh_id']].drop_duplicates()
            df_opens['opener'] = 1

            df_clicks = df_events[df_events['event_type'] == 'Clicks'][['adv_id', 'id_routers', 'dwh_id']].drop_duplicates()
            df_clicks['clicker'] = 1

            df_events = df_events.merge(df_opens, on=['adv_id','id_routers','dwh_id'], how='left')
            df_events = df_events.merge(df_clicks, on=['adv_id','id_routers','dwh_id'], how='left')

            df_events['opener'] = df_events['opener'].fillna(0).astype(int)
            df_events['clicker'] = df_events['clicker'].fillna(0).astype(int)
            dwh_ids = df_events["dwh_id"].dropna().unique().tolist()
            df_contacts = self.recupere_contacts(dwh_ids)
            df_pg = self.recupere_pg([adv_id])
            if df_pg.empty:
                df_pg = pd.DataFrame(columns=["id_routers", "ca", "date_shedule"])
            
            if not df_pg.empty:
                df_pg_grouped = df_pg.groupby(["id_focus","id_routers"], observed=True).agg(
                    ca=("ca", "max"),
                    date_shedule=("date_shedule", lambda x: sorted({d for sub in x if isinstance(sub, list) for d in sub}))
                ).reset_index()
            else:
                df_pg_grouped = pd.DataFrame(columns=["id_routers", "ca", "date_shedule"])
            df_events["id_routers"] = df_events["id_routers"].astype(str).fillna("O_router")
            df_pg_grouped["id_routers"] = df_pg_grouped["id_routers"].astype(str).fillna("O_router")

            df = df_events.merge(df_contacts, on="dwh_id", how="left")
            df = df.merge(df_pg_grouped, on="id_routers", how="left")
            bins = [0, 18, 24, 34, 44, 54, 64, 74, 200]
            labels = ['0-18', '18-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75+']
            df["age_range"] = pd.cut(df["age"], bins=bins, labels=labels)
            df["age_range"] = df["age_range"].cat.add_categories("O_age").fillna("O_age")
            df["gender"] = df["gender"].fillna("O_gender").replace("O", "O_gender")
            df["main_isp"] = df["main_isp"].fillna("O_isp").replace("Other", "O_isp")
            df["age_civilite_isp"] = df["age_range"].astype(str) + "_" + df["gender"].astype(str) + "_" + df["main_isp"].astype(str)
            df["ca"] = df["ca"].astype(float).fillna(0.0)
            for col in ["zipcode", "dep"]:
                if col not in df.columns:
                    df[col] = "Inconnu"
                df[col] = df[col].astype(str)
            database_ids = df["database_id"].dropna().unique().tolist()
            df_db = self.recupere_ktk_id(database_ids)
           
            df = df.merge(df_db, on="database_id", how="left")
            df["ktk_id"] = df["ktk_id"].fillna("ktk_vide")
            df['basename']=df['basename'].fillna("base_vide")
            for col in ["id_focus","id_routers","ktk_id"]:
                df[col] = df[col].astype(str)
            df_unique = df[['id_routers', 'id_focus', 'ktk_id']].drop_duplicates()
            df_unique = self.recuper_optimize(df_unique, batch_size=batch_optimize)
            df = df.merge(df_unique[['id_routers', 'id_focus', 'ktk_id', 'optimized']],
                        on=['id_routers', 'id_focus', 'ktk_id'], how='left')
            df["optimized"] = df["optimized"].fillna("url_vide")
            df["date_shedule"] = df["date_shedule"].apply(lambda x: x if isinstance(x, list) else [])
            group_cols = ["database_id","basename","ktk_id","segmentId", "adv_id","id_focus","id_routers", "tag_id", "brand","client_id","ListId", "zipcode", "dep","age_range", "gender", "main_isp", "age_civilite_isp"]
            df_grouped = df.groupby(group_cols, observed=True).agg(
                sends=("sends", "sum"),
                opens=("opens", "sum"),
                openers=("opener","max"),
                clicks=("clicks", "sum"),
                clickers=("clicker","max"),
                removals=("removals", "sum"),
                complaints=("complaints", "sum"),
                bounces=("bounces", "sum"),
                ca=("ca", "max"),
                subject=("subject", "first"),
                optimized=("optimized","first"),
                date_event=("date_event", "first"),
                date_shedule=("date_shedule", lambda x: sorted({d for sub in x if isinstance(sub, list) for d in sub}))
            ).reset_index()
            df_grouped = df_grouped[df_grouped['sends'] > 0].reset_index(drop=True)
            df_grouped['updated_at'] = datetime.now()
            with open(journal, "a") as f:
                f.write(f"{adv_id}\n")
                process_adv.add(adv_id)
                df_final_all.append(df_grouped)
            print(f"Insertion de l'advertiser .....")
            
            if not df_grouped.empty:
                #df_grouped.to_csv('groupe.csv',index=False,sep=';')
                batch_size = 5000
                for i in range(0, len(df_grouped), batch_size):
                    chunk = df_grouped[i:i+batch_size]
                    self.clk.insert_df(self.table, chunk)
            else:
                continue
            print("Insertion terminée")
            time.sleep(3)
        if df_final_all:
            return pd.concat(df_final_all, ignore_index=True)
        return pd.DataFrame()

    def run(self, max_retry=5, sleep_sec=5):
        last_exception = None
        for attempt in range(1, max_retry + 1):
            try:
                return self.report()
            except Exception as e:
                last_exception = e
                print(f"Erreur tentative {attempt} : {e}")
                if attempt < max_retry:
                    print(f"Nouvelle tentative dans {sleep_sec}s...\n")
                    time.sleep(sleep_sec)
        raise RuntimeError(f"report() a échoué après {max_retry} tentatives") from last_exception
