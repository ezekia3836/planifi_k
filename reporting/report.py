from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
from datetime import timedelta, datetime
import pandas as pd
import time
import os
from sqlalchemy import text
from models.Events import Events
class reporting:

    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.pg = PgConfig().get_client()
        self.table = "reporting"
        self.date_end = pd.to_datetime("today").date()
        self.date_start = self.date_end - timedelta(days=90)
        self.batch_adv_size = 50
        self.adv_ids =Events().get_adv_ids()

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
                    database_id,
                    MessageId      AS id_routers,
                    adv_id,
                    dwh_id,
                    SegmentId      AS segmentId,
                    MessageSubject AS subject,
                    event_type,
                    Date           AS date_event,
                    tag            AS tag_id,
                    brand,
                    client_id,
                    ListId
                FROM events_2
                WHERE adv_id IN ({batch_str})
                AND Date BETWEEN '{self.date_start}' AND '{self.date_end}'
                QUALIFY
                    run_id = max(run_id) OVER (PARTITION BY MessageId, event_type)
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

    def report(self,journal="journal.txt"):
        df_final_all = []
        if os.path.exists(journal):
            with open(journal,'r') as f:
                process_adv = set(int(line.strip()) for line in f.readlines())
        else:
            process_adv = set()

        for adv_id in self.adv_ids:
            if adv_id in process_adv:
                continue
            print(f"Traitement advertiser {adv_id}")
            print("recup events")
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
            print("recup contacts")
            df_contacts = self.recupere_contacts(dwh_ids)
            print("recup focus")
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

            df["date_shedule"] = df["date_shedule"].apply(lambda x: x if isinstance(x, list) else [])
            group_cols = ["database_id", "segmentId", "adv_id","id_focus","id_routers", "tag_id", "brand","client_id","ListId", "zipcode", "dep","age_range", "gender", "main_isp", "age_civilite_isp"]
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
                date_event=("date_event", "first"),
                date_shedule=("date_shedule", lambda x: sorted({d for sub in x if isinstance(sub, list) for d in sub}))
            ).reset_index()

            df_grouped = df_grouped[df_grouped['sends'] > 0].reset_index(drop=True)
            df_grouped['updated_at'] = datetime.now()
            with open(journal, "a") as f:
                f.write(f"{adv_id}\n")
                process_adv.add(adv_id)
                df_final_all.append(df_grouped)
                
            print(f"Insertion de l'advertiser: {adv_id}")
            if not df_grouped.empty:
                batch_size = 5000
                for i in range(0, len(df_grouped), batch_size):
                    chunk = df_grouped[i:i+batch_size]
                    self.clk.insert_df(self.table, chunk)
            else:
                continue
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
