from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
from datetime import timedelta, datetime
import pandas as pd
import time
import uuid
from sqlalchemy import text

class reporting:

    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.pg = PgConfig().get_client()
        self.table = "reporting"
        self.date_end = pd.to_datetime("today").date()
        self.date_start = self.date_end - timedelta(days=90)
        self.batch_adv_size = 50
        self.adv_ids = [4987] 

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
                WITH last_runs AS (
                    SELECT 
                        adv_id,
                        MessageId,
                        event_type,
                        MAX(run_id) AS max_run
                    FROM events
                    WHERE adv_id IN ({batch_str})
                    AND Date BETWEEN '{self.date_start}' AND '{self.date_end}'
                    GROUP BY adv_id, MessageId, event_type
                )
                SELECT
                    e.database_id,
                    e.MessageId AS id_routers,
                    e.adv_id,
                    e.dwh_id,
                    e.SegmentId AS segmentId,
                    e.MessageSubject AS subject,
                    e.event_type,
                    e.Date AS date_event,
                    e.tag AS tag_id,
                    e.brand
                FROM events e
                INNER JOIN last_runs lr
                    ON e.adv_id = lr.adv_id
                AND e.MessageId = lr.MessageId
                AND e.event_type = lr.event_type
                AND e.run_id = lr.max_run
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
        return pd.concat(df_all, ignore_index=True)

    def recupere_contacts(self, dwh_ids, max_retry=3, sleep_sec=2, insert_chunk=5000):
        if not dwh_ids:
            return pd.DataFrame(
                columns=["dwh_id", "age", "gender", "main_isp", "zipcode", "dep"]
            )

        last_exception = None

        for attempt in range(1, max_retry + 1):
            temp_table = f"tmp_dwh_{uuid.uuid4().hex}"

            try:
                self.clk.command(f"""
                    CREATE TEMPORARY TABLE {temp_table}
                    (
                        dwh_id String
                    )
                    ENGINE = Memory
                """)

                for i in range(0, len(dwh_ids), insert_chunk):
                    chunk = dwh_ids[i:i + insert_chunk]
                    values = ",".join(f"('{str(x)}')" for x in chunk)
                    self.clk.command(f"INSERT INTO {temp_table} VALUES {values}")

                query = f"""
                    SELECT
                        t.dwh_id,
                        argMax(age, updated_at) AS age,
                        ifNull(argMax(gender, updated_at), 'O_gender') AS gender,
                        ifNull(argMax(main_isp, updated_at), 'O_isp') AS main_isp,
                        ifNull(argMax(zipcode, updated_at), 'O_zipcode') AS zipcode,
                        ifNull(argMax(dep, updated_at), 'O_departement') AS dep
                    FROM contacts c
                    RIGHT JOIN {temp_table} t ON c.dwh_id = t.dwh_id
                    GROUP BY t.dwh_id
                """

                r = self.clk.query(query)
                df = pd.DataFrame(r.result_rows, columns=r.column_names)
                return df

            except Exception as e:
                last_exception = e
                print(f"Tentative {attempt}/{max_retry} échouée : {e}")
                time.sleep(sleep_sec)

            finally:
                try:
                    self.clk.command(f"DROP TABLE IF EXISTS {temp_table}")
                except:
                    pass

        raise RuntimeError(f"recupere_contacts a échoué après {max_retry} tentatives") from last_exception

    def recupere_pg(self, adv_ids):
        adv_ids_clean = self.clean_adv_ids(adv_ids)
        if not adv_ids_clean:
            return pd.DataFrame(columns=["id_routers","ca","date_shedule"])

        adv_ids_str = ",".join(f"'{x}'" for x in adv_ids_clean)

        query = text(f"""
            SELECT
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
            GROUP BY vd.campaingkind
        """)
        try:
            with self.pg.connect() as conn:
                df = pd.read_sql(query, conn)
            df["date_shedule"] = df["date_shedule"].apply(lambda x: x or [])
            df["id_routers"] = df["id_routers"].apply(lambda x: x or [])
            df = df.explode("id_routers")
            df["id_routers"] = df["id_routers"].astype(str)
            return df[["id_routers","ca","date_shedule"]]
        except Exception as e:
            print("Erreur Postgres:", e)
            return pd.DataFrame(columns=["id_routers","ca","date_shedule"])

    def report(self):
        print('advertisers', self.adv_ids)

        print("Récupération EVENTS")
        df_events = self.recupere_events(self.adv_ids)
        if df_events.empty:
            print("Aucun événement récupéré")
            return pd.DataFrame()

        print("Récupération CONTACTS")
        dwh_ids = df_events["dwh_id"].dropna().unique().tolist()
        df_contacts = self.recupere_contacts(dwh_ids)
        if df_contacts.empty:
            print("aucun contact")
        print("Récupération focus")
        df_pg = self.recupere_pg(self.adv_ids)
        if df_pg.empty:
            df_pg = pd.DataFrame(columns=["id_routers","ca"])

        df_events["id_routers"] = df_events["id_routers"].astype(str).fillna("O_router")
        df_pg["id_routers"] = df_pg["id_routers"].astype(str).fillna("O_router")
        df = df_events.merge(df_contacts,on="dwh_id",how="left")
        df = df.merge(df_pg, on="id_routers", how="left")
        bins = [0,18,24,34,44,54,64,74,200]
        labels = ['0-18','18-24','25-34','35-44','45-54','55-64','65-74','75+']
        df["age_range"] = pd.cut(df["age"], bins=bins, labels=labels)
        df["age_range"] = df["age_range"].cat.add_categories("O_age").fillna("O_age")
        df["gender"] = df["gender"].fillna("O_gender").replace("O","O_gender")
        df["main_isp"] = df["main_isp"].fillna("O_isp").replace("Other","O_isp")
        df["age_civilite_isp"] = df["age_range"].astype(str) + "_" + df["gender"].astype(str) + "_" + df["main_isp"].astype(str)
        df["ca"] = df["ca"].fillna(0.0)
        for col in ["zipcode", "dep"]:
            df[col] = df[col].astype(str)
            if col not in df.columns:
                df[col] = "Inconnu"
        event_types = ["Sends","Opens","Clicks","Removals","Complaints","Bounces"]
        for ev in event_types:
            df[ev.lower()] = (df["event_type"] == ev).astype(int)
        group_cols = ["database_id","segmentId","adv_id","id_routers","tag_id","brand",
                    "age_range","gender","main_isp","age_civilite_isp","zipcode","dep"]

        df["date_shedule"] = df["date_shedule"].apply(lambda x: x if isinstance(x, list) else [])

        df_grouped = df.groupby(group_cols, observed=True).agg(
            sends=("sends","sum"),
            opens=("opens","sum"),
            clicks=("clicks","sum"),
            removals=("removals","sum"),
            complaints=("complaints","sum"),
            bounces=("bounces","sum"),
            ca=("ca","max"),
            subject=("subject","first"),
            date_event=("date_event","first"),
            date_shedule=("date_shedule",lambda x: sorted({ d for sub in x if isinstance(sub, list) for d in sub}))
        ).reset_index()
        df_grouped = df_grouped[df_grouped['sends']>0].reset_index(drop=True)
        df_grouped['updated_at'] = datetime.now()
        print("INSERTION")
        df_grouped.to_csv('testes.csv',index=False,sep=';')

    def run(self, max_retry=3, sleep_sec=5):
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
