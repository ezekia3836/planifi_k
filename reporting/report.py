from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
import numpy as np
import pandas as pd
from datetime import datetime,timedelta
from sqlalchemy import text
from reporting.analyze import analyse
from models.Events import Events
from reporting.build import build
events = Events()
class reporting:
    def __init__(self):
        self.clk= ClickHouseConfig().getClient()
        self.pst = PgConfig().get_client()
        self.date_end = datetime.today().date()
        self.analyze=analyse()
        self.build = build()
        self.advertiser=Events().get_adv_ids()
        self.adv_ids =9663 #",".join(map(str, self.advertiser))
        #self.db_id=1
        #self.bd_name="comptoirdesreducs.com"
        self.table = "reporting"
        self.date_start = self.date_end-timedelta(days=90)
        self.created_at =datetime.now()
    def recupere_pst(self,advertiser):
        try:
            engine=self.pst.connect()
            query = text(f"""
                    SELECT
                vd.id                        AS id,
                vd.namesendout               AS namesendout,
                vd.date_shedule::date        AS shedule_date,
                vd.advertiser,
                vd.base,
                vd.campaingkind AS ca,
                a.id    AS pg_id,
                a.name,

                COALESCE((
                    SELECT json_agg(DISTINCT idsendout)
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
                    ) t
                ), '[]'::json)               AS id_routers,
                (
                    COALESCE((
                        SELECT SUM(vd2.sent)
                        FROM visu.v2_data vd2
                        WHERE vd2.id = ANY (
                            SELECT vdr2.id_reuse
                            FROM visu.v2_data_reuse vdr2
                            WHERE vdr2.id_v2 = vd.id
                        )
                    ), 0) + COALESCE(vd.sent, 0)
                ) AS sent,
                
                (
                    COALESCE((
                        SELECT SUM(vd2.delivered)
                        FROM visu.v2_data vd2
                        WHERE vd2.id = ANY (
                            SELECT vdr2.id_reuse
                            FROM visu.v2_data_reuse vdr2
                            WHERE vdr2.id_v2 = vd.id
                        )
                    ), 0) + COALESCE(vd.delivered, 0)
                ) AS delivered,
                (
                    COALESCE((
                        SELECT SUM(vd2.opens)
                        FROM visu.v2_data vd2
                        WHERE vd2.id = ANY (
                            SELECT vdr2.id_reuse
                            FROM visu.v2_data_reuse vdr2
                            WHERE vdr2.id_v2 = vd.id
                        )
                    ), 0) + COALESCE(vd.opens, 0)
                ) AS opens,
                (
                    COALESCE((
                        SELECT SUM(vd2.openers)
                        FROM visu.v2_data vd2
                        WHERE vd2.id = ANY (
                            SELECT vdr2.id_reuse
                            FROM visu.v2_data_reuse vdr2
                            WHERE vdr2.id_v2 = vd.id
                        )
                    ), 0) + COALESCE(vd.openers, 0)
                ) AS openers,
                (
                    COALESCE((
                        SELECT SUM(vd2.clicks)
                        FROM visu.v2_data vd2
                        WHERE vd2.id = ANY (
                            SELECT vdr2.id_reuse
                            FROM visu.v2_data_reuse vdr2
                            WHERE vdr2.id_v2 = vd.id
                        )
                    ), 0) + COALESCE(vd.clicks, 0)
                ) AS clicks,

                (
                    COALESCE((
                        SELECT SUM(vd2.unsubs)
                        FROM visu.v2_data vd2
                        WHERE vd2.id = ANY (
                            SELECT vdr2.id_reuse
                            FROM visu.v2_data_reuse vdr2
                            WHERE vdr2.id_v2 = vd.id
                        )
                    ), 0) + COALESCE(vd.unsubs, 0)
                ) AS unsubs,

                (
                    COALESCE((
                        SELECT SUM(vd2.complains)
                        FROM visu.v2_data vd2
                        WHERE vd2.id = ANY (
                            SELECT vdr2.id_reuse
                            FROM visu.v2_data_reuse vdr2
                            WHERE vdr2.id_v2 = vd.id
                        )
                    ), 0) + COALESCE(vd.complains, 0)
                ) AS complains,
                (
                            COALESCE((
                        SELECT SUM(vd2.clickers)
                        FROM visu.v2_data vd2 WHERE vd2.id=ANY(
                        SELECT vdr2.id_reuse 
                        FROM visu.v2_data_reuse vdr2 
                        WHERE vdr2.id_v2=vd.id
                        )
                        ),0) + COALESCE(vd.clickers, 0)
                        ) AS clickers

            FROM visu.v2_data vd
            JOIN visu.advertiser a ON a.id = vd.advertiser
            JOIN visu.v2_status st ON st.id = vd.status

            WHERE
                st.id = 5
                AND vd.advertiser IN ({advertiser})
                AND vd.sent > 50
               AND vd.date_shedule BETWEEN DATE '{self.date_start}' AND DATE '{self.date_end}'
            """)
            with engine as conn:
                df_pg = pd.read_sql(query, conn)
                if df_pg.empty:
                    print("Données vide!!")
                return df_pg
        except Exception as e:
            print("Erreur df_pg",e)
        #return df.to_csv('total.csv',index=False,sep=';')
    def recupere_clk(self,advertiser):
        try:
            query=f"""SELECT
                e.database_id AS database_id,
                e.MessageId AS id_routers,
                e.adv_id AS adv_id,
                c.main_isp AS main_isp,
                c.gender AS gender,
                c.age AS age,
                e.date_event AS date_event, 
                e.event_type AS event_type,
                a.name AS advertiser_name,
                t.tag AS tag_name,
                t.id AS tag_id
                FROM events e
                LEFT JOIN contacts c
                    ON e.dwh_id = c.dwh_id
                LEFT JOIN advertiser a 
                    ON toUInt64(e.adv_id) = toUInt64(a.id )
                LEFT JOIN tags t ON e.tag IS NOT NULL AND toUInt64(e.tag) = toUInt64(t.id)
                WHERE e.adv_id IN ({advertiser}) 
                """
            result = self.clk.query(query)
            df= pd.DataFrame(result.result_rows, columns=result.column_names)
            if df.empty:
                print(f"Aucune events pour la campagne {self.advertiser}")
                cols = ['database_id','main_isp','gender','age','tranche_âges','Sends','Clicks','Opens','Removals','Complains','event_type','id_routers']
                df= pd.DataFrame(columns=cols)
            return df
        except Exception as e:
            print("erreur recup_clk",e)
    def report(self):
        rows = []

        df_clk = self.recupere_clk(advertiser=self.adv_ids)
        df_pg = self.recupere_pst(advertiser=self.adv_ids)

       
        df_pg = df_pg.explode('id_routers')
        df_clk['id_routers'] = df_clk['id_routers'].astype(str)
        df_pg['id_routers'] = df_pg['id_routers'].astype(str)
        df_pg = df_pg.rename(columns={'advertiser_id': 'adv_id'})

      
        df = df_clk.merge(df_pg, on='id_routers', how='inner')
        
       
        bins = [0,18,24,34,44,54,64,74,200]
        labels = ['0-18','18-24','25-34','35-44','45-54','55-64','65-74','75+']
        df['age_range'] = pd.cut(df['age'], bins=bins, labels=labels)
        df['age_range'] = df['age_range'].cat.add_categories('O_age').fillna('O_age').astype(str)

        df['gender'] = df['gender'].fillna('O_gender').replace({'O':'O_gender'})
        df['main_isp'] = df['main_isp'].fillna('O_isp').replace({'Other':'O_isp'})
        
        df["date_event"] = pd.to_datetime(df["date_event"])

        df["year"] = df["date_event"].dt.year
        df["month"] = df["date_event"].dt.month
        df["week"] = df["date_event"].dt.isocalendar().week
        df["day"] = df["date_event"].dt.weekday
        df["hour"] = df["date_event"].dt.hour

        for ev in ["Sends","Opens","Clicks","Removals","Complains"]:
            df[ev.lower()] = (df["event_type"] == ev).astype(int)

        dimensions = [
            ("gender", "civilite"),
            ("age_range", "age"),
            ("main_isp", "isp")
        ]

        for keys, g in df.groupby([
            "database_id",
            "adv_id",
            "advertiser_name",
            "id_routers",
            "tag_id",
            "tag_name",
            "year","month","week","day","hour"
        ]):

            (
                database_id, adv_id, advertiser_name,
                router, tag_id, tag_name,
                year, month, week, day, hour
            ) = keys

            ca_value = g["ca"].max(skipna=True)
            ca_value = float(ca_value) if pd.notna(ca_value) else 0.0
            for col, dim_name in dimensions:
                    agg = g.groupby(col).sum(numeric_only=True).reset_index()

                    for _, r in agg.iterrows():
                        rows.append({
                            "database_id": database_id,
                            "adv_id": adv_id,
                            "advertiser_name": advertiser_name,
                            "id_routers": router,
                            "tag_id": tag_id,
                            "tag_name": tag_name,
                            "dimensions": dim_name,
                            "valuedimension": str(r[col]),
                            "sends": int(r["sends"]),
                            "opens": int(r["opens"]),
                            "clicks": int(r["clicks"]),
                            "removals": int(r["removals"]),
                            "complains": int(r["complains"]),
                            "ca": float(ca_value),
                            "year": year,
                            "month": month,
                            "week": int(week),
                            "day": day,
                            "hour": hour,
                            "created_at": self.created_at
                        })
        multi = g.groupby(
                ["age_range","gender","main_isp"]
            ).sum(numeric_only=True).reset_index()

        for _, r in multi.iterrows():
            key = f"{r['age_range']}_{r['gender']}_{r['main_isp']}"

            rows.append({
                "database_id": database_id,
                "adv_id": adv_id,
                "advertiser_name": advertiser_name,
                "id_routers": router,
                "tag_id": tag_id,
                "tag_name": tag_name,
                "dimensions": "age_civilite_isp",
                "valuedimension": key,
                "sends": int(r["sends"]),
                "opens": int(r["opens"]),
                "clicks": int(r["clicks"]),
                "removals": int(r["removals"]),
                "complains": int(r["complains"]),
                "ca": float(ca_value),
                "year": year,
                "month": month,
                "week": int(week),
                "day": day,
                "hour": hour,
                "created_at": self.created_at
            })
        df_final = pd.DataFrame(rows)
                # Supposons que df_final est ton DataFrame final

        # 🔑 Colonnes entières
        int_cols = [
            "database_id", "adv_id", "tag_id",
            "sends", "opens", "clicks", "removals", "complains",
            "year", "month", "week", "day", "hour"
        ]

        for col in int_cols:
            if col in df_final.columns:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype(int)

        float_cols = ["ca"]
        for col in float_cols:
            if col in df_final.columns:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0.0).astype(float)

        str_cols = [
            "advertiser_name", "id_routers", "tag_name",
            "dimensions", "valuedimension"
        ]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].fillna("").astype(str)

        df_final["created_at"] = pd.to_datetime(df_final["created_at"], errors='coerce').fillna(datetime.now())

        for start in range(0, len(df_final), 1000):
                self.clk.insert_df('reporting', df_final.iloc[start:start+1000])
        print('Reporting inseré!!')
   