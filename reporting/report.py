from config.PgConfig import PgConfig
from config.ClickHouseConfig import ClickHouseConfig
import numpy as np
import pandas as pd
from datetime import datetime,timedelta,date
from sqlalchemy import text
from reporting.analyze import analyse
class reporting:
    def __init__(self):
        self.clk= ClickHouseConfig().getClient()
        self.pst = PgConfig().get_client()
        self.date_end = datetime.today().date()
        self.analyze=analyse()
        self.advertiser=13105
        self.db_id=1
        self.bd_name="comptoirdesreducs.com"
        self.table = "reporting"
        self.date_start = self.date_end-timedelta(days=90)
        self.created_at =datetime.now()
    def recupere_pst(self):
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
                a.id                         AS advertiser_id,
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
                AND vd.advertiser = {self.advertiser}
                AND vd.base={self.db_id}
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
    def recupere_clk(self):
        try:
            query=f"""SELECT
                e.database_id AS database_id,
                e.MessageId AS id_routers,
                e.adv_id AS advertiser_id,
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
                WHERE e.adv_id={self.advertiser} 
                AND e.database_id={self.db_id}
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
    def safe_utf8(self,text):
         if isinstance(text, str):
            return text.encode('utf-8', errors='replace').decode('utf-8')
         return str(text)
    def make_data(self,df):
            data={}
            sends = int(df['Sends'].sum())
            opens = int(df['Opens'].sum())
            clicks = int(df['Clicks'].sum())
            unsubs = int(df['Removals'].sum())

            opens = min(opens, sends)
            clicks = min(clicks, sends)

            if sends > 0:
                taux_openers = round((opens / sends) * 100, 2) if sends else 0.0
                taux_clickers = round((clicks / sends) * 100, 2) if sends else 0.0
                taux_unsubs = round((unsubs / sends) * 100, 2) if sends else 0.0
            else:
                taux_openers = taux_clickers = taux_unsubs = 0.0

            if opens > 0:
                cto_rate = round((clicks / opens) * 100, 2)
            else:
                cto_rate = 0.0

            data['sends']=sends
            data['taux_clickers']=taux_clickers
            data['taux_desabo']=taux_unsubs
            data['taux_cto']=cto_rate
            
            analyse={
                "taux_click":self.analyze.analyze_click_rate(taux_clickers),
                "taux_cto":self.analyze.analyze_cto_rate(cto_rate,opens),
                "taux_desabo":self.analyze.analyze_unsub_rate(taux_unsubs)
            }
            data['analyse']=analyse
            return data
    def safe_str(self, value):
        return str(value).encode("utf-8", errors="replace").decode("utf-8")
    def build_dim(self,group, dim, make_data, safe_key, max_items=None):
        result = {}
        for k, sub in group.groupby(dim):
            data = make_data(sub)
            if not data or data.get('sends', 0) <= 0:
                continue
            result[safe_key(k)] = data
            if max_items and len(result) >= max_items:
                break
        return result
    def build_comb(self,group, make_data, safe_key):
        result = {}
        for (a, g, i), sub in group.groupby(['age_range', 'gender', 'main_isp']):
            data = make_data(sub)
            if not data or data.get('sends', 0) <= 0:
                continue
            key = f"{safe_key(a)}_{safe_key(g)}_{safe_key(i)}"
            result[key] = data
        return result
    def build_global_block(self, group):
        data = {}

        for dim, name in [
            ('gender', 'civilite'),
            ('age_range', 'age'),
            ('main_isp', 'isp')
        ]:
            dim_data = self.build_dim(
                group,
                dim=dim,
                make_data=self.make_data,
                safe_key=self.safe_str
            )
            if dim_data:
                data[name] = dim_data

        comb = self.build_comb(
            group,
            make_data=self.make_data,
            safe_key=self.safe_str
        )
        if comb:
            data['age_civilite_isp'] = comb

        total_sent = float(group['Sends'].sum())
        total_clicks = float(group['Clicks'].sum())
        total_opens = float(group['Opens'].sum())
        total_unsubs = float(group['Removals'].sum())
        total_ca = float(group['ca'].sum())

        clk = round(total_clicks / total_sent * 100, 2) if total_sent else 0.0
        taux_opens = round(total_opens / total_sent * 100, 2) if total_sent else 0.0
        taux_desabo = round(total_unsubs / total_sent * 100, 2) if total_sent else 0.0
        cto_rate = round(total_clicks / total_opens * 100, 2) if total_opens else 0.0
        ecpm = round((total_ca / total_sent) * 1000, 2) if total_sent else 0.0

        data['data_global'] = {
            "volume": total_sent,
            "ecpm": ecpm,
            "taux_clicks": clk,
            "taux_opens": taux_opens,
            "taux_cto": cto_rate,
            "taux_desabo": taux_desabo,
            "total_ca": total_ca,
            "analyse": {
                "taux_clicks": self.analyze.analyze_click_rate(clk),
                "taux_desabo": self.analyze.analyze_unsub_rate(taux_desabo),
                "taux_cto": self.analyze.analyze_cto_rate(cto_rate, total_opens)
            }
        }
        return data

    def report(self):
        rows = []
        df_clk = self.recupere_clk()
        df_pg = self.recupere_pst()
        df_pg_expanded = df_pg.explode('id_routers')
        df_clk['id_routers'] = df_clk['id_routers'].astype(str)
        df_pg_expanded['id_routers'] = df_pg_expanded['id_routers'].astype(str)
        df_pg_expanded = df_pg_expanded.rename(columns={
            'advertiser': 'adv_id',
            'tag': 'tag_id'
        })
        df = df_clk.merge(df_pg_expanded, on='id_routers', how='inner')
        bins = [0, 18, 24, 34, 44, 54, 64, 74, 200]
        labels = ['0-18', '18-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75+']
        df['age_range'] = pd.cut(df['age'], bins=bins, labels=labels)
        df['age_range'] = df['age_range'].cat.add_categories('O_age').fillna('O_age')
        df['age_range'] = df['age_range'].astype(str)

        df['gender'] = df['gender'].fillna('O_gender')
        df['gender'] = df['gender'].replace({'O':'O_gender'})
        df['main_isp'] = df['main_isp'].fillna('O_isp')
        df['main_isp'] = df['main_isp'].replace({'Other':'O_isp'})

        event_types = ['Sends', 'Opens', 'Clicks', 'Removals']
        for ev in event_types:
            df[ev] = (df['event_type'] == ev).astype(int)

        router_global_df = (df.groupby(['database_id', 'id_routers', 'adv_id', 'advertiser_name'], as_index=False).agg({'Sends': 'sum', 'Opens': 'sum', 'Clicks': 'sum', 'Removals': 'sum'}))

        router_global_df['taux_clicks'] = router_global_df['Clicks'].div(router_global_df['Sends'])

        router_global_df['taux_opens'] = router_global_df['Opens'].div(router_global_df['Sends'])

        router_global_df['taux_desabo'] = router_global_df['Removals'].div(router_global_df['Sends'])
        cols=['taux_clicks','taux_opens','taux_desabo']
        for col in cols:
            router_global_df[col]=router_global_df[col].replace([np.inf,-np.inf],0).fillna(0).mul(100).round(2)
        router_globals = {
            r.id_routers: {
                'total_sent': float(r.Sends),
                'taux_clicks': round(float(r.taux_clicks),2),
                'taux_opens': round(float(r.taux_opens),2),
                'taux_desabo': round(float(r.taux_desabo),2)
            }
            for r in router_global_df.itertuples()
        }
        for keys, group in df.groupby(['database_id','id_routers','tag_id','tag_name','adv_id','advertiser_name']):
            db_id, router, tag_id, tag_name, adv_id, advertiser_name = keys
            final_data = {}

            def build_dim(dim):
                out = {}
                for k, sub in group.groupby(dim):
                    d = self.make_data(sub)
                    if d:
                        out[self.safe_str(k)] = d
                return out

            for dim, name in [('gender','civilite'),('age_range','age'),('main_isp','isp')]:
                dim_data = build_dim(dim)
                if dim_data:
                    final_data[name] = dim_data
            comb = {}
            for (a,g,i), sub in group.groupby(['age_range','gender','main_isp']):
                d = self.make_data(sub)
                if d:
                    key = f"{self.safe_str(a)}_{self.safe_str(g)}_{self.safe_str(i)}"
                    comb[key] = d
                if len(comb) >= 50:
                    break
            if comb:
                final_data['age_civilite_isp'] = comb
            if router in router_globals:
                final_data['global_router'] = router_globals[router]
            if final_data:
                rows.append({
                    'database_id': db_id,
                    'id_routers': router,
                    'tag_id': tag_id,
                    'tag_name': tag_name,
                    'adv_id': adv_id,
                    'advertiser_name': advertiser_name,
                    'data': final_data,
                    'create_at': self.created_at
                })
        #global_adveriser
        for (adv_id, advertiser_name), group in df.groupby(['adv_id', 'advertiser_name']):
            rows.append({
                'database_id': 0,
                'id_routers': 0,
                'tag_id': 0,
                'tag_name': None,
                'adv_id': adv_id,
                'advertiser_name': advertiser_name,
                'data': self.build_global_block(group),
                'create_at': self.created_at
            })
        #global_db
        for (db_id, adv_id, advertiser_name), group in df.groupby(['database_id', 'adv_id', 'advertiser_name']):
            rows.append({
                'database_id': db_id,
                'id_routers': 0,
                'tag_id': 0,
                'tag_name': None,
                'adv_id': adv_id,
                'advertiser_name': advertiser_name,
                'data': self.build_global_block(group),
                'create_at': self.created_at
            })

        df_final = pd.DataFrame(rows)
        #df_final.to_csv('alls.csv',index=False,sep=';')
        for col in ['id_routers','database_id','tag_id','adv_id']:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype('Int64')
        for start in range(0, len(df_final), 1000):
            self.clk.insert_df('reporting', df_final.iloc[start:start+1000])
        print('Reporting inseré!!')

    def get_test(self):
            engine = self.pst.connect()
            query = text(f"""SELECT advertiser,idsendout FROM visu.v2_data WHERE base='{self.db_id}' AND advertiser='{self.advertiser}' AND  date_shedule BETWEEN DATE '{self.date_start}' AND DATE '{self.date_end}' """)
            with engine as conn:
                df = pd.read_sql(query,conn)
                print(df)
                df.to_csv('focus.csv',index=False,sep=';')

    def show_tables(self):
       conn = self.pst.connect()

       df_tables = pd.read_sql("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'visu'
            ORDER BY table_name
        """, conn)
       print(df_tables)
                                
    


    