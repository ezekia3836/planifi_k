import psycopg, json

class Focus:

    def __init__(self, config):
        self.config = config

    def connect(self):
        try:
            conn = psycopg.connect(
                host=self.config['HOST'],
                port=self.config['PORT'],
                dbname=self.config['NAME'],
                user=self.config['USER'],
                password=self.config['PASSWORD']
            )
            return conn
        except Exception as e:
            print(f"Erreur de connexion : {e}")
            return None
        
    def extract_data(self, start_date, end_date, database):
        connexion = self.connect()
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
        if connexion is None:
            print("Aucune connexion à la base de données.")
            return None
        query = f""" 
        SELECT 
                vd.id as id_focus,
                vd.date_shedule, 
                vd.idsendout as id_router,
                vd.base as base_id,
                vd.tag,
                vd.advertiser as adv_id,
                vd.creaname as brand,
                subq.type, 
                subq.parend_id
            FROM visu.v2_data vd
            JOIN (
                SELECT DISTINCT ids.id_p, 
                    CASE 
                        WHEN ids.id_p = sub.child_id THEN 'parent'
                        WHEN ids.id_p = sub.parent_id THEN 'child'
                    END AS type, 
                    sub.child_id as parend_id
                FROM (
                    SELECT 
                        vd.id AS child_id,
                        vr_link.id_reuse AS parent_id
                    FROM visu.v2_data vd
                    LEFT JOIN visu.v2_data_reuse vr_link ON vd.id = vr_link.id_v2
                    LEFT JOIN visu.v2_data_reuse vr_reuse ON vd.id = vr_reuse.id_reuse
                    WHERE vd.date_shedule BETWEEN DATE '{start_date}' AND DATE '{end_date}'  
                    AND vd.status = 5
                    AND vd.sent > 10
                    AND vd.base IN ({database['stats_id']})
                    AND (
                        (vr_link.id_reuse IS NULL AND vr_reuse.id_v2 IS NULL)
                        OR
                        (vr_link.id_reuse IS NOT NULL AND vr_reuse.id_v2 IS NULL)
                    )
                ) AS sub
                JOIN LATERAL (
                    VALUES 
                    (sub.child_id),
                    (sub.parent_id)
                ) AS ids(id_p) ON true
                WHERE ids.id_p IS NOT NULL
            ) AS subq ON vd.id = subq.id_p
            WHERE vd.date_shedule BETWEEN DATE '{start_date}' AND DATE '{end_date}'  
            ORDER BY vd.id;
        """
        try:
            with connexion.cursor() as cursor:
                cursor.execute(query)  
                columns = [desc[0] for desc in cursor.description] 
                rows = cursor.fetchall()
                data = [dict(zip(columns, row)) for row in rows]
                return json.dumps(data, default=str)
        except Exception as e:
            print(f"Erreur lors de l'extraction des données : {e}")
            return None