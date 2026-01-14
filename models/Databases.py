# app/database.py
from config.ClickHouseConfig import ClickHouseConfig
from datetime import datetime
import pandas as pd
class Database:
    def __init__(self, client=None):
        self.client = client if client else ClickHouseConfig().getClient()
        self.table_name = "databases"

    # Créer un enregistrement
    def create(self, id: int, name: str, owner: str, is_active: int = 1):
        query = f"""
        INSERT INTO {self.table_name} (id, name, owner, is_active, created_at)
        VALUES ({id}, '{name}', '{owner}', {is_active}, '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}')
        """
        self.client.command(query)
        return {"status": "success", "id": id}

    # Lire un enregistrement par ID
    def read(self, id: int):
        query = f"SELECT * FROM {self.table_name} WHERE id = {id} LIMIT 1"
        result = self.client.query(query).result_rows
        if result:
            return dict(zip([col[0] for col in self.client.query(f"DESCRIBE TABLE {self.table_name}").result_rows], result[0]))
        return None

    # Lire tous les enregistrements
    def read_all(self):
        query = f"SELECT * FROM {self.table_name} where isActive=1"
        result = self.client.query(query).result_rows
        columns = [col[0] for col in self.client.query(f"DESCRIBE TABLE {self.table_name}").result_rows]
        return [dict(zip(columns, row)) for row in result]

    # Mettre à jour un enregistrement
    def update(self, id: int, **kwargs):
        set_clause = ", ".join([f"{key} = '{value}'" for key, value in kwargs.items()])
        query = f"ALTER TABLE {self.table_name} UPDATE {set_clause} WHERE id = {id}"
        self.client.command(query)
        return {"status": "updated", "id": id}

    # Supprimer un enregistrement
    def delete(self, id: int):
        query = f"ALTER TABLE {self.table_name} DELETE WHERE id = {id}"
        self.client.command(query)
        return {"status": "deleted", "id": id}
    def add_databases(self):
        try:
            data = pd.read_excel('databases.xlsx')
            df =pd.DataFrame(data)
            df['create_at'] = datetime.now()
            int_cols = ['id','dwh_id','stats_id','ktk_id','es_id','isActive','segment_id_all']
            str_cols = ['acronyms','country','basename','api_url','api_key','service','es_url','owner']

            for col in int_cols:
                df[col]=pd.to_numeric(df[col],errors='coerce').fillna(0).astype('Int64')
            for col in str_cols:
                df[col]=df[col].astype(str).fillna('')
            res=self.clk.insert_df('databases',df)
            if res:
                print('ok')
            else:
                print('errot')
        except Exception as e:
            print('exception',e)
