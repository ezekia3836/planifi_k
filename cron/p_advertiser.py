import pandas as pd
from config.ClickHouseConfig import ClickHouseConfig
from models.Databases import Database
from config.PgConfig import PgConfig
from models.Tags_advertiser import TagsAdvertiser
advertiser = TagsAdvertiser()
class p_advertiser:
    BATCH_SIZE = 10_000

    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.db_model = TagsAdvertiser(client=self.clk)
        self.pg = PgConfig().get_client()
        self.table_name='advertiser'

    def fetch_advertiser(self):
        query = """
            SELECT
                id,
                name,
                desabled,
                created_at,
                update_at,
                user_create,
                user_update
            FROM visu.advertiser
        """
        return pd.read_sql_query(
            query,
            self.pg,
            chunksize=self.BATCH_SIZE
        )
    
    def start_advertiser(self):
        total=0
        nombre=self.db_model.verifier_table(self.table_name)
        if nombre>0:
            self.db_model.vider_table(self.table_name)
        for df in self.fetch_advertiser():
            if df.empty:
                continue
            df = df.where(pd.notnull(df), None)
            total+=len(df)
            self.db_model.insert_dataframe(self.table_name,df)
        print(total)
        print(" Insertion advertiser terminée!!!")

            
