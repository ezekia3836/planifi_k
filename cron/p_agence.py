from config.config import Config as config
from config.ClickHouseConfig import ClickHouseConfig
from config.PgConfig import PgConfig
from sqlalchemy import text
import pandas as pd

class p_agence():

    def __init__(self):
        self.clk = ClickHouseConfig().getClient_prod()
        self.pg = PgConfig().get_client()
        self.table="agences"
    def get_agences(self):
        query = "SELECT id, name FROM visu.client"
        with self.pg.connect() as conn:
            result = conn.execute(text(query))
            return result.fetchall()
    def insert_agences(self):
        rows = self.get_agences()

        if not rows:
            return

        try:
            self.clk.query(f"TRUNCATE TABLE {self.table}")
            print("Table agences vidée")
        except Exception as e:
            print(f"Erreur TRUNCATE: {e}")
            return
        df = pd.DataFrame(rows, columns=["id", "name"])
        try:
            self.clk.insert_df(self.table, df)
            print(f"{len(df)} agences insérées")
        except Exception as e:
            print(f"Erreur insertion ClickHouse: {e}")
    def run(self):
        self.insert_agences()
        