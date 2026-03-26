import pandas as pd
import time
import logging
from config.PgConfig import PgConfig
from sqlalchemy import text
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class GetExpert:
    def __init__(self):
        self.engine = PgConfig().get_client()

    def chunk_dataframe(self, df, chunk_size=500):
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i + chunk_size]

    def fetch_all(self, query, retries=3):
        for attempt in range(retries):
            try:
                df = pd.read_sql(query, self.engine)
                return df
            except Exception as e:
                logging.warning(f" Retry {attempt+1} - erreur : {e}")
                time.sleep(1)
        raise Exception(" Échec après plusieurs tentatives")

    def get_expert(self, output_file="resultats.csv", chunk_size=1000):
        logging.info("")

        query = text("""
            SELECT d.expertapiid, d.expertserver, d.id, da.idsendout
            FROM visu.v2_database d
            JOIN visu.v2_data da ON d.id = da.base
        """)

        first = True
        total_rows = 0

        try:
            for chunk_df in pd.read_sql(query, self.engine, chunksize=chunk_size):

                if chunk_df.empty:
                    continue
                chunk_df = chunk_df.drop_duplicates(
                    subset=['expertapiid', 'expertserver', 'id', 'idsendout']
                )
                chunk_df["expertserver"] = chunk_df["expertserver"].astype(int)
                chunk_df.to_csv(
                    output_file,
                    mode='w' if first else 'a',
                    sep=';',
                    header=first,
                    index=False,
                    encoding='utf-8'
                )

                total_rows += len(chunk_df)
                first = False

                logging.info(f" Chunk traité : {len(chunk_df)} lignes")

                time.sleep(0.1) 

            logging.info(f" Terminé : {total_rows} lignes exportées")
        except Exception as e:
            logging.error(f" Erreur globale : {e}")