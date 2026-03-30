from genericpath import isfile

import numpy as np
import pandas as pd
from datetime import datetime
import time
import logging
from config.PgConfig import PgConfig
import os
from sqlalchemy import text
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
SEGMENT_DIR="SEGMENTS"
if not os.path.exists(SEGMENT_DIR):
    os.makedirs(SEGMENT_DIR)
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

    def get_expert(self, output_file=f"{SEGMENT_DIR}/resultats.csv", chunk_size=1000):
        if os.path.isfile(output_file):
            os.remove(output_file)
        logging.info("Recupère (data focus)")
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
                chunk_df = chunk_df.replace([np.inf,-np.inf],pd.NA)
                chunk_df = chunk_df.dropna(subset=["expertapiid","expertserver"])
                chunk_df["expertserver"] = pd.to_numeric(chunk_df["expertserver"],errors='coerce').astype(int)
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
                time.sleep(0.1) 
            logging.info(f" Terminé : {total_rows} lignes exportées")
            return output_file
        except Exception as e:
            logging.error(f" Erreur globale : {e}")