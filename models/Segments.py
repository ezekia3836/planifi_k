from genericpath import isfile

import numpy as np
import pandas as pd
from datetime import datetime
import time
import logging
from config.ClickHouseConfig import ClickHouseConfig
import os
from sqlalchemy import text
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
SEGMENT_DIR="SEGMENTS"
if not os.path.exists(SEGMENT_DIR):
    os.makedirs(SEGMENT_DIR)
class GetExpert:
    def __init__(self):
        self.client = ClickHouseConfig().getClient_prod()
    def chunk_dataframe(self, df, chunk_size=500):
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i:i + chunk_size]
    def _execute_query(self, query,params=None):
        result = self.client.query(query,parameters=params or {})
        return [dict(zip(result.column_names, r)) for r in result.result_rows]
    def fetch_all(self, query, retries=3):
        for attempt in range(retries):
            try:
                df = pd.read_sql(query, self.engine)
                return df
            except Exception as e:
                logging.warning(f" Retry {attempt+1} - erreur : {e}")
                time.sleep(1)
        raise Exception(" Échec après plusieurs tentatives")
    def get_database(self, output_file=f"{SEGMENT_DIR}/resultats.csv"):
        if os.path.isfile(output_file):
            os.remove(output_file)

        logging.info("Récupère (data databases)")

        query = """
            SELECT api_url, api_key, service,id 
            FROM databases WHERE api_url IS NOT NULL
        """
        rows = self._execute_query(query)
        if not rows:
            logging.warning("Aucune donnée récupérée")
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=rows[0].keys())
        df.to_csv(output_file, sep=';', index=False, encoding="utf-8")
        return df
        
