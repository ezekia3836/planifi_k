import os
import time
import logging
import pandas as pd
import requests
from config.ClickHouseConfig import ClickHouseConfig
from logs.SegmentLogs import LogManager,clean_csv_dir
from bs4 import BeautifulSoup
from models.Segments import GetExpert
from datetime import datetime
CSV_DIR = "csv_segments"
os.makedirs(CSV_DIR, exist_ok=True)
class p_segment:
    def __init__(self):
        self.TABLE_NAME="list_prod_segment"
        self.client = ClickHouseConfig().getClient_prod()
      
    def get_segments(self, api_url, api_key, database_id):
        url = f"{api_url}Api/Segments"
        params = {"apiKey": api_key}

        try:
            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 403:
                logging.warning("403 Forbidden")
                return []
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "xml")
            segments = []
            for seg in soup.find_all("Segment"):
                seg_id = seg.find("Id")
                seg_name = seg.find("Name")
                if not seg_id or not seg_name:
                    continue
                segments.append({
                    "id_segment": int(seg_id.text),
                    "segment_name": seg_name.text,
                    "database_id": database_id,
                    "updated_at": datetime.now()
                })
            return segments
        except Exception as e:
            logging.warning(f"Erreur get_segments (database_id={database_id}): {e}")
            return []


    def insert_clickhouse(self,segments):
        if not segments:
            return
        df = pd.DataFrame(segments)
        df = df[['id_segment','segment_name','database_id','updated_at']]
        self.client.insert(self.TABLE_NAME, df.values.tolist(), column_names=list(df.columns))

    def run(self):
        logging.info("LANCEMENT....")
        clean_csv_dir()
        self.log=LogManager()
        expert_df=GetExpert().get_database()
        if expert_df is None or expert_df.empty:
            logging.info("Données vide")
            return
        
        BATCH_SIZE = 100
        PAUSE_API = 0.3
        buffer_segments = []

        for row in expert_df.itertuples(index=False):
            apiKey = str(row.api_key).strip()
            api_url = str(row.api_url).strip()
            database_id = int(row.id)
            segments = self.get_segments(api_url,apiKey, database_id)
            if not segments:
                continue

            for s in segments:
                s['database_id'] = database_id

            grouped = {}
            for s in segments:
                grouped.setdefault(s['database_id'], []).append({
                    'id_segment': s['id_segment'],
                    'segment_name': s['segment_name'],
                    'database_id': s['database_id'],
                    'updated_at': s['updated_at']
                })

            for db_id, rows in grouped.items():
                db_csv_file = os.path.join(CSV_DIR, f"segments_base_{db_id}.csv")
                df_tmp = pd.DataFrame(rows)[
                    ['id_segment', 'segment_name', 'database_id', 'updated_at']
                ]

                df_tmp.to_csv(
                    db_csv_file,
                    sep=';',
                    index=False,
                    header=not os.path.exists(db_csv_file),
                    mode='a',
                    encoding='utf-8'
                )

            buffer_segments.extend(segments)

            if len(buffer_segments) >= BATCH_SIZE:
                self.insert_clickhouse(buffer_segments)
                self.log.write(buffer_segments)
                buffer_segments = []

            time.sleep(PAUSE_API)

        if buffer_segments:
            self.insert_clickhouse(buffer_segments)
            self.log.write(buffer_segments)

        self.log.close()
        logging.info("Traitement terminé")