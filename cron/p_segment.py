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
      
    def get_segments(self,apiKey, server, idsendout):
        url = f"https://api{server}.esv2.com/v2/Api/Messages/{idsendout}?apiKey={apiKey}"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'xml')
            if response.status_code == 403:
                logging.warning(f"403 Forbidden → idsendout={idsendout}")
                return []
            segments = []
            for seg in soup.find_all('Segment'):
                seg_id = seg.find('Id')
                seg_name = seg.find('Name')
                if seg_id and seg_name:
                    segments.append({
                        "id_segment": int(seg_id.get_text()),
                        "segment_name": seg_name.get_text(),
                        "idsendout": int(idsendout),
                        "expertserver": int(server),
                        "updated_at": datetime.now()
                    })
            return segments
        except Exception as e:
            logging.warning(f"Erreur pour idsendout={idsendout}: {e}")
            return []

    def get_database_id(self,stats_id):
        query = f"SELECT id AS database_id FROM databases WHERE stats_id={stats_id} LIMIT 1"
        res = self.client.query(query)
        if res.result_rows:
            return int(res.result_rows[0][0])
        return None

    def insert_clickhouse(self,segments):
        if not segments:
            return
        df = pd.DataFrame(segments)
        df = df[['id_segment','segment_name','idsendout','expertserver','database_id','updated_at']]
        self.client.insert(self.TABLE_NAME, df.values.tolist(), column_names=list(df.columns))

    def run(self):
        logging.info("LANCEMENT....")
        clean_csv_dir()
        self.log=LogManager()
        output_file=GetExpert().get_expert()
        expert_df = pd.read_csv(output_file, sep=';')
        if expert_df is None or expert_df.empty:
            logging.info("Données vide")
            return
        
        BATCH_SIZE = 100
        PAUSE_API = 0.3
        buffer_segments = []

        for row in expert_df.itertuples(index=False):
            apiKey = str(row.expertapiid).strip()
            server = int(row.expertserver)
            idsendout = int(row.idsendout)
            stats_id = int(row.id)
            segments = self.get_segments(apiKey, server, idsendout)
            if not segments:
                continue

            database_id = self.get_database_id(stats_id)
            if not database_id:
                logging.warning(f"Aucun database_id pour stats_id={stats_id}")
                database_id=0

            for s in segments:
                s['database_id'] = database_id

            grouped = {}
            for s in segments:
                grouped.setdefault(s['database_id'], []).append({
                    'id_segment': s['id_segment'],
                    'segment_name': s['segment_name'],
                    'idsendout': s['idsendout'],
                    'expertserver': s['expertserver'],
                    'database_id': s['database_id'],
                    'updated_at': s['updated_at']
                })

            for db_id, rows in grouped.items():
                db_csv_file = os.path.join(CSV_DIR, f"segments_base_{db_id}.csv")
                df_tmp = pd.DataFrame(rows)[
                    ['id_segment', 'segment_name', 'idsendout', 'expertserver', 'database_id', 'updated_at']
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