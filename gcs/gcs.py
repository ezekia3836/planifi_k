from config.config_gcs import gcs_client,bucket
from datetime import datetime
from config.ClickHouseConfig import ClickHouseConfig
from config.config import Config as config
import pandas as pd
import io
import json
from models.Events import Events  as events
from models.Contacts import Contacts as contacts
class gcs:
    def __init__(self,bucket: str):
        self.client = gcs_client
        self.bucket = gcs_client.bucket(bucket)
        self.clk = ClickHouseConfig().getClient_loc()


    def upload_to_gcs(self,chunk_size,prefix,df,path_gcs):
        index=0
        time = datetime.now().strftime("%Y%m%d_%H%M%S")
        total = len(df)
        csv_buffer = io.StringIO()
        print("upload en cours.....")
        for i in range(0,total,chunk_size):
            end = min(i + chunk_size, total)
            chunk = df.iloc[i:end]
            csv_buffer.seek(0)
            csv_buffer.truncate(0)
            chunk.to_csv(csv_buffer,index=False, sep="|")
            gcs_path = f"{path_gcs}/{prefix}_{time}_{index}.csv"
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
            index+=1
        print("upload terminé!!!!")
        
    def insert_into_clickhouse(self,prefix:str,bucket_name,table):
         blobs = bucket.list_blobs(prefix=prefix)
         csv_files = [b.name for b in blobs if b.name.endswith('.csv')]
         print("Insert en cours.......")
         for file_name in csv_files:
            gcs_url = f"https://storage.googleapis.com/{bucket_name}/{file_name}"
            query = f"""
            INSERT INTO {table}
            SELECT * FROM s3(
                '{gcs_url}',
                '{config.GCS_CONFIG['acces_key']}',
                '{config.GCS_CONFIG['secret_key']}',
                'CSVWithNames'
            ) SETTINGS
        format_csv_delimiter = '|'
            """
            self.clk.command(query)
         print("Insert terminée!!!")
         
    def delete_data_bucket(self,prefix):
        blobs = list(bucket.list_blobs(prefix=prefix))
        if not prefix:
            raise ValueError("Interdiction de supprimer")
        if not blobs:
            print("Aucun fichier à supprimer")
            return 0
        print("Suppr en cours.........")
        bucket.delete_blobs(blobs)
        print("Suppr términée!!!")