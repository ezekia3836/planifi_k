from google.cloud import storage
from datetime import timedelta
from config.ClickHouseConfig import ClickHouseConfig
import logging

logger = logging.getLogger("gcs_loader")

class GCSClickHouseLoader:

    def __init__(self, clickhouse_client, bucket_name, sa_path):

        self.ch = clickhouse_client

        self.client = storage.Client.from_service_account_json(sa_path)
        self.bucket = self.client.bucket(bucket_name)

    def get_ch_client(self):
        return ClickHouseConfig().getClient_prod()

    def generate_signed_url(self, remote_file, expiration_hours=2):

        blob = self.bucket.blob(remote_file)

        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(hours=expiration_hours),
            method="GET"
        )

    def load_and_cleanup(self, remote_file, table):

        blob = self.bucket.blob(remote_file)
        ch = self.get_ch_client()
        try:
            url = self.generate_signed_url(remote_file)

            query = f"""
                INSERT INTO {table}
                SELECT *
                FROM url('{url}', 'Parquet')
            """

            ch.command(query)

            logger.info(f"[CH INSERT OK] {remote_file}")

        except Exception as e:
            logger.error(f"[LOAD ERROR - NO DELETE] {remote_file} : {e}", exc_info=True)
            return False
        try:
            blob.delete()
            logger.info(f"[GCS DELETE OK] {remote_file}")
        except Exception as e:
            logger.error(f"[GCS DELETE ERROR] {remote_file} : {e}")

        return True