import pandas as pd
from config.ClickHouseConfig import ClickHouseConfig
from utils import parse_mobile
class Events:
    def __init__(self, client=None, table_name="events"):
        """
        Classe pour gérer les contacts dans ClickHouse.
        :param client: clickhouse_driver.Client
        :param table_name: nom de la table ClickHouse
        """
        self.client = client if client else ClickHouseConfig().getClient_prod()
        self.table_name = table_name

    def insert_dataframe(self, df):
        try:
            if df.empty:
                return
            self.client.insert_df(self.table_name, df)
        except Exception as e:
            print("error is ", e)

    def clean_events_df(df: pd.DataFrame):
        string_cols = ["dwh_id", "event_type", "removals_raison", "brand"]
        uint_cols   = ["MessageId", "database_id", "tag", "adv_id"]
        datetime_cols = ["date_event"]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)
        for col in uint_cols:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .fillna(0) 
                    .astype("Int64") 
                )
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    
    def get_adv_ids(self):
        try:
            query=f""" select distinct adv_id from planifik.events WHERE adv_id!=0 AND adv_id NOT IN (SELECT DISTINCT adv_id from planifik.reporting ) ORDER BY adv_id ASC LIMIT 20
                """
            result = self.client.query(query)
            return [row[0] for row in result.result_rows]
        except Exception as e:
            print("erreur",e)



