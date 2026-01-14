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
        self.client = client if client else ClickHouseConfig().getClient()
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
        """df.to_csv(
            "events.csv",
            sep="|",
            index=False,
            encoding="utf-8"
        )"""

        return df


