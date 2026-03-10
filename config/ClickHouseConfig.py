from clickhouse_connect import get_client  
import pandas as pd
from config.config import Config
class ClickHouseConfig:
    def __init__(self):
        self.host_loc = Config.CLICKHOUSE_LOCAL['host']
        self.port_loc =  Config.CLICKHOUSE_LOCAL['port']
        self.username_loc =  Config.CLICKHOUSE_LOCAL['username']
        self.password_loc =  Config.CLICKHOUSE_LOCAL['password']
        self.database_loc =  Config.CLICKHOUSE_LOCAL['database']
        #self.host = '34.159.21.189'
        #self.port = 8123
        #self.username = 'admin'
        #self.password = 'Suxohh2op!!'
        #self.database = 'planifik'
        self.host = Config.CLICKHOUSE_PROD['host']
        self.port = Config.CLICKHOUSE_PROD['port']
        self.username = Config.CLICKHOUSE_PROD['username']
        self.password = Config.CLICKHOUSE_PROD['password']
        self.database = Config.CLICKHOUSE_PROD['database']
        self.secure = True

    def getClient_prod(self):
        return get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            settings={
                "async_insert": 1,
                "wait_for_async_insert": 0,
                "insert_quorum": 1,
                #"max_partitions_per_insert_block": 1000
            }
        )
    def getClient_loc(self):
        return get_client(
            host=self.host_loc,
            port=self.port_loc,
            username=self.username_loc,
            password=self.password_loc,
            database=self.database_loc,
            settings={
                "async_insert": 1,
                "wait_for_async_insert": 0,
                "insert_quorum": 1,
                #"max_partitions_per_insert_block": 1000
            }
        )
"""client = ClickHouseConfig().getClient()

df = pd.read_excel('databases.xlsx')

cols = ["id","stats_id","ktk_id","dwh_id","country","segment_id_all","es_id","isActive"]
for col in cols:
    df[col]=df[col].astype(int)
string_cols = ["acronyms", "api_url", "api_key", "service", "es_url", "owner"]
for col in string_cols:
    df[col] = df[col].astype(str)
df=df.fillna(0)
from datetime import datetime
df['create_at'] =datetime.now()  
 


client.insert_df('planifik.databases', df)"""