from sqlalchemy import create_engine
from config.config import Config as config
import pandas as pd
from sqlalchemy import text
class PgConfig:
    def __init__(self):
        self.host=config.FOCUS_CONFIG['HOST']
        self.dbname=config.FOCUS_CONFIG['NAME']
        self.user=config.FOCUS_CONFIG['USER']
        self.password=config.FOCUS_CONFIG['PASSWORD']
        self.port=config.FOCUS_CONFIG['PORT']
        self.connect_timeout=5
    def get_client(self):
        return create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}",
            pool_pre_ping=True
        )
    
