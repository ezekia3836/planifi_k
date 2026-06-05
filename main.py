from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from redis.asyncio import Redis
from user.router import create_auth_router         
from reporting.router2 import router as router2
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from cron.Cron import Cron
from config import config_gcs
import os
from dotenv import load_dotenv
from config.ClickHouseConfig import ClickHouseConfig as client

load_dotenv("app.env")

@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis(host="localhost", port=6379)
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")
    yield

app = FastAPI(title="PlanifiK", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
expire_minutes = 4*60
app.include_router(router2)
app.include_router(
    create_auth_router(                             
        client=client().getClient_prod(),
        secret_key=os.getenv("SECRET_KEY"),
        expire_minutes=expire_minutes
    )
)
def job_cron():
    start = datetime.now()
    cron = Cron()
    # cron.start_cache_batch()
    #cron.start_advertiser()
    #cron.start_cont()
    #cron.start_act()
    cron.start_tags()
    #cron.start_agence()
    # cron.start_report_final()
    #cron.start_segment()
    print(f"[{datetime.now()}] Exécution du cron  {datetime.now() - start}")
#scheduler = BackgroundScheduler()
#scheduler.add_job(job_cron, 'interval', minutes=2)  # ex: toutes les 2 minutes
#scheduler.start()
job_cron()