from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from redis.asyncio import Redis 
from reporting.router_reporting import router as reporting_router
from reporting.router_global import router as router_global
from reporting.router2 import router as router2
from apscheduler.schedulers.background import BackgroundScheduler
from models.Tags_advertiser import TagsAdvertiser
from datetime import datetime
from cron.Cron import Cron

tgadv = TagsAdvertiser()
origins = [
    "http://localhost",
    "http://localhost:3000",
]
@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis(host="localhost", port=6379)
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")
    yield
app = FastAPI(title="PlanifiK", lifespan=lifespan)

# Middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# routers
# app.include_router(database_router.router, prefix="/database", tags=["Database"])
# app.include_router(reporting_router)
# app.include_router(router_global)
app.include_router(router2)

def job_cron():
    start = datetime.now()
    cron = Cron()
    #cron.start_advertiser()
    #cron.start_cont()
    #cron.start_act()
    #cron.start_tags()
    #cron.start_reporting2()
    print(f"[{datetime.now()}] Exécution du cron  {datetime.now() - start}")


#scheduler = BackgroundScheduler()
#scheduler.add_job(job_cron, 'interval', minutes=2)  # ex: toutes les 2 minutes
#scheduler.start()
job_cron()