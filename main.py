from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache
from routers import database_router
from apscheduler.schedulers.background import BackgroundScheduler
from models.Tags_advertiser import TagsAdvertiser
from models.query import Query
from datetime import datetime
from cron.Cron import Cron
import json
import time
tgadv = TagsAdvertiser()
query = Query()
# Configuration CORS
origins = [
    "http://localhost",
    "http://localhost:3000",  
]

@asynccontextmanager
async def lifespan(app: FastAPI):
    FastAPICache.init(InMemoryBackend())
    yield
app = FastAPI(title="PlanifiK FastAPI",lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(database_router.router, prefix="/database", tags=["Database"])

@app.get("/")
def read_root():
    return {"message": "Bienvenue sur FastAPI !"}


@app.get("/tags")
@cache(expire=60)
async def get_tags():
    print("DB")
    return tgadv.read_tags()

@app.get("/tags/{id}")
async def get_tags_byId(id:int):
    return tgadv.get_tags_byId(id)

@app.get("/advertiser")
@cache(expire=60)
async def get_advertiser():
    print("db")
    return tgadv.read_advertiser()

@app.get("/advertiser/{id}")
async def get_advertiser_byid(id:int):
    return tgadv.get_advertiser_byid(id)

@app.get("/search_advertiser/{keywords}")
async def search_advertiser(keywords:str):
    return tgadv.search_advertiser(keywords)

@app.get("/reporting/router/{router}")
@cache(expire=60)
async def get_report(router:int ,adv:int):
    return tgadv.reporting(router,adv)

@app.get("/reporting/adv/{adv}")
@cache(expire=60)
async def get_report_advertiser(adv):
    data = query.global_advertiser(adv)
    return data

@app.get("/reporting/db/{db_id}")
@cache(expire=60)
async def get_report_db(db_id:int):
    return query.global_base(db_id)

@app.get('/reporting/calendrier/{adv}')
@cache(expire=60)
async def calendrier(adv):
    return query.calendrier(adv_id=adv)

@app.get('/reporting/prog/{adv}')
@cache(expire=60)
async def propgramme(adv:int):
    return query.programmes(adv)

@app.get('/reporting/list_advertiser/')
@cache(expire=60)
async def list_advertiser():
    return query.list_advertiser()
@app.get('/reporting/list_tags/')
@cache(expire=60)
async def list_tags():
    return query.list_tags()
@app.get('/reporting/top/')
@cache(expire=60)
async def top_10_object():
    return query.top_10_objet()
@app.get('/reporting/compte/{adv}')
@cache(expire=60)
async def comptage(adv:int):
    return query.advertiser_counts(adv)
def job_cron():
    start = datetime.now()
    cron = Cron()
    #cron.start_advertiser()
    #cron.start_cont()
    #cron.start_act()
    #cron.start_tags()
    cron.start_reporting()
    print(f"[{datetime.now()}] Exécution du cron  {datetime.now() - start}")
    # Ici tu peux mettre ton code, par ex. appeler la Database class

# # Scheduler
#cheduler = BackgroundScheduler()
#cheduler.add_job(job_cron,'interval',minutes=2)
# # scheduler.add_job(job_cron, 'cron', hour=21, minute=0)
# scheduler.add_job(job_cron, 'interval', seconds=5)
#cheduler.start()
job_cron()
