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
from datetime import datetime
from cron.Cron import Cron
from reporting.router import router as reporting_router
tgadv = TagsAdvertiser()

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
app.include_router(reporting_router)

@app.get("/")
def read_root():
    return {"message": "Bienvenue sur Planifik"}

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
