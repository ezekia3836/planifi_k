from models.query2 import Query2
from fastapi import APIRouter,Depends, Query
from typing import Optional
from fastapi import FastAPI, HTTPException
import pandas as pd
from fastapi_cache.decorator import cache 
from reporting.schema2 import (
    GlobalAdvertiserResponse,
    GlobalBaseResponse,
    AdvertisersResponse,
    BasesResponse
)
segment_index = Query2().build_segment_index()
query = Query2()
router = APIRouter(prefix="/reporting", 
    tags=["Reporting"])
@router.get("/advertiser/{adv}", summary="Rapport global d'un advertiser",response_model=GlobalAdvertiserResponse)
@cache(expire=60)
async def get_report_advertiser(adv: int):
    return query.global_advertiser(adv)
@router.get("/database/{base_id}", summary="Rapport global d'une base",response_model=GlobalBaseResponse)
@cache(expire=60)
async def get_report_base(base_id:int):
    return query.global_base(base_id)
@router.get("/all_advertisers/",summary="tout les advertisers dans reporting")
@cache(expire=60)
async def all_advertisers(
    date_schedule:Optional[str]=None,
    date_start:Optional[str]=None,
    date_end:Optional[str]=None
):
    return query.all_advertisers(date_schedule=date_schedule,date_start=date_start,date_end=date_end)

@router.get("/all_bases/",summary="Liste toutes bases dans reporting")
@cache(expire=60)
async def all_bases(
    date_schedule:Optional[str]=None,
    date_start:Optional[str]=None,
    date_end:Optional[str]=None,
    country: list[str] | None = Query(None),
    tags: list[str] | None = Query(None)
):
    return query.all_bases(country=country,tags=tags,date_schedule=date_schedule,date_start=date_start,date_end=date_end)


@router.get("/segment",summary="Liste segments")
def get_segment(id_segment: Optional[int]=None):
    csv_file = segment_index.get(id_segment)
    if id_segment is not None:
        if not csv_file:
            raise HTTPException(status_code=404, detail=f"id_segment {id_segment} non trouvé")
        try:
            df = pd.read_csv(csv_file, sep=';')
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erreur lecture CSV {csv_file}: {e}")
        row = df[df['id_segment'] == id_segment]
        if row.empty:
            raise HTTPException(status_code=404, detail=f"id_segment {id_segment} non trouvé dans {csv_file}")
        return row[['id_segment','segment_name','expertserver','idsendout','database_id']].to_dict(orient='records')[0]
    all_rows=[]
    for csv_file in set(segment_index.values()):
        try:
            df = pd.read_csv(csv_file, sep=';')
            all_rows.extend(df[['id_segment','segment_name','expertserver','idsendout','database_id']].to_dict(orient='records'))
        except Exception as e:
            print(f"Erreur lecture {csv_file}: {e}")
    return all_rows
    
@router.post("/reload_index",summary="Recharge les segments")
def reload_index():
    global segment_index
    segment_index = Query2().build_segment_index()
    return {"message": f"Index reconstruit avec {len(segment_index)} segments"}