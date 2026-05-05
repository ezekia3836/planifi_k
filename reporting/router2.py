from models.query2 import Query2
from reporting.auto.data import data_auto
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
test = data_auto()
query = Query2()
router = APIRouter(prefix="/reporting", 
    tags=["Reporting"])
@router.get("/advertiser/{adv}", summary="Rapport global d'un advertiser", response_model=GlobalAdvertiserResponse)
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
def get_segment(id_segment: Optional[int]=None,database_id:Optional[int]=None):
    return query.get_segment(id_segment,database_id)
    
@router.post("/reload_index",summary="Recharge les segments")
def reload_index():
    global segment_index
    segment_index = Query2().build_segment_index()
    return {"message": f"Index reconstruit avec {len(segment_index)} segments"}

@router.get("/test/{adv_id}")
def get_test(adv_id):
    return test.get_data(adv_id)

@router.get("/agences",summary="Liste des agences")
def get_agences(agence_id: Optional[int]=None):    
    return query.get_agences(agence_id=agence_id)
@router.get("/tags",summary="Liste des tags")
def get_tags(tags_id: Optional[int]=None):    
    return query.get_tags(tags_id=tags_id)