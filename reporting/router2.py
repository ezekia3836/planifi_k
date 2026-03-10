from models.query2 import Query2
from fastapi import APIRouter,Depends, Query
from typing import Optional
from fastapi_cache.decorator import cache 
from reporting.schema2 import (
    GlobalAdvertiserResponse,
    GlobalBaseResponse,
    AdvertisersResponse,
    BasesResponse
)
query = Query2()
router = APIRouter(prefix="/reporting", 
    tags=["Reporting"])
@router.get("/advertiser/{adv}", summary="Rapport global d'un advertiser2",response_model=GlobalAdvertiserResponse)
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
    date_end:Optional[str]=None,
    tags:list[str] | None =Query(None)
):
    return query.all_advertisers(date_schedule=date_schedule,date_start=date_start,date_end=date_end,tags=tags)

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