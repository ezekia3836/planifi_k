from models.query2 import Query2
from fastapi import APIRouter,Depends
from typing import Optional
from fastapi_cache.decorator import cache 
from reporting.security import verify_internal_token
from reporting.schema2 import (
    GlobalAdvertiserResponse,
    GlobalBaseResponse,
    AdvertisersResponse,
    BasesResponse
)
query = Query2()
router = APIRouter(prefix="/reporting", 
    tags=["Reporting"],dependencies=[Depends(verify_internal_token)])
@router.get("/advertiser/{adv}", summary="Rapport global d'un advertiser2",response_model=GlobalAdvertiserResponse)
@cache(expire=60)
async def get_report_advertiser(adv: int):
    return query.global_advertiser(adv)
@router.get("/database/{base_id}", summary="Rapport global d'une base",response_model=GlobalBaseResponse)
@cache(expire=60)
async def get_report_base(base_id:int):
    return query.global_base(base_id)
@router.get("/list_advertisers/",summary="Liste des advertisers dans reporting",response_model=AdvertisersResponse)
@cache(expire=60)
async def get_list_advertisers():
    return query.list_advertisers()
@router.get("/list_bases/",summary="Liste des bases dans reporting",response_model=BasesResponse)
@cache(expire=60)
async def get_list_bases():
    return query.list_bases()
"""
@router.get("/objet/{adv_id}")
@cache(expire=60)
async def get_best_objet(adv_id:int):
    return query.recommend_subject(adv_id)"""
