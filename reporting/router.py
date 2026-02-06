from models.query import Query
from fastapi import APIRouter
from typing import Optional
from fastapi_cache.decorator import cache 
query = Query()
router = APIRouter(
    prefix="/reporting", 
    tags=["Reporting"],   
)

@router.get("/adv/{adv}", summary="Rapport d'un advertiser")
@cache(expire=60)
async def get_report_advertiser(adv: int):
    return query.global_advertiser(adv)

@router.get("/db/{db_id}", summary="Rapport d'une base")
@cache(expire=60)
async def get_report_db(db_id: int):
    return query.global_base(db_id)

@router.get("/calendrier/{adv}", summary="Calendrier d'un advertiser")
@cache(expire=60)
async def calendrier(adv: int):
    return query.calendrier(adv_id=adv)

@router.get("/prog/{adv}", summary="Programme d'un advertiser")
@cache(expire=60)
async def programme(adv: int):
    return query.programmes(adv)

@router.get("/list_advertiser/", summary="Liste des advertisers")
@cache(expire=60)
async def list_advertiser():
    return query.list_advertiser()

@router.get("/list_tags/", summary="Liste des tags")
@cache(expire=60)
async def list_tags():
    return query.list_tags()

@router.get("/top/", summary="Top 10 objets")
@cache(expire=60)
async def top_10_object():
    return query.top_10_objet()

@router.get("/{adv_id}/counts", summary="Comptage par filtre")
@cache(expire=60)
def get_advertiser_counts(
    adv_id: int,
    gender: Optional[str] = None,
    min_age: Optional[int] = None,
    max_age: Optional[int] = None,
    isp: Optional[str] = None
):
    data = query.advertiser_counts(adv_id)
    if gender or min_age or max_age or isp:
        total_filtered = data["filter"](gender=gender, min_age=min_age, max_age=max_age, isp=isp)
        return {"advertiser_id": adv_id, "total": total_filtered}
    return data