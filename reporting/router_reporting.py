from models.query import Query
from fastapi import APIRouter,Depends
from typing import Optional
from fastapi_cache.decorator import cache 
from reporting.security import verify_internal_token
from reporting.schema import (
    GlobalAdvertiserResponse,
    GlobalBaseResponse,
    CountFilterResponse,
    ListAdvertiserReportingResponse
)
query = Query()
router = APIRouter(
    prefix="/reporting", 
    tags=["Reporting"], 
    dependencies=[Depends(verify_internal_token)]
)

@router.get("/adv/{adv}", summary="Rapport d'un advertiser",response_model=GlobalAdvertiserResponse)
@cache(expire=60)
async def get_report_advertiser(adv: int):
    return query.global_advertiser(adv)

@router.get("/db/{db_id}", summary="Rapport d'une base",response_model=GlobalBaseResponse)
@cache(expire=60)
async def get_report_db(db_id: int):
    return query.global_base(db_id)

@router.get("/prog/{adv}", summary="Programme d'un advertiser")
@cache(expire=60)
async def programme(adv: int):
    return query.programmes(adv)
@router.get("/top/", summary="Top 10 objets")
@cache(expire=60)
async def top_10_object():
    return query.top_10_objet()

@router.get("/{adv_id}/counts", summary="Comptage par filtre(age,genre,isp)", response_model=CountFilterResponse)
def get_advertiser_counts(adv_id: int, gender: Optional[str] = None, min_age: Optional[int] = None, max_age: Optional[int] = None, isp: Optional[str] = None):
    data = query.advertiser_counts(adv_id)

    if gender or min_age or max_age or isp:
        filtered = data["filter"](
            gender=gender,
            min_age=min_age,
            max_age=max_age,
            isp=isp
        )
        return {
            "adv_id": adv_id,   
            "comptage": filtered["comptage"]
        }

    total_global = sum(item["total"] for item in data["details"])
    return {
        "adv_id": adv_id,
        "comptage": {
            "gender": None,
            "min_age": None,
            "max_age": None,
            "isp": None,
            "total": total_global
        }
    }
@router.get("/advertisers/",summary="Liste des advertisers reporting",response_model=ListAdvertiserReportingResponse)
async def get_list_adv_ids():
    return query.liste_adv_id_reporting()