from models.query2 import Query2
from service.cache import CacheManager
from reporting.auto.data import data_auto
from fastapi import APIRouter, Query
from typing import Optional
from dotenv import load_dotenv

from reporting.schema2 import (
    GlobalAdvertiserResponse,
    GlobalBaseResponse,
)

load_dotenv("app.env")

segment_index = Query2().build_segment_index()
test = data_auto()

router = APIRouter(
    prefix="/reporting",
    tags=["Reporting"]
)
def cached_or_compute(key: str, compute_func):
    cached = CacheManager.get(key)
    if cached is not None:
        return cached

    data = compute_func()
    CacheManager.set(key, data)
    return data

@router.get(
    "/advertiser/{adv}",
    response_model=GlobalAdvertiserResponse,
    summary="Rapport global d'un advertiser"
)
async def get_adv(adv: int,
    date_schedule: Optional[str] = Query(default=None, description="Date exacte ex: 2024-01-15"),
    date_start:    Optional[str] = Query(default=None, description="Début de plage ex: 2024-01-01"),
    date_end:      Optional[str] = Query(default=None, description="Fin de plage ex: 2024-01-31")
):
    key = CacheManager.key("advertiser", adv,date_schedule,date_start,date_end)

    return cached_or_compute(
        key,
        lambda: Query2().global_advertiser(adv,date_schedule,date_start,date_end)
    )
@router.get(
    "/database/{base_id}",
    response_model=GlobalBaseResponse,
    summary="Rapport global d'une base"
)
async def get_base(
    base_id: int,
    date_schedule: Optional[str] = Query(default=None, description="Date exacte ex: 2024-01-15"),
    date_start:    Optional[str] = Query(default=None, description="Début de plage ex: 2024-01-01"),
    date_end:      Optional[str] = Query(default=None, description="Fin de plage ex: 2024-01-31"),
):
    key = CacheManager.key("database", base_id, date_schedule, date_start, date_end)
    return cached_or_compute(
        key,
        lambda: Query2().global_base(base_id, date_schedule, date_start, date_end)
    )

@router.get("/all_advertisers/", summary="Liste de tous les advertisers dans reporting")
async def all_advertisers(
    date_schedule: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None
):
    key = CacheManager.key(
        "all_advertisers",
        date_schedule or "None",
        date_start or "None",
        date_end or "None",
    )

    return cached_or_compute(
        key,
        lambda: Query2().all_advertisers(
            date_schedule=date_schedule,
            date_start=date_start,
            date_end=date_end
        )
    )


@router.get("/all_bases/", summary="Liste de toutes les bases dans reporting")
async def all_bases(
    date_schedule: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    country: list[str] | None = Query(None),
    tags: list[str] | None = Query(None)
):
    country_key = ",".join(sorted(country)) if country else "None"
    tags_key = ",".join(sorted(tags)) if tags else "None"

    key = CacheManager.key(
        "all_bases",
        date_schedule or "None",
        date_start or "None",
        date_end or "None",
        country_key,
        tags_key
    )

    return cached_or_compute(
        key,
        lambda: Query2().all_bases(
            country=country,
            tags=tags,
            date_schedule=date_schedule,
            date_start=date_start,
            date_end=date_end
        )
    )

@router.get("/segment", summary="Liste de tous les segments")
def get_segment(
    id_segment: Optional[int] = None,
    database_id: Optional[int] = None
):
    key = CacheManager.key(
        "segment",
        id_segment or "None",
        database_id or "None"
    )

    return cached_or_compute(
        key,
        lambda: Query2().get_segment(id_segment, database_id)
    )


@router.post("/reload_index", summary="Recharge les segments")
def reload_index():
    global segment_index
    segment_index = Query2().build_segment_index()
    return {"message": f"Index reconstruit avec {len(segment_index)} segments"}


@router.get("/agences", summary="Liste de toutes les agences")
def get_agences(agence_id: Optional[int] = None):
    key = CacheManager.key("agences", agence_id or "all")

    return cached_or_compute(
        key,
        lambda: Query2().get_agences(agence_id=agence_id)
    )


@router.get("/tags", summary="Liste de tout les tags")
def get_tags(tags_id: Optional[int] = None):
    key = CacheManager.key("tags", tags_id or "all")

    return cached_or_compute(
        key,
        lambda: Query2().get_tags(tags_id=tags_id)
    )


@router.get("/databases", summary="Liste de toutes les databases")
def get_databases(database_id: Optional[int] = None):
    key = CacheManager.key("databases", database_id or "all")

    return cached_or_compute(
        key,
        lambda: Query2().get_databases(database_id=database_id)
    )
