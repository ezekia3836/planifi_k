from models.query2 import Query2
from models.recommended import Recommended
from service.cache import CacheManager
from reporting.auto.data import data_auto
from fastapi import APIRouter, Query, Depends
import os
from datetime import date,timedelta
from typing import Literal
from dateutil.relativedelta import relativedelta
from typing import Optional
from dotenv import load_dotenv
from user.dependencies import make_auth_dependency as curent_user
from reporting.schema2 import (
    GlobalAdvertiserResponse,
    GlobalBaseResponse,
)

load_dotenv("app.env")

segment_index = Query2().build_segment_index()
test = data_auto()

router = APIRouter(
    prefix="/reporting",
    tags=["Reporting"],
    dependencies=[Depends(curent_user(os.getenv("SECRET_KEY")))]
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
async def get_adv(
    adv: int,
    tag_id:          Optional[int] = Query(default=None,  description="Filtrer par tag ex: 8"),
    date_schedule:   Optional[str] = Query(default=None,  description="Date exacte ex: 2024-01-15"),
    date_start:      Optional[str] = Query(default=None,  description="Début de plage ex: 2024-01-01"),
    date_end:        Optional[str] = Query(default=None,  description="Fin de plage ex: 2024-01-31"),
    include_o_age:    bool = Query(default=True, description="Inclure les contacts sans âge (O_age)"),
    include_o_gender: bool = Query(default=True, description="Inclure les contacts sans genre (O_gender)"),
    include_o_isp:    bool = Query(default=True, description="Inclure les contacts sans ISP (O_isp)"),
):
    if not date_schedule and not (date_start and date_end):
        today      = date.today()
        date_end   = str(today.replace(day=1) + relativedelta(months=1) - timedelta(days=1))  # fin du mois courant
        date_start = str(today.replace(day=1) - relativedelta(days=1))                       # fin du mois précédent

    key = CacheManager.key(
        "advertiser", adv, tag_id, date_schedule, date_start, date_end,
        include_o_age, include_o_gender, include_o_isp
    )
    return cached_or_compute(
        key,
        lambda: Query2().global_advertiser(
            adv, tag_id, date_schedule, date_start, date_end,
            include_o_age,
            include_o_gender,
            include_o_isp
        )
    )
@router.get("/database/{base_id}", response_model=GlobalBaseResponse,summary="Rapport global d'une base")
async def get_base(
    base_id: int,
    date_schedule: Optional[str] = Query(default=None, description="Date exacte ex: 2024-01-15"),
    date_start:    Optional[str] = Query(default=None, description="Début de plage ex: 2024-01-01"),
    date_end:      Optional[str] = Query(default=None, description="Fin de plage ex: 2024-01-31"),
    include_o_age:    bool = Query(default=True, description="Inclure les contacts sans âge (O_age)"),
    include_o_gender: bool = Query(default=True, description="Inclure les contacts sans genre (O_gender)"),
    include_o_isp:    bool = Query(default=True, description="Inclure les contacts sans ISP (O_isp)"),
):
    if not date_schedule and not (date_start and date_end):
        today      = date.today()
        date_end   = str(today.replace(day=1) + relativedelta(months=1) - timedelta(days=1))  # fin du mois courant
        date_start = str(today.replace(day=1) - relativedelta(days=1))                       # fin du mois précédent
    key = CacheManager.key("database", base_id, date_schedule, date_start, date_end,include_o_age,include_o_gender,include_o_isp)
    return cached_or_compute(
        key,
        lambda: Query2().global_base(base_id, date_schedule, date_start, date_end,include_o_age,include_o_gender,include_o_isp)
    )

@router.get("/all_advertisers/", summary="Liste de tous les advertisers dans reporting")
async def all_advertisers(
    country: list[str] | None = Query(None),
    date_schedule: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None
):
    if not date_schedule and not (date_start and date_end):
        today      = date.today()
        date_end   = str(today.replace(day=1) + relativedelta(months=1) - timedelta(days=1))  # fin du mois courant
        date_start = str(today.replace(day=1) - relativedelta(days=1))                       # fin du mois précédent
    country_key = ",".join(sorted(country)) if country else "None"
    key = CacheManager.key(
        "all_advertisers",
        date_schedule or "None",
        date_start or "None",
        date_end or "None",
        country_key
    )

    return cached_or_compute(
        key,
        lambda: Query2().all_advertisers(
            date_schedule=date_schedule,
            date_start=date_start,
            date_end=date_end,
            country = country,
        )
    )

@router.get("/all_bases/", summary="Liste de toutes les bases dans reporting")
async def all_bases(
    tags: list[str] | None = Query(None),
    date_schedule: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    country: list[str] | None = Query(None)
):
    if not date_schedule and not (date_start and date_end):
        today      = date.today()
        date_end   = str(today.replace(day=1) + relativedelta(months=1) - timedelta(days=1))
        date_start = str(today.replace(day=1) - relativedelta(days=1))

    # Normalisation : si "all" est présent (n'importe quelle casse), on ignore le reste
    if country and any(c.lower() == "all" for c in country):
        country = "all"

    country_key = (
        "all" if country == "all"
        else ",".join(sorted(country)) if country else "None"
    )
    tags_key = ",".join(sorted(tags)) if tags else "None"
    key = CacheManager.key(
        "all_bases",
        tags_key,
        date_schedule or "None",
        date_start or "None",
        date_end or "None",
        country_key
    )
    return cached_or_compute(
        key,
        lambda: Query2().all_bases(
            tags=tags,
            date_schedule=date_schedule,
            date_start=date_start,
            date_end=date_end,
            country=country
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
@router.get("/filter_by_tags", summary="Filtrer les departements par tag et base")
def filter_by_tags(
    tag_id: int = Query(..., description="ID du tag à filtrer"),
    database_id: int = Query(..., description="ID de la base à filtrer"),
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
):
    if not date_start and not date_end:
        today  = date.today()
        date_end   = str(today.replace(day=1) + relativedelta(months=1) - timedelta(days=1))  # fin du mois courant
        date_start = str(today.replace(day=1) - relativedelta(days=1))                       # début du mois précédent
    dep = Query2().filter_by_tags(tag_id=tag_id, database_id=database_id,date_start=date_start,date_end=date_end)
    return dep

@router.get('/country',summary="Liste pays")
def list_country(country_id: Optional[int]=None):
    country = Query2().get_country(country_id=country_id)
    return country

@router.get(
    "/top_advertisers",
    summary="Top annonceurs par tag et par mois"
)
def top_advertisers_by_tag(tag_id=None, date_start=None, date_end=None,country='FR', sort_by="ecpm"):
    today = date.today()
    if not date_start and not date_end:
        date_start = date(today.year, 1, 1)
        date_end   = date(today.year, 12, 31)

    key  = CacheManager.key("top_advertisers_by_tag", tag_id, str(date_start), str(date_end),country, sort_by)
    data = CacheManager.get(key)
    if data is None:
        data = Query2().top_advertisers_by_tag(tag_id=tag_id, date_start=date_start, date_end=date_end,country=country, sort_by=sort_by)
        CacheManager.set(key, data)
    return data

@router.get("/top_base")
def top_base(tag_id=None, date_start=None, date_end=None,country='FR'):
    today = date.today()
    if not date_start and not date_end:
        date_start = date(today.year, 1, 1)
        date_end   = date(today.year, 12, 31)

    key  = CacheManager.key("top_base", tag_id, str(date_start), str(date_end),country)
    data = CacheManager.get(key)
    if data is None:
        data = Query2().top_10_bases(tag_id=tag_id, date_start=date_start, date_end=date_end,country=country)
        CacheManager.set(key, data)
    return data

@router.get("/recommend", summary="Recommandation hiérarchique tags → advertisers → bases")
def recommend(
    sort_by: Literal["ecpm", "clickers", "ca"] = Query(
        default="ecpm",
        description="Critère de classement : 'ecpm', 'clickers' ou 'ca'"
    ),
    country: str = Query(
        default="FR",
        description="Code pays : 'FR', 'ES', 'IT', ..."
    )
):
    key  = CacheManager.key("recommend", sort_by, country)
    data = CacheManager.get(key)
    if data is None:
        data = Recommended().recommend(sort_by=sort_by, country=country)
        CacheManager.set(key, data)
    return data