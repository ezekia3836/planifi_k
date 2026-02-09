from models.query import Query
from fastapi import APIRouter,Depends
from fastapi_cache.decorator import cache 
from reporting.security import verify_internal_token
from dotenv import load_dotenv
import os
load_dotenv("app.env")
API_TOKEN=os.getenv("API_TOKEN")
query=Query()
from reporting.schema import (
    ListAdvertisersResponse,
    ListTagsResponse
)

router = APIRouter(
    prefix="/reporting", 
    tags=["Reporting"],
    dependencies=[Depends(verify_internal_token)]
)

@router.get("/list_advertiser/", summary="Liste des advertisers",response_model=ListAdvertisersResponse)
@cache(expire=60)
async def list_advertiser():
    return query.list_advertiser()

@router.get("/list_tags/", summary="Liste des Tags",response_model=ListTagsResponse)
@cache(expire=60)
async def list_tags():
    return query.list_tags()