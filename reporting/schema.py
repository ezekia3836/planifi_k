from pydantic import BaseModel
from typing import Dict, List, Optional,Any
from datetime import date

class Analyses(BaseModel):
    taux_clicks:str
    taux_cto:str
    taux_unsubs:str

class Stats(BaseModel):
    sends: int
    clickers: int
    openers: int
    unsubs: int
    complaints: int
    taux_clicks: float
    taux_cto: float
    taux_unsubs: float

class DimensionStats(Stats):
    analyses: Analyses

class Dimensions(BaseModel):
    age_range:Optional[Dict[str,Any]]=None
    civilite:Optional[Dict[str,Any]]=None
    isp:Optional[Dict[str,Any]]=None
    age_civilite_isp:Optional[Dict[str,Any]]=None

class BaseItem(BaseModel):
    database_id: int
    id_routers: str
    tag_id: int
    brand: str
    sends: int
    clickers: int
    openers: int
    unsubs: int
    complaints: int
    taux_clicks: float
    taux_opens: float
    taux_unsubs: float
    taux_cto: float
    ca: float
    ecpm: float
    date_shedule: List[date]
    SegmentId: int
    subject: str
    dimensions: Dimensions
    analyses: Analyses
    rang: int

class GlobalAdvertiserStats(BaseModel):
    sends: int
    clickers: int
    openers: int
    unsubs: int
    complaints: int
    ecpm: float
    ca: float
    taux_clicks: float
    taux_opens: float
    taux_unsubs: float
    taux_cto: float

class GlobalAdvertiserResponse(BaseModel):
    advertiser_id:str
    globales:GlobalAdvertiserStats
    bases:List[BaseItem]


class GobalBaseStats(BaseModel):
    sends_total:int
    clicks_total:int
    opens_total:int
    removals_total:int
    ca_total:int
    ecpm:float
    taux_clicks:float
    taux_opens:float
    taux_unsubs:float
    taux_cto:float
    taux_unsubs:float
    analyses:Analyses

class AdvertiserItem(BaseModel):
    advertiser_id:str
    sends:int
    clicks:int
    opens:int
    removals:int
    ca:int
    dimensions:Dimensions
    ecpm:float
    taux_clicks:float
    taux_opens:float
    taux_unsubs:float
    taux_cto:float
    classe:str
    analyses:Analyses

class GlobalBaseResponse(BaseModel):
    database_id:str
    globale_base:GobalBaseStats
    advertisers:List[AdvertiserItem]

class CountFilter(BaseModel):
    gender: Optional[str] = None
    min_age: Optional[int] = None
    max_age: Optional[int] = None
    isp: Optional[str] = None
    total:int

class CountFilterResponse(BaseModel):
    adv_id:int
    comptage:Optional[CountFilter]=None

class ListTags(BaseModel):
    id:int
    name:str
    dwtag:str

class ListTagsResponse(BaseModel):
    total:int
    tags:List[ListTags]

class ListAdvertisers(BaseModel):
    id:int
    name:str

class ListAdvertisersResponse(BaseModel):
    total:int
    advertisers:List[ListAdvertisers]

