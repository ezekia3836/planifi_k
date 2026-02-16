from pydantic import BaseModel
from typing import Dict, List, Optional,Any
from datetime import date

class Analyses(BaseModel):
    taux_clickers:str
    taux_cto:str
    taux_unsubs:str

class Stats(BaseModel):
    sends: int
    clicks:int
    clickers: int
    opens:int
    openers: int
    unsubs: int
    complaints: int
    bounces:int
    taux_clickers: float
    taux_cto: float
    taux_unsubs: float
    taux_complaints:float=0.0
    taux_bounces:float=0.0

class DimensionStats(Stats):
    analyses: Analyses

class Dimensions(BaseModel):
    age_range:Optional[Dict[str,Any]]=None
    gender:Optional[Dict[str,Any]]=None
    isp:Optional[Dict[str,Any]]=None
    age_civilite_isp:Optional[Dict[str,Any]]=None

class BrandItem(BaseModel):
    name:str
    creativities:str
    sends: int=0
    clicks:int=0
    clickers: int=0
    opens:int=0
    openers: int=0
    unsubs: int=0
    complaints: int=0
    bounces:int=0
    taux_clickers: float=0.0
    taux_cto: float=0.0
    taux_unsubs: float=0.0
    taux_complaints:float=0.0
    taux_bounces:float=0.0

class BaseItem(BaseModel):
    database_id: int
    basename:str
    ktk_id:int
    id_routers: str
    tag_id: int
    brands: List[BrandItem]
    id_focus:int
    client_id:int
    sends: int
    clicks:int
    clickers: int
    opens:int
    openers: int
    unsubs: int
    complaints: int
    bounces:int
    taux_clickers: float=0.0
    taux_openers: float=0.0
    taux_unsubs: float=0.0
    taux_cto: float=0.0
    taux_complaints:float=0.0
    taux_bounces:float=0.0
    ca: float=0.0
    ecpm: float=0.0
    rang: int=0
    date_shedule: List[date]
    SegmentId: int
    subject: str
    analyses: Analyses
    dimensions: Dimensions
   

class GlobalAdvertiserStats(BaseModel):
    sends: int=0
    clicks:int=0
    clickers: int=0
    opens:int=0
    openers: int=0
    unsubs: int=0
    complaints: int=0
    bounces:int=0
    ecpm: float=0.0
    ca: float=0.0
    taux_clickers: float=0.0
    taux_openers: float=0.0
    taux_unsubs: float=0.0
    taux_cto: float=0.0
    taux_complaints:float=0.0
    taux_bounces:float=0.0
    analyses:Analyses={}

class GlobalAdvertiserResponse(BaseModel):
    advertiser_id:str
    globales:Optional[GlobalAdvertiserStats]=None
    bases:List[BaseItem]

class GobalBaseStats(BaseModel):
    sends:int=0
    clicks:int=0
    clickers:int=0
    opens:int=0
    openers:int=0
    removals:int=0
    bounces:int=0
    complaints:int=0
    ca:int=0
    ecpm:float=0.0
    taux_clickers:float=0.0
    taux_openers:float=0.0
    taux_unsubs:float=0.0
    taux_cto:float=0.0
    taux_unsubs:float=0.0
    taux_complaints:float=0.0
    taux_bounces:float=0.0
    analyses:Analyses={}

class AdvertiserItem(BaseModel):
    advertiser_id:str
    client_id:int=0
    id_routers:int=0
    id_focus:int=0
    brands: List[BrandItem]
    tag:int=0
    sends:int=0
    clicks:int=0
    clickers:int=0
    opens:int=0
    openers:int=0
    unsubs:int=0
    complaints:int=0
    bounces:int=0
    ca:int=0
    ecpm:float=0.0
    taux_clickers:float=0.0
    taux_openers:float=0.0
    taux_unsubs:float=0.0
    taux_cto:float=0.0
    taux_complaints:float=0.0
    taux_bounces:float=0.0
    classe:str
    analyses:Analyses={}
    dimensions:Dimensions={}
    
class GlobalBaseResponse(BaseModel):
    database_id:str
    basename:str
    globales:GobalBaseStats
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

class ListAdvertiserReporting(BaseModel):
    adv_id:int
    name:str

class ListAdvertiserReportingResponse(BaseModel):
    total:int
    advertisers:List[ListAdvertiserReporting]

class ListTagsReporting(BaseModel):
    tag_id:int
    tag:str
