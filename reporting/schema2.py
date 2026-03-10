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
    taux_clickers: float
    taux_cto: float
    taux_unsubs: float

class DimensionStats(Stats):
    analyses: Analyses={}

class Dimensions(BaseModel):
    age_range:Optional[Dict[str,Any]]=None
    gender:Optional[Dict[str,Any]]=None
    isp:Optional[Dict[str,Any]]=None

class BrandItem(BaseModel):
    name:str
    creativities:str
    sends: int=0
    clicks:int=0
    clickers: int=0
    opens:int=0
    openers: int=0
    unsubs: int=0
    taux_clickers: float=0.0
    taux_cto: float=0.0
    taux_unsubs: float=0.0
    analyses:Analyses={}

class BaseItem(BaseModel):
    database_id: int
    id_routers: str
    tag_id: int
    brands: List[BrandItem]
    sends: int
    clicks:int
    clickers: int
    opens:int
    openers: int
    unsubs: int
    taux_clickers: float=0.0
    taux_openers: float=0.0
    taux_unsubs: float=0.0
    taux_cto: float=0.0
    ca: float=0.0
    ecpm: float=0.0
    classification:str
    date_schedule: List[date]
    SegmentIds: List[int]
    analyses: Analyses={}
    dimensions: Dimensions={}
   

class GlobalAdvertiserStats(BaseModel):
    sends: int=0
    clicks:int=0
    clickers: int=0
    opens:int=0
    openers: int=0
    unsubs: int=0
    ecpm: float=0.0
    ca: float=0.0
    taux_clickers: float=0.0
    taux_openers: float=0.0
    taux_unsubs: float=0.0
    taux_cto: float=0.0
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
    unsubs:int=0
    ca:int=0
    ecpm:float=0.0
    taux_clickers:float=0.0
    taux_openers:float=0.0
    taux_unsubs:float=0.0
    taux_cto:float=0.0
    taux_unsubs:float=0.0
    analyses:Analyses={}

class AdvertiserItem(BaseModel):
    advertiser_id:str
    id_routers:int=0
    brands: List[BrandItem]
    tag:int=0
    sends:int=0
    clicks:int=0
    clickers:int=0
    opens:int=0
    openers:int=0
    unsubs:int=0
    ca:int=0
    ecpm:float=0.0
    taux_clickers:float=0.0
    taux_openers:float=0.0
    taux_unsubs:float=0.0
    taux_cto:float=0.0
    classification:str
    analyses:Analyses={}
    dimensions:Dimensions={}
    
class GlobalBaseResponse(BaseModel):
    database_id:str
    globales:GobalBaseStats
    advertisers:List[AdvertiserItem]
class ListAdvertisersReporting(BaseModel):
    adv_id:int
    name:str
class AdvertisersResponse(BaseModel):
    total:int
    advertisers:List[ListAdvertisersReporting]
class ListBasesReporting(BaseModel):
    database_id:int
    basename:str
class BasesResponse(BaseModel):
    total:int
    bases:List[ListBasesReporting]