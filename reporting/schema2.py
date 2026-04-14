from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import date


class Stats(BaseModel):
    sends: int = 0
    clicks: int = 0
    clickers: int = 0
    opens: int = 0
    openers: int = 0
    unsubs: int = 0

    taux_clickers: float = 0.0
    taux_cto: float = 0.0
    taux_unsubs: float = 0.0


class Dimensions(BaseModel):
    age_range: Dict[str, Any] = Field(default_factory=dict)
    gender: Dict[str, Any] = Field(default_factory=dict)
    isp: Dict[str, Any] = Field(default_factory=dict)

class BrandItem(BaseModel):
    name: str
    id_routers: str
    tag_id: int
    creativities: str
    subject: str
    segment_id: int
    comment: str
    date_schedule: List[date] = Field(default_factory=list)

    sends: int = 0
    clicks: int = 0
    clickers: int = 0
    opens: int = 0
    openers: int = 0
    unsubs: int = 0

    taux_clickers: float = 0.0
    taux_cto: float = 0.0
    taux_unsubs: float = 0.0
    ca: float = 0.0
    ecpm: float = 0.0
    dimensions: Dimensions = Field(default_factory=Dimensions)

class BaseItem(BaseModel):
    database_id: int
    brands: List[BrandItem] = Field(default_factory=list)
    sends: int = 0
    clicks: int = 0
    clickers: int = 0
    opens: int = 0
    openers: int = 0
    unsubs: int = 0
    taux_clickers: float = 0.0
    taux_openers: float = 0.0
    taux_unsubs: float = 0.0
    taux_cto: float = 0.0
    ca: float = 0.0
    ecpm: float = 0.0
    classification: str = ""
    dimensions: Dimensions = Field(default_factory=Dimensions)

class DepStatsModel(BaseModel):
    taux_clickers: float = 0.0
    taux_openers: float = 0.0
    taux_unsubs: float = 0.0

class GlobalAdvertiserStats(BaseModel):
    sends: int = 0
    clicks: int = 0
    clickers: int = 0
    opens: int = 0
    openers: int = 0
    unsubs: int = 0
    ecpm: float = 0.0
    ca: float = 0.0
    taux_clickers: float = 0.0
    taux_openers: float = 0.0
    taux_unsubs: float = 0.0
    taux_cto: float = 0.0

    analyse_dep: Dict[str, DepStatsModel] = Field(default_factory=dict)

class GlobalAdvertiserResponse(BaseModel):
    advertiser_id: str
    globales: GlobalAdvertiserStats = Field(default_factory=GlobalAdvertiserStats)
    bases: List[BaseItem] = Field(default_factory=list)


class GobalBaseStats(BaseModel):
    sends: int = 0
    clicks: int = 0
    clickers: int = 0
    opens: int = 0
    openers: int = 0
    unsubs: int = 0
    ca: float = 0.0
    ecpm: float = 0.0
    taux_clickers: float = 0.0
    taux_openers: float = 0.0
    taux_unsubs: float = 0.0
    taux_cto: float = 0.0
    analyse_dep: Dict[str, DepStatsModel] = Field(default_factory=dict)

class AdvertiserItem(BaseModel):
    advertiser_id: int
    brands: List[BrandItem] = Field(default_factory=list)
    sends: int = 0
    clicks: int = 0
    clickers: int = 0
    opens: int = 0
    openers: int = 0
    unsubs: int = 0
    ca: float = 0.0
    ecpm: float = 0.0
    taux_clickers: float = 0.0
    taux_openers: float = 0.0
    taux_unsubs: float = 0.0
    taux_cto: float = 0.0
    classification: str = ""
    dimensions: Dimensions = Field(default_factory=Dimensions)

class GlobalBaseResponse(BaseModel):
    database_id: str
    globales: GobalBaseStats
    advertisers: List[AdvertiserItem] = Field(default_factory=list)

class ListAdvertisersReporting(BaseModel):
    adv_id: int
    name: str

class AdvertisersResponse(BaseModel):
    total: int
    advertisers: List[ListAdvertisersReporting]

class ListBasesReporting(BaseModel):
    database_id: int
    basename: str


class BasesResponse(BaseModel):
    total: int
    bases: List[ListBasesReporting]