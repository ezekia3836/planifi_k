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

class ModelItem(BaseModel):
    model: str
    payvalue: float | None = None
    comment: str | None = None
    
class BrandItem(BaseModel):
    name: Optional[str] = None
    id_routers: List[int] = Field(default_factory=list)
    tag_id: Optional[int] = None
    agence_id: Optional[int] = None
    creativities: Optional[str] = None
    subject: Optional[str] = None
    models: List[ModelItem] = Field(default_factory=list)
    segment_id: List[int] = Field(default_factory=list)
    ListId: List[int] = Field(default_factory=list)
    ListName: List[str] = Field(default_factory=list)
    date_schedule: List[date] = Field(default_factory=list)
    sends: Optional[int] = 0
    clickers: Optional[int] = 0
    openers: Optional[int] = 0
    unsubs: Optional[int] = 0
    taux_clickers: Optional[float] = 0.0
    taux_cto: Optional[float] = 0.0
    taux_unsubs: Optional[float] = 0.0
    taux_openers: Optional[float] = 0.0
    clicks_val: Optional[int] = 0
    leads_val: Optional[int] = 0
    volume_val: Optional[int] = 0
    ca: Optional[float] = 0.0
    ecpm: Optional[float] = 0.0
    dimensions: Optional[Dimensions] = Field(default_factory=Dimensions)

class DimensionItem(BaseModel):
    value: str
    sends: float
    clickers: int
    openers: int
    unsubs:int
    taux_clickers:float
    taux_openers:float
    taux_unsubs:float
    taux_cto: float

class DimensionAnalysis(BaseModel):
    privilegier: Optional[List[DimensionItem]] = None
    eviter: Optional[List[DimensionItem]] = None

class AnalyseDimensions(BaseModel):
    age_range: DimensionAnalysis = DimensionAnalysis()
    gender: DimensionAnalysis = DimensionAnalysis()
    isp: DimensionAnalysis = DimensionAnalysis()

class BaseItem(BaseModel):
    database_id: int
    brands: List[BrandItem] = Field(default_factory=list)
    sends: int = 0
    clickers: int = 0
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
    clickers: int =0
    taux_clickers: float = 0.0
    taux_openers: float = 0.0
    taux_unsubs: float = 0.0

class GlobalAdvertiserStats(BaseModel):
    sends: int = 0
    clickers: int = 0
    openers: int = 0
    unsubs: int = 0
    ecpm: float = 0.0
    ca: float = 0.0
    taux_clickers: float = 0.0
    taux_openers: float = 0.0
    taux_unsubs: float = 0.0
    taux_cto: float = 0.0
    analyses: Dict[str, str] = Field(default_factory=dict)
    analyse_dep: Dict[str, DepStatsModel] = Field(default_factory=dict)
    recommendation_segments: AnalyseDimensions = Field(default_factory=AnalyseDimensions)

class GlobalAdvertiserResponse(BaseModel):
    advertiser_id: str
    advertiser_name: Optional[str] = None
    globales: GlobalAdvertiserStats = Field(default_factory=GlobalAdvertiserStats)
    bases: List[BaseItem] = Field(default_factory=list)

class GobalBaseStats(BaseModel):
    sends: int = 0
    clickers: int = 0
    openers: int = 0
    unsubs: int = 0
    ca: float = 0.0
    ecpm: float = 0.0
    taux_clickers: float = 0.0
    taux_openers: float = 0.0
    taux_unsubs: float = 0.0
    taux_cto: float = 0.0
    analyses: Dict[str, str] = Field(default_factory=dict)
    analyse_dep: Dict[str, DepStatsModel] = Field(default_factory=dict)
    recommendation_segments: AnalyseDimensions = Field(default_factory=AnalyseDimensions)
    
class AdvertiserItem(BaseModel):
    advertiser_id: int
    advertiser_name: Optional[str] = None
    brands: List[BrandItem] = Field(default_factory=list)
    sends: int = 0
    clickers: int = 0
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