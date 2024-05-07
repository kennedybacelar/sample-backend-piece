from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, validator


class CoreModel(BaseModel):
    """
    Any common logic to be shared by all models goes here
    """

    @classmethod
    def primary_key(cls) -> List[str]:
        return []

    @classmethod
    def unique_fields(cls) -> List[str]:
        return []


class DateTimeModelMixin(BaseModel):
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    @validator("created_at", "updated_at", pre=True)
    def default_datetime(cls, value: datetime) -> datetime:
        return value or datetime.utcnow()


class ExtraFieldMixin(BaseModel):
    extra: Optional[dict] = None


class IDModelMixin(BaseModel):
    id: int

    @property
    def id_(self) -> int:
        return self.id


class StringIdModelMixin(BaseModel):
    id: str

    @property
    def id_(self) -> str:
        return self.id
