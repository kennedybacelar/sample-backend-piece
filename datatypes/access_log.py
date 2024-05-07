from datetime import datetime
from typing import Optional
from pydantic import validator, IPvAnyAddress

from .common import CoreModel, ExtraFieldMixin


class AccessLog(ExtraFieldMixin, CoreModel):
    log_time: Optional[datetime]
    ip_address: Optional[IPvAnyAddress]
    user_id: int
    path: str
    method: str

    @validator("log_time", pre=True)
    def default_datetime(cls, value: datetime) -> datetime:
        return value or datetime.utcnow()
