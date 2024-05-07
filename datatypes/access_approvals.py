from datetime import datetime

from typing import Optional
from pydantic import validator


from .common import IDModelMixin, CoreModel, ExtraFieldMixin


class AccessApprovalBase(ExtraFieldMixin, CoreModel):
    created_at: Optional[datetime]
    user_id: int
    is_approved: bool
    approved_by: Optional[int] = None

    @validator("created_at", pre=True)
    def default_datetime(cls, value: datetime) -> datetime:
        return value or datetime.utcnow()


class AccessApprovalCreate(AccessApprovalBase):
    pass


class AccessApprovalUpdate(AccessApprovalBase):
    pass


class AccessApprovalInDB(IDModelMixin, AccessApprovalBase):
    pass
