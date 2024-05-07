from typing import Optional
from .common import CoreModel, ExtraFieldMixin, IDModelMixin, DateTimeModelMixin


class UserInfoBase(ExtraFieldMixin, CoreModel):
    user_id: Optional[int] = None
    integration_name: str
    integration_type: str
    sub: str
    name: Optional[str] = None
    email: Optional[str] = None
    preferred_username: Optional[str] = None
    profile: Optional[str] = None
    picture: Optional[str] = None
    website: Optional[str] = None


class UserInfoCreate(UserInfoBase):
    pass


class UserInfoUpdate(UserInfoBase):
    pass


class UserInfoInDB(IDModelMixin, DateTimeModelMixin, UserInfoBase):
    user_id: int


class UserInfoPublic(IDModelMixin, DateTimeModelMixin, UserInfoBase):
    pass
