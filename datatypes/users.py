from datetime import datetime
from typing import Optional, List
from pydantic import Field

from .common import CoreModel, ExtraFieldMixin, IDModelMixin, DateTimeModelMixin
from .userinfos import UserInfoBase


class InactiveUsers(CoreModel):
    user_id: int
    login: Optional[str]
    email: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    last_login: Optional[datetime]


class UserBase(ExtraFieldMixin, CoreModel):
    login: Optional[str] = Field(None, max_length=128)
    email: Optional[str] = Field(None, max_length=256)
    is_admin: bool = False
    marketing_consent_accepted: bool = False
    first_name: Optional[str] = Field(None, max_length=256)
    last_name: Optional[str] = Field(None, max_length=256)
    company_name: Optional[str] = Field(None, max_length=256)
    position: Optional[str] = Field(None, max_length=256)
    development_team_size: Optional[str] = Field(None, max_length=32)
    registration_ready: bool = False
    login_ready: bool = False
    is_active: bool = True
    stripe_customer_id: Optional[str] = None

    @classmethod
    def from_user_info(cls, user_info: UserInfoBase):
        return cls(login=user_info.preferred_username or user_info.sub, email=user_info.email)

    @property
    def full_name(self) -> str:
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        elif self.login:
            return self.login
        elif self.email:
            return self.email.split("@")[0]  # pylint: disable=no-member
        else:
            return "[Anonymous]"


class UserCreate(UserBase):
    email: str


class UserRegister(UserCreate):
    reseller_id: Optional[str] = None
    reseller_code: Optional[str] = None


class UserUpdate(UserBase):
    pass


class UserInDB(IDModelMixin, DateTimeModelMixin, UserBase):
    pass


class UserPublic(IDModelMixin, DateTimeModelMixin, UserBase):
    pass


class UserInAdminRepr(IDModelMixin, DateTimeModelMixin):
    name: Optional[str] = Field(None, max_length=128)
    email: Optional[str] = Field(None, max_length=256)
    is_admin: bool = False
    is_active: bool = True
    last_interaction_at: Optional[datetime] = None
    access_approved: Optional[bool] = None


class UserHeader(IDModelMixin, CoreModel):
    login: Optional[str] = Field(None, max_length=128)


class UserPurged(IDModelMixin, DateTimeModelMixin):
    email: str
    deleted_at: datetime
    workspaces_purged: List[int]
