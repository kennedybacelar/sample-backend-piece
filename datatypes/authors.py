from datetime import date
from enum import Enum
from typing import Set, List, Optional, Tuple

from pydantic import BaseModel, root_validator

from gitential2.datatypes.export import ExportableModel
from .common import CoreModel, IDModelMixin, DateTimeModelMixin, ExtraFieldMixin


class AuthorAlias(CoreModel):
    name: Optional[str] = None
    email: Optional[str] = None
    login: Optional[str] = None

    def is_empty(self):
        return (not self.name) and (not self.email) and (not self.login)


class AuthorBase(ExtraFieldMixin, CoreModel):
    active: bool
    name: Optional[str]
    email: Optional[str]
    aliases: List[AuthorAlias]

    # validate_assignnment set to True in order to force automatic validation upon update
    class Config:
        validate_assignment = True

    @root_validator()
    def _handle_null_name_and_email(cls, values: dict) -> dict:
        if not (values["name"] and values["email"]):
            login = None
            for alias in values["aliases"]:
                if not values["name"]:
                    values["name"] = alias.name
                    if not login and alias.login:
                        login = alias.login
                if not values["email"]:
                    values["email"] = alias.email
                if values["name"] and values["email"]:
                    return values
        values["name"] = values["name"] or login or "unknown"
        return values

    @property
    def all_emails(self) -> Set[str]:
        emails = {a.email for a in self.aliases if a.email}
        if self.email not in emails and self.email:
            emails.add(self.email)
        return emails


class AuthorCreate(AuthorBase):
    pass


class AuthorUpdate(AuthorBase):
    pass


class AuthorInDB(IDModelMixin, DateTimeModelMixin, AuthorBase, ExportableModel):
    def export_names(self) -> Tuple[str, str]:
        return "author", "authors"

    def export_fields(self) -> List[str]:
        return ["id", "created_at", "updated_at", "active", "name", "email", "aliases"]


class AuthorPublic(AuthorInDB):
    pass


class IdAndTitle(BaseModel):
    id: int
    title: str


class AuthorPublicExtended(AuthorInDB):
    teams: Optional[List[IdAndTitle]]
    projects: Optional[List[IdAndTitle]]


class AuthorsPublicExtendedSearchResult(BaseModel):
    total: Optional[int] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    authors_list: List[AuthorPublicExtended]


class AuthorNamesAndEmails(BaseModel):
    names: List[str] = []
    emails: List[str] = []


class DateRange(BaseModel):
    start: date
    end: date


class AuthorsSortingType(str, Enum):
    name = "name"
    email = "email"
    active = "active"
    projects = "projects"
    teams = "teams"


class AuthorsSorting(BaseModel):
    type: AuthorsSortingType
    is_desc: bool


class AuthorFilters(BaseModel):
    limit: Optional[int] = 5
    offset: Optional[int] = 0
    sorting_details: Optional[AuthorsSorting] = AuthorsSorting(type=AuthorsSortingType.name, is_desc=False)
    date_range: Optional[DateRange] = None
    developer_names: Optional[List[str]] = []
    developer_emails: Optional[List[str]] = []
    developer_ids: Optional[List[int]] = []
    project_ids: Optional[List[int]] = []
    team_ids: Optional[List[int]] = []
    repository_ids: Optional[List[int]] = []
