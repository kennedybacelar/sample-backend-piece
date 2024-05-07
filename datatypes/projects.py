from datetime import datetime

from enum import Enum
from typing import Optional, List, Tuple
from pydantic import Field


from .common import IDModelMixin, CoreModel, DateTimeModelMixin, ExtraFieldMixin
from .repositories import RepositoryCreate, RepositoryPublic, RepositoryStatus
from .its_projects import ITSProjectCreate
from .export import ExportableModel
from .sprints import Sprint


class ProjectExportDatatype(str, Enum):
    commits = "commits"
    patches = "patches"
    pull_requests = "pull-requests"


class ProjectBase(ExtraFieldMixin, CoreModel):
    name: Optional[str]
    shareable: bool = False
    pattern: Optional[str] = None
    sprints_enabled: bool = False
    sprint: Optional[Sprint] = None


class ProjectCreate(ProjectBase):
    name: str = Field(..., min_length=2, max_length=128)


class ProjectCreateWithRepositories(ProjectCreate):
    repos: List[RepositoryCreate]
    its_projects: Optional[List[ITSProjectCreate]] = None


class ProjectUpdate(ProjectBase):
    pass


class ProjectUpdateWithRepositories(ProjectUpdate):
    repos: List[RepositoryCreate]
    its_projects: Optional[List[ITSProjectCreate]] = []


class ProjectInDB(IDModelMixin, DateTimeModelMixin, ProjectBase, ExportableModel):
    def export_names(self) -> Tuple[str, str]:
        return "project", "projects"

    def export_fields(self) -> List[str]:
        return ["id", "created_at", "updated_at", "name", "shareable", "pattern", "sprints_enabled", "sprint"]


class ProjectPublic(IDModelMixin, DateTimeModelMixin, ProjectBase):
    pass


class ProjectPublicWithRepositories(ProjectPublic):
    repos: List[RepositoryPublic]


class ProjectStatus(CoreModel):
    id: int
    name: str
    status: str
    done: bool
    last_refresh: datetime = datetime.utcnow()
    repos: List[RepositoryStatus]
