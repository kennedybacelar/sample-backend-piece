from typing import Optional, List, Tuple
from gitential2.datatypes.export import ExportableModel

from .common import CoreModel, IDModelMixin, DateTimeModelMixin
from .authors import AuthorInDB
from .sprints import Sprint


class TeamBase(CoreModel):
    name: str
    sprints_enabled: bool = False
    sprint: Optional[Sprint] = None


class TeamCreate(TeamBase):
    pass


class TeamCreateWithAuthorIds(TeamCreate):
    authors: List[int]


class TeamUpdate(TeamBase):
    pass


class TeamInDB(IDModelMixin, DateTimeModelMixin, TeamBase, ExportableModel):
    def export_names(self) -> Tuple[str, str]:
        return ("team", "teams")

    def export_fields(self) -> List[str]:
        return ["id", "created_at", "updated_at", "name", "sprints_enabled", "sprint"]


class TeamPublic(TeamInDB):
    pass


class TeamWithAuthors(TeamInDB):
    authors: List[AuthorInDB]


class TeamPublicWithAuthors(TeamPublic):
    authors: List[AuthorInDB]
