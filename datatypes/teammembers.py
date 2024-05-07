from typing import Optional, Tuple, List
from gitential2.datatypes.export import ExportableModel

from .common import IDModelMixin, CoreModel


class TeamMemberBase(CoreModel):
    team_id: Optional[int] = None
    author_id: Optional[int] = None


class TeamMemberCreate(TeamMemberBase):
    team_id: int
    author_id: int


class TeamMemberUpdate(TeamMemberBase):
    team_id: int
    author_id: int


class TeamMemberInDB(IDModelMixin, TeamMemberBase, ExportableModel):
    team_id: int
    author_id: int

    def export_names(self) -> Tuple[str, str]:
        return ("team_member", "team_members")

    def export_fields(self) -> List[str]:
        return ["id", "team_id", "author_id"]
