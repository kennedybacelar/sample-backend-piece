from typing import List, Union
from structlog import get_logger
from gitential2.datatypes.teams import TeamCreate, TeamCreateWithAuthorIds, TeamInDB, TeamUpdate, TeamPublicWithAuthors
from gitential2.utils import remove_none
from .context import GitentialContext


logger = get_logger(__name__)


def list_teams(g: GitentialContext, workspace_id: int) -> List[TeamInDB]:
    return list(g.backend.teams.all(workspace_id))


def add_authors_to_team(g: GitentialContext, workspace_id: int, team_id: int, author_ids: List[int]):
    added_members = g.backend.team_members.add_members_to_team(workspace_id, team_id, author_ids)
    return len(added_members)


def remove_authors_from_team(g: GitentialContext, workspace_id: int, team_id: int, author_ids: List[int]):
    return g.backend.team_members.remove_members_from_team(workspace_id, team_id, author_ids)


def create_team(
    g: GitentialContext, workspace_id: int, team_create: Union[TeamCreate, TeamCreateWithAuthorIds]
) -> TeamInDB:
    team_create_, author_ids = (
        (TeamCreate(**team_create.dict(exclude={"authors"})), team_create.authors)
        if isinstance(team_create, TeamCreateWithAuthorIds)
        else (team_create, [])
    )
    logger.info("creating team", workspace_id=workspace_id, name=team_create_.name, author_ids=author_ids)
    team = g.backend.teams.create(workspace_id, team_create_)
    if author_ids:
        add_authors_to_team(g, workspace_id, team.id, author_ids)
    return team


def delete_team(g: GitentialContext, workspace_id: int, team_id: int) -> int:
    logger.info("deleting team", workspace_id=workspace_id, team_id=team_id)
    g.backend.team_members.remove_all_members_from_team(workspace_id, team_id)
    return g.backend.teams.delete(workspace_id, team_id)


def update_team(g: GitentialContext, workspace_id: int, team_id: int, team_update: TeamUpdate) -> TeamInDB:
    return g.backend.teams.update(workspace_id, team_id, team_update)


def get_team_with_authors(g: GitentialContext, workspace_id: int, team_id: int) -> TeamPublicWithAuthors:
    team = g.backend.teams.get_or_error(workspace_id, team_id)
    team_dict = team.dict()
    author_ids = g.backend.team_members.get_team_member_author_ids(workspace_id, team_id)
    authors = remove_none([g.backend.authors.get(workspace_id, author_id) for author_id in author_ids])
    team_dict["authors"] = authors
    return TeamPublicWithAuthors(**team_dict)
