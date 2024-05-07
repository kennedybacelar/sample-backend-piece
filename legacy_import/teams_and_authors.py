from typing import List
from pydantic import ValidationError

from structlog import get_logger
from gitential2.core import GitentialContext

from gitential2.datatypes.authors import AuthorInDB
from gitential2.datatypes.authors import AuthorAlias
from gitential2.datatypes.teams import TeamInDB
from gitential2.datatypes.teammembers import TeamMemberInDB

logger = get_logger(__name__)


def import_teams_and_authors(
    g: GitentialContext,
    workspace_id: int,
    legacy_aliases: List[dict],
    legacy_teams_authors: List[dict],
    legacy_teams: List[dict],
    legacy_authors: List[dict],
):
    g.backend.initialize_workspace(workspace_id)
    for author in legacy_authors:
        _import_author(g, author, workspace_id, legacy_aliases)
    for team in legacy_teams:
        _import_team(g, team, workspace_id)
    for team_author in legacy_teams_authors:
        _create_team_author(g, team_author, workspace_id=workspace_id)


def _import_aliases(aliases: List[dict], author_id: int, name: str) -> list:
    tmp = []
    for alias in aliases:
        if alias["author"]["id"] == author_id:
            tmp.append(
                AuthorAlias(
                    email=alias["email"],
                    name=name,
                )
            )
    return tmp


def _import_author(g: GitentialContext, author: dict, workspace_id: int, aliases: List[dict]):
    try:
        processed_aliases = _import_aliases(aliases, author["id"], author["name"])
        author_create = AuthorInDB(
            id=author["id"],
            active=author["active"],
            name=author["name"],
            email=author["email"],
            aliases=processed_aliases,
        )
        logger.info("Importing author", workspace_id=workspace_id)
        g.backend.authors.insert(workspace_id, author["id"], author_create)
    except ValidationError as e:
        print(f"Failed to import author {author['email']}", e)


def _import_team(g: GitentialContext, team: dict, workspace_id: int):
    try:
        team_create = TeamInDB(
            id=team["id"],
            name=team["name"],
            sprints_enabled=team["sprints_enabled"],
            created_at=team["created_at"],
        )
        logger.info("Importing team", workspace_id=workspace_id)
        g.backend.teams.insert(workspace_id, team["id"], team_create)
    except ValidationError as e:
        print(f"Failed to import team {team['name']}", e)


def _create_team_author(g: GitentialContext, team_author: dict, workspace_id: int):
    try:
        team_author_create = TeamMemberInDB(
            id=team_author["id"],
            team_id=team_author["team"]["id"],
            author_id=team_author["author"]["id"],
        )
        logger.info("adding teammember", workspace_id=workspace_id)
        g.backend.team_members.insert(workspace_id, team_author["id"], team_author_create)
    except ValidationError as e:
        print(f"Failed to create teammember {e}")
