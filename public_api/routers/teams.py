from typing import List, Union
from fastapi import APIRouter, Depends
from gitential2.core.context import GitentialContext
from gitential2.core.permissions import check_permission

from gitential2.core.teams import (
    list_teams,
    create_team,
    update_team,
    delete_team,
    get_team_with_authors,
    remove_authors_from_team,
    add_authors_to_team,
)
from gitential2.datatypes.permissions import Entity, Action
from gitential2.datatypes.teams import (
    TeamCreate,
    TeamCreateWithAuthorIds,
    TeamPublic,
    TeamUpdate,
    TeamPublicWithAuthors,
)
from ..dependencies import current_user, gitential_context

router = APIRouter(tags=["teams"])


@router.get("/workspaces/{workspace_id}/teams", response_model=List[TeamPublic])
def list_teams_(
    workspace_id: int, current_user=Depends(current_user), g: GitentialContext = Depends(gitential_context)
):
    check_permission(g, current_user, Entity.team, Action.read, workspace_id=workspace_id)
    return list_teams(g, workspace_id)


@router.get("/workspaces/{workspace_id}/teams/{team_id}", response_model=TeamPublicWithAuthors)
def get_team_(
    workspace_id: int,
    team_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.team, Action.read, workspace_id=workspace_id)
    return get_team_with_authors(g, workspace_id, team_id)


@router.post("/workspaces/{workspace_id}/teams", response_model=TeamPublic)
def create_team_(
    workspace_id: int,
    team_create: Union[TeamCreateWithAuthorIds, TeamCreate],
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.team, Action.create, workspace_id=workspace_id)
    return create_team(g, workspace_id, team_create)


@router.put("/workspaces/{workspace_id}/teams/{team_id}", response_model=TeamPublic)
def update_team_(
    workspace_id: int,
    team_id: int,
    team_update: TeamUpdate,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.team, Action.update, workspace_id=workspace_id)
    return update_team(g, workspace_id, team_id, team_update)


@router.delete("/workspaces/{workspace_id}/teams/{team_id}")
def delete_team_(
    workspace_id: int,
    team_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.team, Action.delete, workspace_id=workspace_id)
    return delete_team(g, workspace_id, team_id)


@router.post("/workspaces/{workspace_id}/teams/{team_id}/authors")
def add_authors(
    author_ids: List[int],
    workspace_id: int,
    team_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.team, Action.update, workspace_id=workspace_id)
    return add_authors_to_team(g, workspace_id, team_id, author_ids)


@router.delete("/workspaces/{workspace_id}/teams/{team_id}/authors")
def remove_authors(
    author_ids: List[int],
    workspace_id: int,
    team_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.team, Action.update, workspace_id=workspace_id)
    return remove_authors_from_team(g, workspace_id, team_id, author_ids)
