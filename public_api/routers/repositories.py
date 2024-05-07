# pylint: skip-file
from typing import List, Optional

from fastapi import APIRouter, Depends, Query, Response

from gitential2.core.context import GitentialContext
from gitential2.core.permissions import check_permission
from gitential2.core.repositories import (
    get_available_repositories_for_workspace,
    search_public_repositories,
    get_repository,
    list_repositories,
    create_repositories,
    delete_repositories,
    list_project_repositories,
    get_available_repo_groups,
    get_available_repositories_paginated,
    DEFAULT_REPOS_LIMIT,
    DEFAULT_REPOS_OFFSET,
    DEFAULT_REPOS_ORDER_BY_OPTION,
    DEFAULT_REPOS_ORDER_BY_DIRECTION,
    RepoCacheOrderByDirections,
    RepoCacheOrderByOptions,
    refresh_cache_of_repositories_for_user_or_users,
)
from gitential2.datatypes.permissions import Entity, Action
from gitential2.datatypes.repositories import RepositoryCreate
from ..dependencies import current_user, gitential_context
from ...datatypes.user_repositories_cache import UserRepositoryGroup
from ...utils.router_utils import get_paginated_response

router = APIRouter(tags=["repositories"])


@router.get("/workspaces/{workspace_id}/available-repo-groups", response_model=List[UserRepositoryGroup])
def available_repo_groups(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_available_repo_groups(g=g, workspace_id=workspace_id)


@router.post("/workspaces/{workspace_id}/available-repos")
def available_repos(
    workspace_id: int,
    user_organization_name_list: Optional[List[str]] = None,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_available_repositories_for_workspace(
        g=g, workspace_id=workspace_id, user_organization_name_list=user_organization_name_list
    )


@router.post("/workspaces/{workspace_id}/refresh-repos-cache")
def refresh_repos_cache(
    workspace_id: int,
    refresh_cache: Optional[bool] = Query(False, alias="refreshCache"),
    force_refresh_cache: Optional[bool] = Query(False, alias="forceRefreshCache"),
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(
        g=g,
        current_user=current_user,
        entity=Entity.user,
        action=Action.update,
        enable_when_on_prem=True,
        workspace_id=workspace_id,
    )
    refresh_cache_of_repositories_for_user_or_users(
        g=g, workspace_id=workspace_id, refresh_cache=refresh_cache, force_refresh_cache=force_refresh_cache
    )
    return True


@router.get("/workspaces/{workspace_id}/available-repos-paginated")
def available_repos_paginated(
    response: Response,
    workspace_id: int,
    refresh_cache: Optional[bool] = Query(False, alias="refreshCache"),
    force_refresh_cache: Optional[bool] = Query(False, alias="forceRefreshCache"),
    user_organization_name_list: Optional[List[str]] = Query(None, alias="userOrganizationNameList"),
    limit: Optional[int] = Query(DEFAULT_REPOS_LIMIT, alias="limit"),
    offset: Optional[int] = Query(DEFAULT_REPOS_OFFSET, alias="offset"),
    order_by_option: Optional[RepoCacheOrderByOptions] = Query(DEFAULT_REPOS_ORDER_BY_OPTION, alias="sortingOption"),
    order_by_direction: Optional[RepoCacheOrderByDirections] = Query(
        DEFAULT_REPOS_ORDER_BY_DIRECTION, alias="sortingDirection"
    ),
    integration_type: Optional[str] = Query(None, alias="integrationType"),
    namespace: Optional[str] = Query(None, alias="namespace"),
    credential_id: Optional[int] = Query(None, alias="credentialId"),
    search_pattern: Optional[str] = Query(None, alias="searchPattern"),
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)

    total_count, limit, offset, repositories = get_available_repositories_paginated(
        g=g,
        workspace_id=workspace_id,
        refresh_cache=refresh_cache,
        force_refresh_cache=force_refresh_cache,
        user_organization_name_list=user_organization_name_list,
        limit=limit,
        offset=offset,
        order_by_option=order_by_option,
        order_by_direction=order_by_direction,
        integration_type=integration_type,
        namespace=namespace,
        credential_id=credential_id,
        search_pattern=search_pattern,
    )

    return get_paginated_response(
        response=response,
        items=repositories,
        total_count=total_count,
        limit=limit,
        offset=offset,
    )


@router.get("/workspaces/{workspace_id}/search-public-repos")
def search_public_repos(
    workspace_id: int,
    search: str,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return search_public_repositories(g, workspace_id, search=search)


@router.get("/workspaces/{workspace_id}/repos")
def workspace_repos(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return list_repositories(g, workspace_id)


@router.get("/workspaces/{workspace_id}/repos/{repository_id}")
def get_repo(
    workspace_id: int,
    repository_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):

    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_repository(g, workspace_id, repository_id)


@router.post("/workspaces/{workspace_id}/repos")
def add_repos(
    workspace_id: int,
    repository_creates: List[RepositoryCreate],
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.repository, Action.create, workspace_id=workspace_id)
    return create_repositories(g, workspace_id, repository_creates)


@router.delete("/workspaces/{workspace_id}/repos")
def delete_repos(
    workspace_id: int,
    repository_ids: List[int],
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.repository, Action.delete, workspace_id=workspace_id)
    return delete_repositories(g, workspace_id, repository_ids)


@router.get("/workspaces/{workspace_id}/projects/{project_id}/repos")
def project_repos(
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return list_project_repositories(g, workspace_id, project_id=project_id)
