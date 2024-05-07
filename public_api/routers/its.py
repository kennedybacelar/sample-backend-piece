from typing import Optional, List

from fastapi import APIRouter, Depends, Query, Response

from gitential2.core.context import GitentialContext
from gitential2.core.its import (
    list_available_its_projects,
    list_project_its_projects,
    get_available_its_project_groups,
    DEFAULT_ITS_PROJECTS_LIMIT,
    DEFAULT_ITS_PROJECTS_OFFSET,
    DEFAULT_ITS_PROJECTS_ORDER_BY_OPTION,
    DEFAULT_ITS_PROJECTS_ORDER_BY_DIRECTION,
    get_available_its_projects_paginated,
    ITSProjectCacheOrderByOptions,
    ITSProjectOrderByDirections,
    refresh_cache_of_its_projects_for_user_or_users,
)
from gitential2.core.permissions import check_permission
from gitential2.datatypes.permissions import Entity, Action
from ..dependencies import current_user, gitential_context
from ...datatypes.user_its_projects_cache import UserITSProjectGroup
from ...utils.router_utils import get_paginated_response

router = APIRouter(tags=["its"])


@router.get("/workspaces/{workspace_id}/available-its-project-groups", response_model=List[UserITSProjectGroup])
def available_its_project_groups(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_available_its_project_groups(g=g, workspace_id=workspace_id)


@router.get("/workspaces/{workspace_id}/available-its-projects")
def available_its_projects(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return list_available_its_projects(g=g, workspace_id=workspace_id)


@router.post("/workspaces/{workspace_id}/refresh-its-projects-cache")
def refresh_its_projects_cache(
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
    refresh_cache_of_its_projects_for_user_or_users(
        g=g, workspace_id=workspace_id, refresh_cache=refresh_cache, force_refresh_cache=force_refresh_cache
    )
    return True


@router.get("/workspaces/{workspace_id}/available-its-projects-paginated")
def available_its_projects_paginated(
    response: Response,
    workspace_id: int,
    refresh_cache: Optional[bool] = Query(False, alias="refreshCache"),
    force_refresh_cache: Optional[bool] = Query(False, alias="forceRefreshCache"),
    limit: Optional[int] = Query(DEFAULT_ITS_PROJECTS_LIMIT, alias="limit"),
    offset: Optional[int] = Query(DEFAULT_ITS_PROJECTS_OFFSET, alias="offset"),
    order_by_option: Optional[ITSProjectCacheOrderByOptions] = Query(
        DEFAULT_ITS_PROJECTS_ORDER_BY_OPTION, alias="sortingOption"
    ),
    order_by_direction: Optional[ITSProjectOrderByDirections] = Query(
        DEFAULT_ITS_PROJECTS_ORDER_BY_DIRECTION, alias="sortingDirection"
    ),
    integration_type: Optional[str] = Query(None, alias="integrationType"),
    namespace: Optional[str] = Query(None, alias="namespace"),
    credential_id: Optional[int] = Query(None, alias="credentialId"),
    search_pattern: Optional[str] = Query(None, alias="searchPattern"),
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)

    total_count, limit, offset, its_projects = get_available_its_projects_paginated(
        g=g,
        workspace_id=workspace_id,
        refresh_cache=refresh_cache,
        force_refresh_cache=force_refresh_cache,
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
        items=its_projects,
        total_count=total_count,
        limit=limit,
        offset=offset,
    )


@router.get("/workspaces/{workspace_id}/projects/{project_id}/its-projects")
def project_its_projects(
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return list_project_its_projects(g, workspace_id, project_id=project_id)
