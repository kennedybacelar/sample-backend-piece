from typing import List

from fastapi import APIRouter, Depends
from structlog import get_logger

from gitential2.core.context import GitentialContext
from gitential2.core.permissions import check_permission
from gitential2.datatypes.permissions import Entity, Action
from ..dependencies import gitential_context, current_user
from ...core.dashboards import list_dashboards, get_dashboard, delete_dashboard, create_dashboard, update_dashboard
from ...datatypes.dashboards import DashboardPublic, DashboardUpdate, DashboardCreate

router = APIRouter(tags=["dashboards"])

logger = get_logger(__name__)


@router.get("/workspaces/{workspace_id}/dashboards", response_model=List[DashboardPublic])
def workspace_dashboards(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.dashboard, Action.read, workspace_id=workspace_id)
    return list_dashboards(g, workspace_id=workspace_id)


@router.get("/workspaces/{workspace_id}/dashboards/{dashboard_id}", response_model=DashboardPublic)
def get_dashboard_(
    workspace_id: int,
    dashboard_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.dashboard, Action.read, workspace_id=workspace_id)
    return get_dashboard(g, workspace_id=workspace_id, dashboard_id=dashboard_id)


@router.post("/workspaces/{workspace_id}/dashboards", response_model=DashboardPublic)
def create_dashboard_(
    dashboard_create: DashboardCreate,
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.dashboard, Action.create, workspace_id=workspace_id)
    return create_dashboard(g, workspace_id, dashboard_create)


@router.put("/workspaces/{workspace_id}/dashboards/{dashboard_id}", response_model=DashboardPublic)
def update_dashboard_(
    dashboard_update: DashboardUpdate,
    workspace_id: int,
    dashboard_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.dashboard, Action.update, workspace_id=workspace_id)
    return update_dashboard(g, workspace_id=workspace_id, dashboard_id=dashboard_id, dashboard_update=dashboard_update)


@router.delete("/workspaces/{workspace_id}/dashboards/{dashboard_id}", response_model=bool)
def delete_dashboard_(
    workspace_id: int,
    dashboard_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.dashboard, Action.delete, workspace_id=workspace_id)
    return delete_dashboard(g, workspace_id=workspace_id, dashboard_id=dashboard_id)
