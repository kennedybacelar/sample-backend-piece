from typing import List

from fastapi import APIRouter, Depends
from structlog import get_logger

from gitential2.datatypes.charts import ChartPublic, ChartCreate, ChartUpdate
from ..dependencies import gitential_context, current_user
from ...core import GitentialContext, check_permission
from ...core.dashboards import list_charts, get_chart, create_chart, update_chart, delete_chart
from ...datatypes.permissions import Entity, Action

router = APIRouter(tags=["charts"])

logger = get_logger(__name__)


@router.get("/workspaces/{workspace_id}/charts", response_model=List[ChartPublic])
def workspace_charts(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.chart, Action.read, workspace_id=workspace_id)
    return list_charts(g, workspace_id)


@router.get("/workspaces/{workspace_id}/charts/{chart_id}", response_model=ChartPublic)
def get_chart_(
    workspace_id: int,
    chart_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.chart, Action.read, workspace_id=workspace_id)
    return get_chart(g, workspace_id, chart_id)


@router.post("/workspaces/{workspace_id}/charts", response_model=ChartPublic)
def create_chart_(
    chart_create: ChartCreate,
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.chart, Action.read, workspace_id=workspace_id)
    return create_chart(g, workspace_id, chart_create)


@router.put("/workspaces/{workspace_id}/charts/{chart_id}", response_model=ChartPublic)
def update_chart_(
    chart_update: ChartUpdate,
    workspace_id: int,
    chart_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.chart, Action.read, workspace_id=workspace_id)
    return update_chart(g, workspace_id, chart_id, chart_update)


@router.delete("/workspaces/{workspace_id}/charts/{chart_id}", response_model=bool)
def delete_chart_(
    workspace_id: int,
    chart_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.chart, Action.read, workspace_id=workspace_id)
    return delete_chart(g, workspace_id, chart_id)
