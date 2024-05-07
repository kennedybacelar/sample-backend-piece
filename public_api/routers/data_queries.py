from fastapi import APIRouter, Depends

from gitential2.datatypes.data_queries import DataQuery, MultiQuery, DQResultOrientation
from gitential2.datatypes.permissions import Entity, Action
from gitential2.core.context import GitentialContext
from gitential2.core.permissions import check_permission

from gitential2.core.data_queries import process_data_query, process_data_queries

# from gitential2.core.subscription import limit_filter_time
# from gitential2.core.workspaces import is_workspace_subs_prof

from ..dependencies import gitential_context, current_user

router = APIRouter(tags=["data-queries"])


@router.post("/workspaces/{workspace_id}/data-query")
def workspace_data_query(
    val: DataQuery,
    workspace_id: int,
    current_user=Depends(current_user),
    orientation: DQResultOrientation = DQResultOrientation.LIST,
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    # if not is_workspace_subs_prof(g, workspace_id):
    #     val = limit_filter_time(workspace_id, val)
    return process_data_query(g, workspace_id, val, orientation=orientation)


@router.post("/workspaces/{workspace_id}/data-queries")
def workspace_data_queries(
    queries: MultiQuery,
    workspace_id: int,
    current_user=Depends(current_user),
    orientation: DQResultOrientation = DQResultOrientation.LIST,
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)

    # ret: dict = {}
    # is_professional = is_workspace_subs_prof(g, workspace_id)
    # for name, val in queries.items():
    #     if not is_professional:
    #         val = limit_filter_time(workspace_id, val)
    #     result = collect_stats_v2(g, workspace_id, val)
    #     ret[name] = result
    # return ret
    return process_data_queries(g, workspace_id, queries, orientation=orientation)
