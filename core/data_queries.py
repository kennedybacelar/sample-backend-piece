import math
from datetime import timedelta
from typing import Dict, List, Tuple, cast, Union, Optional
from ibis.expr.types import TableExpr, ColumnExpr
import pandas as pd
import numpy as np
from structlog import get_logger
from gitential2.datatypes.data_queries import (
    DQColumnAttrName,
    DQColumnExpr,
    DQFnColumnExpr,
    DQFunctionName,
    DQResult,
    DQResultOrientation,
    DQSelectionExpr,
    DQSingleColumnExpr,
    DQSortByExpr,
    DQSourceName,
    MultiQuery,
    DataQuery,
    DQFilterExpr,
    DQType,
    DQDimensionExpr,
    DQ_ITS_SOURCE_NAMES,
)
from gitential2.datatypes.sprints import Sprint
from .context import GitentialContext


logger = get_logger(__name__)


def process_data_queries(
    g: GitentialContext,
    workspace_id: int,
    queries: MultiQuery,
    orientation: DQResultOrientation = DQResultOrientation.LIST,
) -> Dict[str, DQResult]:
    ret = {}
    for name, query in queries.items():
        result = process_data_query(g, workspace_id, query, orientation=orientation)
        ret[name] = result
    return ret


def process_data_query(
    g: GitentialContext,
    workspace_id: int,
    query: DataQuery,
    orientation: DQResultOrientation = DQResultOrientation.LIST,
) -> DQResult:
    result, total = execute_data_query(g, workspace_id, query)
    result_json = _to_jsonable_result(result, orientation=orientation)
    return DQResult(results=result_json, total=total, limit=query.limit, offset=query.offset, orientation=orientation)


def _to_jsonable_result(result: pd.DataFrame, orientation: DQResultOrientation) -> Union[dict, list]:
    if result.empty:
        return {}
    ret = result.replace([np.inf, -np.inf], np.NaN)
    ret = ret.where(pd.notnull(ret), None)
    return _replace_nans(ret.to_dict(orient=orientation.as_literal))


def _replace_nans(obj):
    if isinstance(obj, dict):
        return {k: _replace_nans(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_replace_nans(v) for v in obj]
    elif obj in [np.inf, -np.inf, np.NaN, float("nan")] or (isinstance(obj, float) and math.isnan(obj)):
        return None
    else:
        return obj


def execute_data_query(g: GitentialContext, workspace_id: int, query: DataQuery) -> Tuple[pd.DataFrame, int]:
    query = _simplify_query(g, workspace_id, query)
    _, ibis_query = parse_data_query(g, workspace_id, query)

    total_count = None
    if query.limit is not None and query.offset is not None:
        total_count = ibis_query.count().execute()
        ibis_query = ibis_query.limit(query.limit, offset=query.offset)

    result: pd.DataFrame = ibis_query.execute()
    if total_count is None:
        total_count = len(result.index)
    return result, total_count


def parse_data_query(g: GitentialContext, workspace_id: int, query: DataQuery) -> TableExpr:
    table = g.backend.get_ibis_table(workspace_id, query.source_name.value)
    filters = _prepare_filters(query.filters, table)
    ibis_query = table.filter(filters) if filters else table
    selections = _prepare_selections(query.selections, ibis_query)

    if query.query_type == DQType.select:
        ibis_query = ibis_query.select(selections)
    elif query.query_type == DQType.aggregate:
        dimensions = _prepare_dimensions(query.dimensions, ibis_query)
        ibis_query = ibis_query.group_by(dimensions).aggregate(selections)
    if query.sort_by:
        sort_by_s = _prepare_sort_by_s(query.sort_by, ibis_query)
        ibis_query = ibis_query.sort_by(sort_by_s)

    return table, ibis_query


def _simplify_query(g: GitentialContext, workspace_id: int, query: DataQuery):
    # replace project_id to itsp_id or repo_id
    # replace team_id to dev_id
    _adding_sprint_dimension_info_into_filters(g, workspace_id, query)
    query.filters = [_simplify_filter(g, workspace_id, f, query.source_name) for f in query.filters]
    return query


def _dev_id_column_name(table_name: str) -> Optional[str]:
    dev_id_column_name = {
        "its_issues": "assignee_api_id",
        "its_issue_comments": "author_dev_id",
        "pull_requests": "user_aid",
        "pull_request_comments": "author_aid",
        "deploy_commits": "author_id",
    }

    return dev_id_column_name.get(table_name)


def _getting_date_field_name_by_table(table_name: str) -> Optional[str]:
    table_column_name_to_sprint_filter = {
        "its_issues": "created_at",
        "its_issue_comments": "created_at",
        "pull_requests": "created_at",
        "pull_request_comments": "created_at",
        "pull_request_commits": "created_at",
        "deploy_commits": "deployed_at",
    }

    return table_column_name_to_sprint_filter.get(table_name)


def _get_project_or_team_sprint(
    g: GitentialContext, workspace_id: int, project_or_team: Tuple[str, int]
) -> Optional[Sprint]:

    if project_or_team[0] == "project":
        return g.backend.projects.get_or_error(workspace_id=workspace_id, id_=project_or_team[1]).sprint
    elif project_or_team[0] == "team":
        return g.backend.teams.get_or_error(workspace_id=workspace_id, id_=project_or_team[1]).sprint
    return None


def _adding_sprint_dimension_info_into_filters(g: GitentialContext, workspace_id: int, query: DataQuery):
    project_id, team_id = None, None
    for i, dimension in enumerate(query.dimensions):
        if dimension == "sprint":
            for _filter in query.filters:
                if (
                    _filter.fn == DQFunctionName.EQ
                    and isinstance(_filter.args[0], DQSingleColumnExpr)
                    and _filter.args[0].col == "project_id"
                ):
                    project_id = "project", int(cast(int, _filter.args[1]))
                    break
                if (
                    _filter.fn == DQFunctionName.EQ
                    and isinstance(_filter.args[0], DQSingleColumnExpr)
                    and _filter.args[0].col == "team_id"
                ):
                    team_id = "team", int(cast(int, _filter.args[1]))
                    break
            query.dimensions.pop(i)
            project_or_team = project_id or team_id
            if project_or_team:
                sprint = _get_project_or_team_sprint(g, workspace_id, project_or_team)
                if sprint:
                    start_date = sprint.date
                    final_date = start_date + timedelta(weeks=sprint.weeks)

                    table_name = query.source_name.value
                    column_to_be_used_in_sprint_filter = _getting_date_field_name_by_table(table_name)

                    # Some tables available as data-query sources don't have neither created_at nor any other date wise column
                    # Then, for these tables, the dimension sprint won't take any effect

                    if column_to_be_used_in_sprint_filter:

                        new_filter = DQFnColumnExpr(
                            fn=DQFunctionName.BETWEEN,
                            args=[
                                DQSingleColumnExpr(col=column_to_be_used_in_sprint_filter),
                                start_date.isoformat(),
                                final_date.isoformat(),
                            ],
                        )
                        query.filters.append(new_filter)

                    else:
                        logger.warning("Sprint dimension not allowed for this data source", source_name=table_name)
            break


def _simplify_filter(
    g: GitentialContext, workspace_id: int, f: DQFilterExpr, source_name: DQSourceName
) -> DQFilterExpr:
    if f.args:
        if f.fn == DQFunctionName.EQ and isinstance(f.args[0], DQSingleColumnExpr) and f.args[0].col == "project_id":
            project_id = int(cast(int, f.args[1]))
            if source_name in DQ_ITS_SOURCE_NAMES:
                itsp_ids = g.backend.project_its_projects.get_itsp_ids_for_project(workspace_id, project_id)
                return DQFnColumnExpr(fn=DQFunctionName.IN, args=[DQSingleColumnExpr(col="itsp_id"), itsp_ids])
            else:
                repo_ids = g.backend.project_repositories.get_repo_ids_for_project(workspace_id, project_id)
                return DQFnColumnExpr(fn=DQFunctionName.IN, args=[DQSingleColumnExpr(col="repo_id"), repo_ids])
        elif f.fn == DQFunctionName.EQ and isinstance(f.args[0], DQSingleColumnExpr) and f.args[0].col == "team_id":
            team_id = int(cast(int, f.args[1]))
            dev_ids = g.backend.team_members.get_team_member_author_ids(workspace_id, team_id)
            dev_id_column_name = _dev_id_column_name(source_name)
            return DQFnColumnExpr(fn=DQFunctionName.IN, args=[DQSingleColumnExpr(col=dev_id_column_name), dev_ids])
    return f


def _prepare_filters(filters: List[DQFilterExpr], table: TableExpr) -> list:
    return [_parse_column_expr(filter_, table) for filter_ in filters or []]


def _prepare_selections(selections: List[DQSelectionExpr], table: TableExpr) -> list:
    return [_parse_column_expr(selection, table) for selection in selections]


def _prepare_dimensions(dimensions: List[DQDimensionExpr], table: TableExpr) -> list:
    return [_parse_column_expr(dimension, table) for dimension in dimensions or []]


def _prepare_sort_by_s(sort_by_s: List[DQSortByExpr], table: TableExpr):
    return [_prepare_sort_by(sort_by_expr, table) for sort_by_expr in sort_by_s or []]


# pylint: disable=unused-argument
def _prepare_sort_by(sort_by_expr: DQSortByExpr, table: TableExpr) -> Tuple[str, bool]:
    return _parse_single_column_expr(sort_by_expr, table), not sort_by_expr.desc


def _parse_column_expr(column_expr: DQColumnExpr, table: TableExpr) -> ColumnExpr:
    if isinstance(column_expr, DQSingleColumnExpr):
        return _parse_single_column_expr(column_expr, table)
    elif isinstance(column_expr, DQFnColumnExpr):
        return _parse_fn_column_expr(column_expr, table)
    else:
        # static value, just return with it
        return column_expr


def _parse_single_column_expr(column_expr: DQSingleColumnExpr, table: TableExpr):
    ret = table[column_expr.col]
    if column_expr.attr:
        ret = _parse_attr(ret, column_expr.attr)
    if column_expr.as_:
        ret = ret.name(column_expr.as_)
    return ret


def _parse_fn_column_expr(column_expr: DQFnColumnExpr, table: TableExpr):
    fn_name = column_expr.fn
    parsed_args = [_parse_column_expr(arg, table) for arg in column_expr.args]

    fn_definitions = {
        # aggregations
        DQFunctionName.MEAN: lambda pa: pa[0].mean(),
        DQFunctionName.SUM: lambda pa: pa[0].sum(),
        DQFunctionName.COUNT: lambda pa: pa[0].count() if pa else table.count(),
        # filtering
        DQFunctionName.EQ: lambda pa: pa[0] == pa[1],
        DQFunctionName.NEQ: lambda pa: pa[0] != pa[1],
        DQFunctionName.LT: lambda pa: pa[0] < pa[1],
        DQFunctionName.LTE: lambda pa: pa[0] <= pa[1],
        DQFunctionName.GT: lambda pa: pa[0] > pa[1],
        DQFunctionName.GTE: lambda pa: pa[0] >= pa[1],
        DQFunctionName.IN: lambda pa: pa[0].isin(pa[1]),
        DQFunctionName.NIN: lambda pa: pa[0].notin(pa[1]),
        DQFunctionName.BETWEEN: lambda pa: pa[0].between(pa[1], pa[2]),
        # mathematical operations
        DQFunctionName.MUL: lambda pa: pa[0] * pa[1],
        DQFunctionName.DIV: lambda pa: pa[0] / pa[1],
        DQFunctionName.ADD: lambda pa: pa[0] + pa[1],
        DQFunctionName.SUB: lambda pa: pa[0] - pa[1],
        # null checks
        DQFunctionName.ISNULL: lambda pa: pa[0].isnull(),
        DQFunctionName.NOTNULL: lambda pa: pa[0].notnull(),
    }
    ret = fn_definitions[fn_name](parsed_args)

    if column_expr.attr:
        ret = _parse_attr(ret, column_expr.attr)

    if column_expr.as_:
        ret = ret.name(column_expr.as_)
    return ret


def _parse_attr(expr, attr_name: DQColumnAttrName):
    predefined_attrs = {
        DQColumnAttrName.ROUND_TO_DAY: lambda col: col.date(),
        DQColumnAttrName.ROUND_TO_WEEK: lambda col: col.date().truncate("W"),
        DQColumnAttrName.ROUND_TO_MONTH: lambda col: col.date().truncate("M"),
        DQColumnAttrName.ROUND_TO_HOUR: lambda col: col.truncate("H"),
        DQColumnAttrName.TO_DAY_OF_WEEK: lambda col: col.date().day_of_week.index(),
        DQColumnAttrName.TO_HOUR_OF_DAY: lambda col: col.hour(),
        DQColumnAttrName.EPOCH_SECONDS: lambda col: col.epoch_seconds(),
    }
    if attr_name in predefined_attrs:
        return predefined_attrs[attr_name](expr)
    else:
        attr = getattr(expr, attr_name.value)
        if callable(attr):
            return attr()
        else:
            return attr
