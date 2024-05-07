# pylint: disable=too-complex,too-many-branches
import math
from typing import Generator, List, Any, Dict, Optional, cast, Tuple
from datetime import datetime, date, timedelta, timezone

from functools import partial
from structlog import get_logger
from pydantic import BaseModel
import pandas as pd
import numpy as np
import ibis

from gitential2.utils import common_elements_if_not_none

from gitential2.datatypes.stats import (
    AggregationFunction,
    IbisTables,
    MetricDef,
    Query,
    MetricName,
    DimensionName,
    FilterName,
    QueryType,
    RELATIVE_DATE_DIMENSIONS,
    TableDef,
    TableName,
    DATE_DIMENSIONS,
)
from gitential2.datatypes.pull_requests import PullRequestState
from gitential2.datatypes.sprints import Sprint

from .context import GitentialContext
from .authors import list_active_author_ids

from ..exceptions import NotFoundException

logger = get_logger(__name__)


class QueryResult(BaseModel):
    query: Query
    values: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True


def _prepare_dimensions(
    dimensions, table_def: TableDef, ibis_tables, ibis_table, g: GitentialContext, workspace_id: int, query: Query
):
    ret = []
    # Try this out first
    # if (DimensionName.name in dimensions or DimensionName.email in dimensions) and DimensionName.aid not in dimensions:
    #     dimensions.append(DimensionName.aid)
    for dimension in dimensions:
        res = _prepare_dimension(dimension, table_def, ibis_tables, ibis_table, g, workspace_id, query)
        if res is not None:
            ret.append(res)
    return ret


# pylint: disable=too-many-return-statements
def _prepare_dimension(
    dimension: DimensionName,
    table_def: TableDef,
    ibis_tables: IbisTables,
    ibis_table,
    g: GitentialContext,
    workspace_id: int,
    query: Query,
):  # pylint: disable=too-complex
    if dimension in DATE_DIMENSIONS:
        if TableName.pull_requests in table_def:
            date_field_name = "created_at"
            # ibis_table = ibis_tables.pull_requests
        elif TableName.pull_request_comments in table_def:
            date_field_name = "published_at"
        elif TableName.deploy_commits in table_def:
            date_field_name = "deployed_at"
        else:
            date_field_name = "date"
            # ibis_table = ibis_tables.commits

        if dimension == DimensionName.day:
            return (ibis_table[date_field_name].date().epoch_seconds() * 1000).name("date")
        elif dimension == DimensionName.week:
            return (ibis_table[date_field_name].date().truncate("W").epoch_seconds() * 1000).name("date")
        elif dimension == DimensionName.month:
            return (ibis_table[date_field_name].date().truncate("M").epoch_seconds() * 1000).name("date")
        elif dimension == DimensionName.hour:
            return (ibis_table[date_field_name].truncate("H").epoch_seconds() * 1000).name("date")

    elif dimension in RELATIVE_DATE_DIMENSIONS:
        if TableName.pull_requests in table_def:
            date_field_name = "created_at"
        elif TableName.pull_request_comments in table_def:
            date_field_name = "published_at"
        elif TableName.deploy_commits in table_def:
            date_field_name = "deployed_at"
        else:
            date_field_name = "date"

        if dimension == DimensionName.day_of_week:
            return (ibis_table[date_field_name].date().day_of_week.index()).name("day_of_week")
        elif dimension == DimensionName.hour_of_day:
            return (ibis_table[date_field_name].hour()).name("hour_of_day")
        elif dimension == DimensionName.sprint:
            sprint = _get_sprint_info(g, workspace_id, query.extra)
            if not sprint:
                raise NotFoundException("No Sprint set for project or team")

            first_sprint_date, sprints_timestamps_to_replace = _prepare_sprint_x_ref_aggregation(query, sprint)
            changing_day_filter_lower_value(query, first_sprint_date)

            datetime_column_to_timestamp = ibis_table[date_field_name].date()

            # Replacing the dates for the dates whose sprint they belong
            if sprints_timestamps_to_replace:
                datetime_column_to_timestamp = datetime_column_to_timestamp.substitute(sprints_timestamps_to_replace)

            # Epoch seconds values have to be multiplied by 1000 because java script (frontend) has a different internal scale
            datetime_column_to_timestamp = (datetime_column_to_timestamp.epoch_seconds() * 1000).name("date")

            return datetime_column_to_timestamp

    elif dimension == DimensionName.pr_state:
        return ibis_tables.pull_requests.state.name("pr_state")

    elif dimension == DimensionName.language:
        return ibis_table["lang"].name("language")
    elif dimension == DimensionName.name:
        return ibis_table["name"].name("name")
    elif dimension == DimensionName.email:
        return ibis_table["email"].name("email")
    elif dimension == DimensionName.repo_id:
        return ibis_table["repo_id"].name("repo_id")
    elif dimension == DimensionName.aid and TableName.pull_requests not in table_def:
        return ibis_table["aid"].name("aid")
    elif dimension == DimensionName.developer_id:
        if (TableName.commits in table_def) or (TableName.patches in table_def):
            return ibis_table["aid"].name("developer_id")
        elif TableName.pull_requests in table_def:
            return ibis_table["user_aid"].name("developer_id")
        elif TableName.pull_request_comments in table_def:
            return ibis_table["author_aid"].name("developer_id")
    elif dimension == DimensionName.istest:
        return ibis_table["is_test"].name("istest")
    elif dimension == DimensionName.environment:
        return ibis_table["environment"].name("environment")
    return None


def _prepare_metrics(metrics, table_def: TableDef, ibis_tables, ibis_table, q: Query):
    ret = []
    for metric in metrics:
        if isinstance(metric, MetricDef):
            res = _prepare_generic_metric(metric, ibis_table)
        elif TableName.commits in table_def:
            res = _prepare_commits_metric(metric, ibis_table, q)
        elif TableName.pull_requests in table_def:
            res = _prepare_prs_metric(metric, ibis_tables)
        elif TableName.patches in table_def:
            res = _prepare_patch_metric(metric, ibis_table)
        elif TableName.deploy_commits in table_def:
            res = _prepare_deploy_commits_metric(metric, ibis_table)

        if res is not None:
            ret.append(res)
    return ret


def _prepare_generic_metric(metric: MetricDef, ibis_table):
    if metric.field == "ploc":
        base_field = ibis_table["loc_i"].nullif(0) - ibis_table["uploc"].nullif(0)
    else:
        base_field = ibis_table[metric.field]

    if metric.aggregation == AggregationFunction.MEAN:
        field = base_field.mean()
    elif metric.aggregation == AggregationFunction.COUNT:
        field = base_field.count()
    elif metric.aggregation == AggregationFunction.SUM:
        field = base_field.sum()

    if metric.name:
        return field.name(metric.name)
    else:
        return field.name(f"{metric.aggregation}_{metric.field}")


def _prepare_deploy_commits_metric(metric: MetricName, ibis_table):

    deploy_commits = ibis_table

    count_deploys = deploy_commits.count().name("id")

    deploy_commit_metrics = {
        MetricName.count_deploys: count_deploys,
    }

    if metric not in deploy_commit_metrics:
        raise ValueError(f"missing metric {metric}")
    return deploy_commit_metrics.get(metric)


def _prepare_commits_metric(metric: MetricName, ibis_table, q: Query):
    # t = ibis_tables
    commits = ibis_table

    # commit metrics
    count_commits = commits.count().name("count_commits")

    loc_effort = commits.loc_effort_c.sum().name("sum_loc_effort")
    avg_loc_effort = commits.loc_effort_c.mean().name("avg_loc_effort")

    sum_hours = commits.hours.sum().name("sum_hours")
    avg_hours = commits.hours.mean().name("avg_hours")
    sum_ploc = (commits.loc_i_c.sum() - commits.uploc_c.sum()).name("sum_ploc")
    sum_uploc = commits.uploc_c.sum().name("sum_uploc")
    efficiency = (sum_ploc / (commits.loc_i_c.sum()).nullif(0) * 100).name("efficiency")
    churn_calc = (100 - (sum_ploc / (commits.loc_i_c.sum()).nullif(0) * 100)).name("churn_calc")
    nunique_contributors = commits.aid.nunique().name("nunique_contributors")
    comp_sum = (commits.comp_i_c.sum() - commits.comp_d_c.sum()).name("comp_sum")
    utilization = (sum_hours / q.utilization_working_hours() * 100).name("utilization")
    avg_velocity = commits.velocity.mean().name("avg_velocity")
    loc_sum = commits.loc_i_c.sum().name("loc_sum")

    commit_metrics = {
        MetricName.count_commits: count_commits,
        MetricName.sum_loc_effort: loc_effort,
        MetricName.avg_loc_effort: avg_loc_effort,
        MetricName.sum_hours: sum_hours,
        MetricName.avg_hours: avg_hours,
        MetricName.sum_ploc: sum_ploc,
        MetricName.sum_uploc: sum_uploc,
        MetricName.efficiency: efficiency,
        MetricName.nunique_contributors: nunique_contributors,
        MetricName.comp_sum: comp_sum,
        MetricName.utilization: utilization,
        MetricName.avg_velocity: avg_velocity,
        MetricName.loc_sum: loc_sum,
        MetricName.churn_calc: churn_calc,
    }
    if metric not in commit_metrics:
        raise ValueError(f"missing metric {metric}")
    return commit_metrics.get(metric)


def _prepare_patch_metric(metric: MetricName, ibis_table):
    # pylint: disable=singleton-comparison, compare-to-zero
    if metric == MetricName.sum_loc_test:
        return ibis_table.loc_i.sum(where=ibis_table.is_test == True).name("sum_loc_test")
    elif metric == MetricName.sum_loc_impl:
        return ibis_table.loc_i.sum(where=ibis_table.is_test == False).name("sum_loc_impl")
    elif metric == MetricName.loc_effort_p:
        return ibis_table.loc_effort_p.sum().name("loc_effort_p")
    else:
        return None


def _prepare_prs_metric(metric: MetricName, ibis_tables: IbisTables):

    prs = ibis_tables.pull_requests

    sum_pr_count = prs.count().name("sum_pr_count")
    sum_pr_open = prs.title.count(where=prs.state == PullRequestState.open.name).name("sum_pr_open")
    sum_pr_closed = prs.title.count(where=prs.state == PullRequestState.closed.name).name("sum_pr_closed")
    sum_pr_merged = prs.title.count(where=prs.state == PullRequestState.merged.name).name("sum_pr_merged")
    avg_pr_commit_count = prs["commits"].mean().name("avg_pr_commit_count")
    avg_pr_code_volume = prs["additions"].mean().name("avg_pr_code_volume")
    avg_pr_cycle_time = (
        ((prs["merged_at"].epoch_seconds() - prs["first_commit_authored_at"].epoch_seconds()).abs() / 3600)
        .mean()
        .name("avg_pr_cycle_time")
    )
    avg_review_time = (
        ((prs["merged_at"].epoch_seconds() - prs["first_reaction_at"].epoch_seconds()).abs() / 3600)
        .mean()
        .name("avg_review_time")
    )
    avg_pickup_time = (
        ((prs["first_reaction_at"].epoch_seconds() - prs["created_at"].epoch_seconds()).abs() / 3600)
        .mean()
        .name("avg_pickup_time")
    )

    avg_development_time = (
        ((prs["created_at"].epoch_seconds() - prs["first_commit_authored_at"].epoch_seconds()).abs() / 3600)
        .mean()
        .name("avg_development_time")
    )
    sum_review_comment_count = prs["commits"].sum().name("sum_review_comment_count")
    avg_pr_review_comment_count = prs["commits"].mean().name("avg_pr_review_comment_count")

    pr_merge_ratio = (sum_pr_merged / sum_pr_count.nullif(0) * 100).name("pr_merge_ratio")

    pr_metrics = {
        MetricName.sum_pr_count: sum_pr_count,
        MetricName.avg_pr_commit_count: avg_pr_commit_count,
        MetricName.avg_pr_code_volume: avg_pr_code_volume,
        MetricName.avg_pr_cycle_time: avg_pr_cycle_time,
        MetricName.avg_review_time: avg_review_time,
        MetricName.avg_pickup_time: avg_pickup_time,
        MetricName.avg_development_time: avg_development_time,
        MetricName.sum_review_comment_count: sum_review_comment_count,
        MetricName.avg_pr_review_comment_count: avg_pr_review_comment_count,
        MetricName.sum_pr_open: sum_pr_open,
        MetricName.sum_pr_closed: sum_pr_closed,
        MetricName.sum_pr_merged: sum_pr_merged,
        MetricName.pr_merge_ratio: pr_merge_ratio,
    }

    return pr_metrics.get(metric)


def _get_sprint_info(g: GitentialContext, workspace_id: int, query_raw_filters: Optional[dict]) -> Optional[Sprint]:
    sprint = None
    if query_raw_filters:
        team_id = query_raw_filters.get(FilterName.team_id)
        project_id = query_raw_filters.get(FilterName.project_id)
        if team_id:
            sprint = g.backend.teams.get_or_error(workspace_id, team_id).sprint
        elif project_id:
            sprint = g.backend.projects.get_or_error(workspace_id, project_id).sprint
        else:
            raise NotFoundException("Missing project_id or team_id in the query's filters")
    return sprint


def _calculate_first_sprint_date(sprint: Sprint, from_date_sprint_range: date) -> date:
    total_delta = (sprint.date - from_date_sprint_range).total_seconds()

    # count_of_sprints = Count of sprints between date_min and sprint start date (info fetched from project table)
    # gap_in_seconds_from_date_min_to_first_sprint = The offset between the date_min and the first sprint aggregation
    _count_of_sprints, gap_in_seconds_from_date_min_to_first_sprint = divmod(
        total_delta, timedelta(weeks=sprint.weeks).total_seconds()
    )
    first_sprint_initial_date = from_date_sprint_range + timedelta(
        days=timedelta(seconds=gap_in_seconds_from_date_min_to_first_sprint).days
    )
    return first_sprint_initial_date


def changing_day_filter_lower_value(query: Query, first_sprint_date: date):
    """
    Changing the lower bond of day filter to the first sprint date.
    That way there will be no records that belong to sprints started in a date prior to the date range define in the filter
    """
    if query.filters.get(FilterName.day):
        query.filters[FilterName.day][0] = first_sprint_date


def _prepare_sprint_x_ref_aggregation(query: Query, sprint: Sprint) -> Tuple[date, dict]:
    """
    Return a dictionary with all the dates that have to be replaced by the sprint date which they belong to
    Also return the date of the first sprint in a given interval (determined by the filter day in the query)
    """

    # from_date_sprint_range: The minimum value of the sprint range in case the key 'day' is not passed in the query filter - default has been set to 2000-01-01
    # to_date_sprint_range: in case a upper date limit is not passed in the day filter, today() is considered as the default value
    from_date_sprint_range = (
        query.filters[FilterName.day][0].date() if query.filters.get(FilterName.day) else date(2000, 1, 1)
    )
    to_date_sprint_range = (
        query.filters[FilterName.day][1].date() if query.filters.get(FilterName.day) else datetime.today().date()
    )

    # The effective date of the first sprint given the interval
    first_sprint_date = _calculate_first_sprint_date(sprint, from_date_sprint_range)
    all_sprint_timestamps = list(
        ts
        for ts in _calculate_timestamps_between(
            date_dimension=DimensionName.sprint,
            from_date=first_sprint_date,
            to_date=to_date_sprint_range,
            sprint_lenght_in_weeks=sprint.weeks,
        )
    )

    dict_all_sprint_timestamps_to_replace = {}

    for sprint_timestamp in all_sprint_timestamps:
        for idx in range(timedelta(weeks=sprint.weeks).days):
            if idx:
                day_belonged_to_sprint = sprint_timestamp + timedelta(days=idx)
                dict_all_sprint_timestamps_to_replace[day_belonged_to_sprint] = sprint_timestamp

    return first_sprint_date, dict_all_sprint_timestamps_to_replace


def _get_author_ids_from_emails(g: GitentialContext, workspace_id: int, emails: List[str]):
    ret = []
    emails_set = set(emails)
    for author in g.backend.authors.all(workspace_id):
        if author.all_emails.intersection(emails_set):
            ret.append(author.id)
    return ret


def _prepare_filters(  # pylint: disable=too-complex,unused-argument
    g: GitentialContext,
    workspace_id: int,
    filters: Dict[FilterName, Any],
    table_def: TableDef,
    ibis_table,
) -> list:
    filters_dict = filters

    _ibis_filters: dict = {
        TableName.commits: {
            FilterName.repo_ids: lambda t: t.repo_id.isin,
            FilterName.developer_ids: lambda t: t.aid.isin,
            FilterName.day: lambda t: t.date.between,
            FilterName.is_merge: lambda t: t.is_merge.__eq__,
            FilterName.is_bugfix: lambda t: t.is_bugfix.__eq__,
            FilterName.is_pr_open: lambda t: t.is_pr_open.__eq__,
            FilterName.is_pr_closed: lambda t: t.is_pr_closed.__eq__,
            FilterName.is_pr_exists: lambda t: t.is_pr_exists.__eq__,
            # "keyword": t.message.lower().re_search,
            # "outlier": t.outlier.__eq__,
            # "commit_msg": t.message.lower().re_search,
        },
        TableName.pull_requests: {
            FilterName.repo_ids: lambda t: t.repo_id.isin,
            FilterName.day: lambda t: t.created_at.between,
            FilterName.is_bugfix: lambda t: t.is_bugfix.__eq__,
            FilterName.developer_ids: lambda t: t.user_aid.isin,
        },
        TableName.patches: {
            FilterName.repo_ids: lambda t: t.repo_id.isin,
            FilterName.developer_ids: lambda t: t.aid.isin,
            FilterName.day: lambda t: t.date.between,
            FilterName.is_merge: lambda t: t.is_merge.__eq__,
            FilterName.is_collaboration: lambda t: t.is_collaboration.__eq__,
            FilterName.is_new_code: lambda t: t.is_new_code.__eq__,
            FilterName.is_test: lambda t: t.is_test.__eq__,
            FilterName.is_bugfix: lambda t: t.is_bugfix.__eq__,
        },
        TableName.pull_request_comments: {
            FilterName.repo_ids: lambda t: t.repo_id.isin,
            FilterName.developer_ids: lambda t: t.author_aid.isin,
            FilterName.day: lambda t: t.published_at.between,
        },
        TableName.deploy_commits: {
            FilterName.repo_ids: lambda t: t.repo_id.isin,
            FilterName.day: lambda t: t.deployed_at.between,
        },
    }

    ret = []
    for filter_key, values in filters_dict.items():
        if filter_key in _ibis_filters.get(table_def[0], {}):
            filter_ = _ibis_filters[table_def[0]][filter_key](ibis_table)
            if filter_.__name__ == "isin":
                ret.append(filter_(values))
            elif isinstance(values, list):
                ret.append(filter_(*values))
            else:
                ret.append(filter_(values))
        else:
            logger.warning("FILTER_KEY_MISSING", filter_key=filter_key, table_def=table_def)

    return ret


def _prepare_sort_by(query: Query):
    if (
        not set({DimensionName.day, DimensionName.week, DimensionName.month, DimensionName}).isdisjoint(
            set(query.dimensions or [])
        )
        and not query.sort_by
    ):
        logger.debug("adding date sort_by", dimensions=query.dimensions, sort_by=query.sort_by)
        return ["date"]
    else:
        return query.sort_by


class IbisQuery:
    def __init__(self, g: GitentialContext, workspace_id: int, query: Query):
        self.g = g
        self.workspace_id = workspace_id
        self.query = query

    def execute(self) -> QueryResult:
        logger.debug("Executing query", query=self.query, workspace_id=self.workspace_id)
        ibis_tables = self.g.backend.get_ibis_tables(self.workspace_id)
        ibis_table = ibis_tables.get_table(self.query.table_def)
        ibis_metrics = _prepare_metrics(self.query.metrics, self.query.table_def, ibis_tables, ibis_table, self.query)
        ibis_dimensions = (
            _prepare_dimensions(
                self.query.dimensions,
                self.query.table_def,
                ibis_tables,
                ibis_table,
                self.g,
                self.workspace_id,
                self.query,
            )
            if self.query.dimensions
            else None
        )

        # ibis_dimensions = None
        ibis_filters = _prepare_filters(self.g, self.workspace_id, self.query.filters, self.query.table_def, ibis_table)

        if ibis_metrics:
            if self.query.type == QueryType.aggregate:
                # ibis_table.aggregate(ibis_metrics, by=query.dimensions).filter(query.filters)
                ibis_query = ibis_table.aggregate(metrics=ibis_metrics, by=ibis_dimensions).filter(ibis_filters)
            else:
                ibis_query = ibis_table.filter(ibis_filters).select(ibis_metrics)

            compiled = ibis.postgres.compile(ibis_query)
            logger.debug("**IBIS QUERY**", compiled_query=str(compiled), query=ibis_query)

            result = ibis_tables.conn.execute(ibis_query)
        else:
            result = pd.DataFrame()

        result = _sort_dataframe(result, query=self.query)
        return QueryResult(query=self.query, values=result)


def _sort_dataframe(result: pd.DataFrame, query: Query) -> pd.DataFrame:
    sort_by = _prepare_sort_by(query)
    if sort_by and not result.empty:
        logger.debug("SORTING", columns=result.columns, sort_by=sort_by)
        if isinstance(sort_by[0], list):
            by, ascending = map(list, zip(*sort_by))
            result.sort_values(by=cast(List[str], by), ascending=cast(List[bool], ascending), inplace=True)
        else:
            result.sort_values(by=sort_by, inplace=True)

    logger.debug("RESULT AFTER SORTING", result=result)
    return result


def _to_jsonable_result(result: QueryResult) -> dict:
    if result.values.empty:
        return {}
    ret = result.values.replace([np.inf, -np.inf], np.NaN)
    ret = ret.where(pd.notnull(ret), None)
    logger.debug("INDEX", index=ret.index)
    return replace_nans(ret.to_dict(orient="list"))


def replace_nans(obj):
    if isinstance(obj, dict):
        return {k: replace_nans(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nans(v) for v in obj]
    elif obj in [np.inf, -np.inf, np.NaN, float("nan")] or (isinstance(obj, float) and math.isnan(obj)):
        return None
    else:
        return obj


def _add_missing_timestamp_to_result(result: QueryResult):
    if result.values.empty:
        return result
    date_dimension = _get_date_dimension(result.query)
    day_filter = result.query.filters.get(FilterName.day)
    if not date_dimension or not day_filter:
        return result

    from_date = (
        day_filter[0].date()
        if isinstance(day_filter[0], datetime)
        else datetime.strptime(day_filter[0], "%Y-%m-%d").date()
    )
    to_date = (
        day_filter[1].date()
        if isinstance(day_filter[1], datetime)
        else datetime.strptime(day_filter[1], "%Y-%m-%d").date()
    )

    all_timestamps = [
        int(ts.timestamp()) * 1000 for ts in _calculate_timestamps_between(date_dimension, from_date, to_date)
    ]
    date_col = ""
    if "datetime" in result.values.columns:
        date_col = "datetime"
    elif "date" in result.values.columns:
        date_col = "date"
    # print(result.values.columns)
    for ts in all_timestamps:
        if True not in (result.values[date_col] == ts).values:
            if True in (result.values[date_col] > ts).values:
                row = _create_empty_row(ts, date_col, result.values.columns, 0)
            else:
                row = _create_empty_row(ts, date_col, result.values.columns, None)
            result.values = result.values.append(row, ignore_index=True)
    result.values = _sort_dataframe(result.values, query=result.query)
    return result


def _create_empty_row(ts: int, date_column: str, column_list, default_field_value: Optional[int]) -> dict:
    ret: dict = {}
    default_values: dict = {
        "language": "Others",
        "name": "",
        "email": "",
        "developer_id": None,
    }
    for col in column_list:
        if col == date_column:
            ret[col] = ts
        elif col in default_values:
            ret[col] = default_values[col]
        else:
            ret[col] = default_field_value
    return ret


def _get_date_dimension(query: Query) -> Optional[DimensionName]:
    if query.dimensions:
        for d in query.dimensions:
            if d in DATE_DIMENSIONS:
                return d
    return None


def _start_of_the_day(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)


def _end_of_the_day(day: date) -> datetime:
    return datetime.combine(day, datetime.max.time(), tzinfo=timezone.utc)


def _next_hour(d: datetime) -> datetime:
    return d + timedelta(hours=1)


def _next_day(d: datetime) -> datetime:
    return d + timedelta(days=1)


def _next_week(d: datetime) -> datetime:
    return d + timedelta(days=7)


def _next_month(d: datetime) -> datetime:
    if d.month == 12:
        return d.replace(year=d.year + 1, month=1)
    else:
        return d.replace(month=d.month + 1)


def _next_sprint(d: datetime, sprint_lenght_in_weeks: int) -> datetime:
    return d + timedelta(weeks=sprint_lenght_in_weeks)


def _calculate_timestamps_between(
    date_dimension: DimensionName,
    from_date: date,
    to_date: date,
    sprint_lenght_in_weeks: int = 1,
) -> Generator[datetime, None, None]:
    from_ = _start_of_the_day(from_date)
    to_ = _end_of_the_day(to_date)
    if date_dimension == DimensionName.hour:
        start_ts = from_
        get_next = _next_hour
    elif date_dimension == DimensionName.day:
        start_ts = from_
        get_next = _next_day
    elif date_dimension == DimensionName.week:
        start_ts = from_ - timedelta(days=from_date.weekday())
        get_next = _next_week
    elif date_dimension == DimensionName.month:
        start_ts = from_ - timedelta(days=from_date.day - 1)
        get_next = _next_month
    elif date_dimension == DimensionName.sprint:
        start_ts = from_
        get_next = partial(_next_sprint, sprint_lenght_in_weeks=sprint_lenght_in_weeks)
    current_ts = start_ts
    while current_ts < to_:
        yield current_ts
        current_ts = get_next(current_ts)


def collect_stats_v2_raw(g: GitentialContext, workspace_id: int, query: Query) -> QueryResult:
    prepared_query = prepare_query(g, workspace_id, query)
    result = _add_missing_timestamp_to_result(IbisQuery(g, workspace_id, prepared_query).execute())
    return result


def collect_stats_v2(g: GitentialContext, workspace_id: int, query: Query):
    result = collect_stats_v2_raw(g, workspace_id, query)
    return _to_jsonable_result(result)


def prepare_query(g: GitentialContext, workspace_id: int, query: Query) -> Query:
    return _add_developer_ids_to_filter(
        g,
        workspace_id,
        Query(
            metrics=query.metrics,
            dimensions=query.dimensions,
            filters=_simplify_filters(g, workspace_id, query.filters),
            sort_by=query.sort_by,
            type=query.type,
            table=query.table,
            extra=query.filters,
        ),
    )


def _add_developer_ids_to_filter(g: GitentialContext, workspace_id: int, query: Query) -> Query:
    ret = query.copy()
    if query.dimensions and DimensionName.developer_id in query.dimensions:
        ret.filters[FilterName.developer_ids] = common_elements_if_not_none(
            ret.filters.get(FilterName.developer_ids), list_active_author_ids(g, workspace_id)
        )
        print(ret)
    return ret


def _simplify_filters(g: GitentialContext, workspace_id: int, filters: Dict[FilterName, Any]) -> Dict[FilterName, Any]:
    ret: Dict[FilterName, Any] = {}
    for filter_name, filter_value in filters.items():

        # simplify to developer_ids

        if filter_name == FilterName.team_id:
            team_id = int(filter_value)
            team_member_developer_ids = g.backend.team_members.get_team_member_author_ids(
                workspace_id=workspace_id, team_id=team_id
            )
            ret[FilterName.developer_ids] = common_elements_if_not_none(
                ret.get(FilterName.developer_ids), team_member_developer_ids
            )
        elif filter_name == FilterName.emails:
            author_ids = _get_author_ids_from_emails(g, workspace_id, filter_value)
            ret[FilterName.developer_ids] = common_elements_if_not_none(ret.get(FilterName.developer_ids), author_ids)
        elif filter_name in [FilterName.author_ids, FilterName.developer_ids]:
            ret[FilterName.developer_ids] = common_elements_if_not_none(ret.get(FilterName.developer_ids), filter_value)

        elif filter_name == FilterName.active:
            author_ids = list_active_author_ids(g, workspace_id)
            ret[FilterName.developer_ids] = common_elements_if_not_none(ret.get(FilterName.developer_ids), author_ids)

        # simplify to repo_ids

        elif filter_name == FilterName.project_id:
            project_id = filter_value
            repo_ids = g.backend.project_repositories.get_repo_ids_for_project(
                workspace_id=workspace_id, project_id=project_id
            )
            ret[FilterName.repo_ids] = common_elements_if_not_none(ret.get(FilterName.repo_ids), repo_ids)
        elif filter_name == FilterName.repo_ids:
            ret[FilterName.repo_ids] = common_elements_if_not_none(ret.get(FilterName.repo_ids), filter_value)

        # skip account_id, not needed anymore
        elif filter_name == FilterName.account_id:
            continue

        # ismerge vs is_merge LOL
        elif filter_name == FilterName.ismerge:
            ret[FilterName.is_merge] = filter_value

        elif filter_name == FilterName.day:
            start, end = filter_value
            ret[FilterName.day] = [_as_timestamp(start), _as_timestamp(end, end_of_day=True)]

        # Other filters kept as is
        else:
            ret[filter_name] = filter_value

    return ret


def _as_timestamp(date_str, end_of_day=False):
    d = date.fromisoformat(date_str)
    if not end_of_day:
        return datetime(d.year, d.month, d.day)
    else:
        return datetime(d.year, d.month, d.day, 23, 59, 59)
