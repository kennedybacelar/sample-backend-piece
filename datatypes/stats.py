from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum
from pydantic import BaseModel  # , validator


class MetricName(str, Enum):
    # Patch metrics
    sum_loc_test = "sum_loc_test"
    sum_loc_impl = "sum_loc_impl"
    loc_effort_p = "loc_effort_p"

    # Commit metrics
    count_commits = "count_commits"
    sum_loc_effort = "sum_loc_effort"
    avg_loc_effort = "avg_loc_effort"
    sum_hours = "sum_hours"
    sum_ploc = "sum_ploc"
    sum_uploc = "sum_uploc"
    efficiency = "efficiency"
    utilization = "utilization"
    nunique_contributors = "nunique_contributors"
    comp_sum = "comp_sum"
    avg_velocity = "avg_velocity"
    loc_sum = "loc_sum"
    avg_hours = "avg_hours"
    churn_calc = "churn_calc"

    # PR metrics
    avg_pr_commit_count = "avg_pr_commit_count"
    avg_pr_code_volume = "avg_pr_code_volume"
    avg_review_time = "avg_review_time"
    avg_pickup_time = "avg_pickup_time"
    avg_development_time = "avg_development_time"
    pr_merge_ratio = "pr_merge_ratio"
    sum_pr_closed = "sum_pr_closed"
    sum_pr_merged = "sum_pr_merged"
    sum_pr_open = "sum_pr_open"
    sum_review_comment_count = "sum_review_comment_count"
    avg_pr_review_comment_count = "avg_pr_review_comment_count"
    sum_pr_count = "sum_pr_count"
    avg_pr_cycle_time = "avg_pr_cycle_time"

    # Deploy_commits metrics
    count_deploys = "count_deploys"


PR_METRICS = [
    MetricName.avg_pr_commit_count,
    MetricName.avg_pr_code_volume,
    MetricName.avg_review_time,
    MetricName.avg_pickup_time,
    MetricName.avg_development_time,
    MetricName.pr_merge_ratio,
    MetricName.sum_pr_closed,
    MetricName.sum_pr_merged,
    MetricName.sum_pr_open,
    MetricName.sum_review_comment_count,
    MetricName.avg_pr_review_comment_count,
    MetricName.sum_pr_count,
    MetricName.avg_pr_cycle_time,
]

COMMIT_METRICS = [
    MetricName.count_commits,
    MetricName.sum_loc_effort,
    MetricName.avg_loc_effort,
    MetricName.sum_hours,
    MetricName.sum_ploc,
    MetricName.sum_uploc,
    MetricName.efficiency,
    MetricName.utilization,
    MetricName.nunique_contributors,
    MetricName.comp_sum,
    MetricName.avg_velocity,
    MetricName.loc_sum,
    MetricName.avg_hours,
    MetricName.churn_calc,
]

PATCH_METRICS = [
    MetricName.sum_loc_test,
    MetricName.sum_loc_impl,
    MetricName.loc_effort_p,
]

DEPLOY_METRICS = [
    MetricName.count_deploys,
]


class FilterName(str, Enum):
    repo_ids = "repo_ids"

    # author ids and developer ids are the same, we have both for backward compatibility
    author_ids = "author_ids"
    developer_ids = "developer_ids"

    emails = "emails"
    name = "name"
    day = "day"
    ismerge = "ismerge"
    is_merge = "is_merge"
    active = "active"
    keyword = "keyword"
    outlier = "outlier"
    commit_msg = "commit_msg"
    account_id = "account_id"
    project_id = "project_id"
    team_id = "team_id"
    is_bugfix = "is_bugfix"
    is_collaboration = "is_collaboration"
    is_new_code = "is_new_code"
    is_pr_exists = "is_pr_exists"
    is_pr_open = "is_pr_open"
    is_pr_closed = "is_pr_closed"
    is_test = "is_test"


class DimensionName(str, Enum):
    repo_id = "repo_id"

    # author/developer dimensions
    aid = "aid"
    name = "name"
    email = "email"
    developer_id = "developer_id"

    # date dimensions
    day = "day"
    week = "week"
    month = "month"
    hour = "hour"

    # patch dimensions
    newpath = "newpath"
    istest = "istest"
    language = "language"

    # commit dimensions
    commit_id = "commit_id"
    keyword = "keyword"

    # pr dimensions
    pr_state = "pr_state"

    # deploy_commits dimensions
    environment = "environment"

    # relative date dimensions
    day_of_week = "day_of_week"
    hour_of_day = "hour_of_day"
    sprint = "sprint"


AUTHOR_DIMENSIONS = [DimensionName.name, DimensionName.email, DimensionName.aid]

PATCH_DIMENSIONS = [
    DimensionName.newpath,
    DimensionName.istest,
    DimensionName.language,
]
DATE_DIMENSIONS = [
    DimensionName.day,
    DimensionName.week,
    DimensionName.month,
    DimensionName.hour,
]

RELATIVE_DATE_DIMENSIONS = [
    DimensionName.day_of_week,
    DimensionName.hour_of_day,
    DimensionName.sprint,
]

DEPLOY_DIMENSIONS = [
    DimensionName.environment,
]


class StatsRequest(BaseModel):
    metrics: List[MetricName]
    dimensions: Optional[List[DimensionName]] = None
    filters: Dict[FilterName, Any]
    sort_by: Optional[List[Union[str, int]]] = None
    type: str = "aggregate"  # or "select"


class QueryType(str, Enum):
    aggregate = "aggregate"
    select = "select"


class TableName(str, Enum):
    # simple tables
    commits = "commits"
    patches = "patches"
    pull_requests = "pull_requests"
    pull_request_comments = "pull_request_comments"
    authors = "authors"
    deploy_commits = "deploy_commits"


TableDef = List[TableName]


class AggregationFunction(str, Enum):
    MEAN = "mean"
    SUM = "sum"
    COUNT = "count"


class MetricDef(BaseModel):
    aggregation: AggregationFunction
    field: str
    name: Optional[str] = None


class Query(BaseModel):
    metrics: List[Union[MetricName, MetricDef]]
    dimensions: Optional[List[DimensionName]] = None
    filters: Dict[FilterName, Any]
    sort_by: Optional[List[Any]] = None
    type: QueryType
    table: Optional[TableName] = None
    extra: Optional[dict] = None

    # @validator("metrics")
    # def mixed_metrics(cls, v):
    #     if all(m in PR_METRICS for m in v) or all(m in COMMIT_METRICS for m in v) or all(m in PATCH_METRICS for m in v):
    #         return v
    #     else:
    #         raise ValueError("Cannot mix PR, PATCH and COMMIT metrics.")

    @property
    def table_def(self) -> TableDef:
        if all(m in PR_METRICS for m in self.metrics):
            return [TableName.pull_requests]
        elif all(m in PATCH_METRICS for m in self.metrics):
            ret = [TableName.patches]
            if any(d in AUTHOR_DIMENSIONS for d in self.dimensions or []):
                ret.append(TableName.authors)
            return ret
        elif all(m in COMMIT_METRICS for m in self.metrics):
            ret = [TableName.commits]
            if any(m in PATCH_METRICS for m in self.metrics) or any(
                d in PATCH_DIMENSIONS for d in self.dimensions or []
            ):
                ret.append(TableName.patches)
            if any(d in AUTHOR_DIMENSIONS for d in self.dimensions or []):
                ret.append(TableName.authors)
            return ret
        elif all(m in DEPLOY_METRICS for m in self.metrics):
            return [TableName.deploy_commits]
        elif self.table is not None:
            return [self.table]
        else:
            raise ValueError("Cannot mix PR, PATCH and COMMIT metrics.")

    def utilization_working_hours(self):
        dimensions = self.dimensions or []

        if DimensionName.day in dimensions:
            return 8
        elif DimensionName.week in dimensions:
            return 8 * 5
        elif DimensionName.month in dimensions:
            return 8 * 5 * 4

        from_, to_ = self.filters.get("day", self.filters.get("month", [None, None]))
        if from_ and to_:
            if isinstance(from_, str) and isinstance(to_, str):
                to_date = datetime.strptime(to_, "%Y-%m-%d").date()
                from_date = datetime.strptime(from_, "%Y-%m-%d").date()
            elif isinstance(from_, datetime) and isinstance(to_, datetime):
                to_date = to_.date()
                from_date = from_.date()

            elapsed = to_date - from_date
            return 8 * max(elapsed.days * 5 / 7, 1)
        else:
            return 1


class QueryResult(BaseModel):
    query: Query
    results: Any


# class StatsRequest(BaseModel):
#     metrics: List[MetricName]
#     dimensions: Optional[List[DimensionName]] = None
#     filters: Dict[FilterName, Any]
#     sort_by: Optional[List[Union[str, int]]] = None
#     type  = "aggregate"  # or "select"


class IbisTables:
    conn: Any
    pull_requests: Any
    commits: Any
    patches: Any
    authors: Any
    pull_request_comments: Any
    deploy_commits: Any

    def get_table(self, table_def: TableDef) -> Any:
        # commits_columns = columns = [col for col in self.commits.columns if col not in ["is_test"]]
        commits_patches_overlapping_columns = [
            "date",
            "aid",
            "is_merge",
            "cid",
            # "commit_id",
            # "repo_id",
        ]
        patches_join_columns = [c for c in self.patches.columns if c not in commits_patches_overlapping_columns]

        # expr = self.commits.inner_join(
        #     self.patches,
        #     [self.commits.repo_id == self.patches.repo_id, self.commits.commit_id == self.patches.commit_id],
        #     # ["repo_id", "commit_id"],
        # )

        # e = expr[
        #     self.commits,
        #     self.patches["lang"],
        #     self.patches["is_test"],
        #     self.patches["anomaly"],
        #     self.patches["outlier"],
        # ]
        # m = e.materialize()

        # print("*********************************")
        # print(m)

        # # print("****", ibis.postgres.compile(expr))
        # print("*********************************")
        # # print(expr["date"].materialize())

        if table_def == [TableName.pull_requests]:
            return self.pull_requests
        elif table_def == [TableName.pull_request_comments]:
            return self.pull_request_comments
        elif table_def == [TableName.commits]:
            return self.commits
        elif table_def == [TableName.patches]:
            return self.patches
        elif table_def == [TableName.deploy_commits]:
            return self.deploy_commits
        elif table_def == [TableName.commits, TableName.patches]:

            expr = self.commits.inner_join(
                self.patches,
                [self.commits.repo_id == self.patches.repo_id, self.commits.commit_id == self.patches.commit_id],
                # ["repo_id", "commit_id"],
            )
            e = expr[
                self.commits["repo_id"],
                self.commits["commit_id"],
                self.commits["loc_effort_c"],
                self.commits["hours"],
                self.commits["loc_i_c"],
                self.commits["uploc_c"],
                self.commits["comp_i_c"],
                self.commits["comp_d_c"],
                self.commits["velocity"],
                self.commits["aid"],
                self.commits["aemail"],
                self.commits["aname"],
                self.commits["date"],
                self.commits["is_merge"],
                self.patches["lang"],
                self.patches["is_test"],
                self.patches["anomaly"],
                self.patches["outlier"],
            ]
            m = e.materialize()
            return m

        elif table_def == [TableName.commits, TableName.authors]:
            e = self.commits.inner_join(self.authors, [("aid", "id")])
            m = e.materialize()
            return m
        elif table_def == [TableName.commits, TableName.patches, TableName.authors]:
            e = self.commits.inner_join(
                self.patches[patches_join_columns],
                (self.patches.commit_id == self.commits.commit_id) & (self.patches.repo_id == self.commits.repo_id),
            ).inner_join(self.authors, ["aid"])
            m = e.materialize()
            return m
        elif table_def == [TableName.patches, TableName.authors]:
            e = self.patches.inner_join(self.authors, [("aid", "id")])
            m = e.materialize()
            return m
        else:
            raise ValueError(f"Unknown ibis table def {table_def}")
