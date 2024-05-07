from datetime import datetime
from typing import List, Optional, Set
from collections import defaultdict
from gitential2.datatypes.stats import FilterName, Query, DimensionName, MetricName, QueryType
from gitential2.datatypes.authors import AuthorFilters, DateRange
from .context import GitentialContext
from .stats_v2 import IbisQuery
from .authors_list import list_extended_committer_authors  # pylint: disable=import-outside-toplevel,cyclic-import


def get_repos_projects(g: GitentialContext, workspace_id: int) -> dict:
    projects = {p.id: p for p in g.backend.projects.all(workspace_id)}
    project_repos = g.backend.project_repositories.all(workspace_id)
    ret = defaultdict(list)
    for pr in project_repos:
        p = projects.get(pr.project_id)
        if p:
            ret[pr.repo_id].append({"id": pr.repo_id, "project_id": p.id, "project_name": p.name})
    return ret


def _get_commit_counts_by_dev_and_repo(
    g: GitentialContext,
    workspace_id: int,
    project_id: Optional[int] = None,
    repo_id: Optional[int] = None,
    from_: Optional[str] = None,
    to_: Optional[str] = None,
):
    query = Query(
        dimensions=[
            DimensionName.aid,
            DimensionName.repo_id,
        ],
        filters={FilterName.is_merge: False, FilterName.active: False},
        metrics=[MetricName.count_commits],
        type=QueryType.aggregate,
        sort_by=[["aid", True], ["count_commits", False]],
    )
    if project_id:
        query.filters[FilterName.project_id] = project_id
    if repo_id:
        query.filters[FilterName.repo_ids] = [repo_id]
    if from_ and to_:
        query.filters[FilterName.day] = [from_, to_]
    result = IbisQuery(g, workspace_id, query).execute()
    return result.values.to_dict("records")


def get_repo_top_devs(g: GitentialContext, workspace_id: int) -> dict:
    dev_repo_commit_counts = _get_commit_counts_by_dev_and_repo(g, workspace_id)
    author_names = {author.id: author.name for author in g.backend.authors.all(workspace_id) if author.active}
    results: dict = defaultdict(lambda: {"developer_ids": [], "names": []})

    for row in dev_repo_commit_counts:
        if row["aid"] in author_names:
            name = author_names[row["aid"]]

            results[row["repo_id"]]["developer_ids"].append(row["aid"])
            results[row["repo_id"]]["names"].append(name)

    return _to_categories_series_response(results, series_names=["developer_ids", "names"])


def get_dev_top_repos(g: GitentialContext, workspace_id: int) -> dict:
    dev_repo_commit_counts = _get_commit_counts_by_dev_and_repo(g, workspace_id)
    author_emails = {author.id: author.email for author in g.backend.authors.all(workspace_id) if author.active}
    repository_names = {repo.id: repo.name for repo in g.backend.repositories.all(workspace_id)}

    results: dict = defaultdict(lambda: {"repo_ids": [], "repo_names": []})

    for row in dev_repo_commit_counts:
        if row["aid"] in author_emails and row["repo_id"] in repository_names:
            results[row["aid"]]["repo_ids"].append(row["repo_id"])
            results[row["aid"]]["repo_names"].append(repository_names[row["repo_id"]])

    return _to_categories_series_response(results, series_names=["repo_ids", "repo_names"])


def authors_in_projects(g: GitentialContext, workspace_id: int) -> dict:
    dev_repo_commit_counts = _get_commit_counts_by_dev_and_repo(g, workspace_id)
    author_emails = {author.id: author.email for author in g.backend.authors.all(workspace_id) if author.active}
    project_names = {project.id: project.name for project in g.backend.projects.all(workspace_id)}
    repos_to_projects = defaultdict(list)

    for project_repo in g.backend.project_repositories.all(workspace_id):
        repos_to_projects[project_repo.repo_id].append(project_repo.project_id)

    results: dict = defaultdict(lambda: {"project_ids": [], "project_names": []})

    for row in dev_repo_commit_counts:
        if row["aid"] in author_emails and row["repo_id"] in repos_to_projects:
            project_ids = repos_to_projects[row["repo_id"]]
            for project_id in project_ids:
                if project_id not in results[row["aid"]]["project_ids"] and project_id in project_names:
                    results[row["aid"]]["project_ids"].append(project_id)
                    results[row["aid"]]["project_names"].append(project_names[project_id])
    return results


def get_dev_related_projects(g: GitentialContext, workspace_id: int) -> dict:
    results = authors_in_projects(g, workspace_id)
    return _to_categories_series_response(results, series_names=["project_ids", "project_names"])


def _to_categories_series_response(result_dict: dict, series_names: List[str]):
    ret: dict = {"categories": [], "series": {}}

    for series_name in series_names:
        ret["series"][series_name] = []

    for key, value in result_dict.items():
        ret["categories"].append(key)
        for series_name in series_names:
            ret["series"][series_name].append(",".join(str(v) for v in value[series_name]))

    return ret


def get_developers(
    g: GitentialContext,
    workspace_id: int,
    project_id: Optional[int] = None,
    repo_id: Optional[int] = None,
    team_id: Optional[int] = None,
    from_: Optional[str] = None,
    to_: Optional[str] = None,
    is_dev_active_filter_on: Optional[bool] = True,  # pylint: disable=unused-argument
    limit: int = 10000,
) -> list:
    date_range = DateRange(
        start=from_ or datetime.min,
        end=to_ or datetime.max,
    )
    author_filters = AuthorFilters(
        workspace_id=workspace_id,
        project_ids=[project_id] if project_id else [],
        repository_ids=[repo_id] if repo_id else [],
        team_ids=[team_id] if team_id else [],
        date_range=date_range,
        limit=limit,
    )

    developers = list_extended_committer_authors(g, workspace_id, author_filters).authors_list
    ret = [{"name": dev.name, "email": dev.email, "id": dev.id} for dev in developers]
    return ret


def get_devs_assigned_to_active_repos(g: GitentialContext, workspace_id: int, repo_ids: Set[int]) -> List[int]:
    query = Query(
        dimensions=[
            DimensionName.developer_id,
            DimensionName.repo_id,
        ],
        filters={FilterName.repo_ids: repo_ids},
        metrics=[MetricName.count_commits],
        type=QueryType.aggregate,
    )
    result = IbisQuery(g, workspace_id, query).execute()
    return result.values.developer_id.unique()
