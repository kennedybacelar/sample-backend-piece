# pylint: disable=compare-to-zero, unnecessary-comprehension
import datetime as dt

from typing import List
from gitential2.datatypes.stats import DimensionName, FilterName, MetricName, Query, QueryType

from .context import GitentialContext
from .repositories import list_project_repositories
from .workspaces import get_own_workspaces
from .subscription import get_current_subscription
from .stats_v2 import collect_stats_v2
from .access_log import get_last_interaction_at
from .authors import list_active_authors


def calculate_workspace_usage_statistics(g: GitentialContext, workspace_id: int) -> dict:
    workspace = g.backend.workspaces.get_or_error(workspace_id)
    projects = g.backend.projects.all(workspace_id)
    active_authors = list_active_authors(g, workspace_id)
    active_author_dicts = [{"name": author.name, "email": "author.email", "id": author.id} for author in active_authors]
    active_author_ids: List[int] = [a.id for a in active_authors]
    repositories: dict = {}

    for p in projects:
        project_repositories = list_project_repositories(g, workspace_id, project_id=p.id)
        for repo in project_repositories:
            if repo.id not in repositories:
                repositories[repo.id] = repo

    repos_by = {}
    repos_by["total"] = [repo for repo in repositories.values()]
    repos_by["private"] = [repo for repo in repositories.values() if repo.private is True]
    repos_by["public"] = [repo for repo in repositories.values() if repo.private is False]
    repos_by["github"] = [repo for repo in repositories.values() if repo.integration_type == "github"]
    repos_by["bitbucket"] = [repo for repo in repositories.values() if repo.integration_type == "bitbucket"]
    repos_by["vsts"] = [repo for repo in repositories.values() if repo.integration_type == "vsts"]
    repos_by["gitlab"] = [repo for repo in repositories.values() if repo.integration_type == "gitlab"]

    usage_stats = {
        "workspace_id": workspace.id,
        "workspace_name": workspace.name,
        "authors_with_active_flag": active_author_dicts,
        "authors_with_active_flag_count": len(active_authors),
    }
    for group_name, repos in repos_by.items():
        usage_stats[f"{group_name}_repos_count"] = len(repos)
        usage_stats[f"{group_name}_committers_30days"] = _get_commiters_count(
            g, workspace_id, repo_ids=[repo.id for repo in repos], active_author_ids=active_author_ids, days=30
        )
        usage_stats[f"{group_name}_committers_90days"] = _get_commiters_count(
            g, workspace_id, repo_ids=[repo.id for repo in repos], active_author_ids=active_author_ids, days=90
        )

    return usage_stats


def _get_commiters_count(g, workspace_id: int, repo_ids: List[int], active_author_ids: List[int], days=30):
    if repo_ids:
        results = collect_stats_v2(
            g,
            workspace_id=workspace_id,
            query=Query(
                dimensions=[DimensionName.aid],
                filters={
                    FilterName.repo_ids: repo_ids,
                    FilterName.day: [
                        (dt.date.today() - dt.timedelta(days=days)).isoformat(),
                        dt.date.today().isoformat(),
                    ],
                },
                metrics=[MetricName.count_commits],
                type=QueryType.aggregate,
            ),
        )
        if "aid" in results:
            aids = [aid for aid in results["aid"] if aid in active_author_ids]
            return len(aids)
        else:
            return 0
    else:
        return 0


def calculate_user_statistics(g: GitentialContext, user_id: int) -> dict:
    user = g.backend.users.get_or_error(user_id)

    usage_stats: dict = {
        "user": user.dict(exclude={"extra"}),
        "subscription": get_current_subscription(g, user_id=user_id).dict(),
    }
    usage_stats["user"]["last_interaction_at"] = get_last_interaction_at(g, user.id)
    workspaces = get_own_workspaces(g, user.id)
    usage_stats["workspace_stats"] = {
        workspace.id: calculate_workspace_usage_statistics(g, workspace.id) for workspace in workspaces
    }
    usage_stats["sum"] = {}
    for prefix in ["total", "private", "public", "github", "bitbucket", "vsts", "gitlab"]:
        for postfix in ["repos_count", "committers_30days", "committers_90days"]:
            field_name = "_".join([prefix, postfix])
            usage_stats["sum"][field_name] = sum(ws[field_name] for ws in usage_stats["workspace_stats"].values())

    usage_stats["sum"]["authors_with_active_flag_count"] = sum(
        ws["authors_with_active_flag_count"] for ws in usage_stats["workspace_stats"].values()
    )
    return usage_stats
