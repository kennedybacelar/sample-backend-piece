from datetime import datetime, timedelta
from typing import Optional, List
from enum import Enum
from collections import namedtuple
from structlog import get_logger
from sqlalchemy import select, and_, exists

from gitential2.core import GitentialContext
from gitential2.datatypes.cli_v2 import CleanupType

logger = get_logger(__name__)

CommitTables = namedtuple("CommitTables", ["cid_column_name", "repo_id_column_name"])
PullRequestsTables = namedtuple("PullRequestsTables", ["prid_column_name", "repo_id_column_name"])
ITSProjectsTables = namedtuple("ITSProjectsTables", ["issue_id_column_name", "itsp_id_column_name"])


class CleaningGroup(str, Enum):
    commits = "commits"
    pull_requests = "pull_requests"
    its_projects = "its_projects"


# its_sptrint table is left out - has to be added later on
all_tables_info = {
    CleaningGroup.commits: {
        "calculated_commits": CommitTables("commit_id", "repo_id"),
        "extracted_patches": CommitTables("commit_id", "repo_id"),
        "calculated_patches": CommitTables("commit_id", "repo_id"),
        "extracted_patch_rewrites": CommitTables("commit_id", "repo_id"),
        "extracted_commit_branches": CommitTables("commit_id", "repo_id"),
        # The reference table should be the last item of iteration
        "extracted_commits": CommitTables("commit_id", "repo_id"),
    },
    CleaningGroup.pull_requests: {
        "pull_request_commits": PullRequestsTables("pr_number", "repo_id"),
        "pull_request_comments": PullRequestsTables("pr_number", "repo_id"),
        "pull_request_labels": PullRequestsTables("pr_number", "repo_id"),
        # The reference table should be the last item of iteration
        "pull_requests": PullRequestsTables("number", "repo_id"),
    },
    CleaningGroup.its_projects: {
        "its_issue_changes": ITSProjectsTables("issue_id", "itsp_id"),
        "its_issue_times_in_statuses": ITSProjectsTables("issue_id", "itsp_id"),
        "its_issue_comments": ITSProjectsTables("issue_id", "itsp_id"),
        "its_issue_linked_issues": ITSProjectsTables("issue_id", "itsp_id"),
        # "its_sprints": ITSProjectsTables("issue_id", "itsp_id"),
        "its_issue_sprints": ITSProjectsTables("issue_id", "itsp_id"),
        "its_issue_worklogs": ITSProjectsTables("issue_id", "itsp_id"),
        # The reference table should be the last item of iteration
        "its_issues": ITSProjectsTables("id", "itsp_id"),
    },
}


# pylint: disable=too-complex
def perform_data_cleanup(
    g: GitentialContext,
    workspace_ids: List[int],
    cleanup_type: Optional[CleanupType] = CleanupType.full,
    date_to: Optional[datetime] = None,
    its_date_to: Optional[datetime] = None,
):
    repo_analysis_limit_in_days = g.settings.extraction.repo_analysis_limit_in_days
    its_project_analysis_limit_in_days = g.settings.extraction.its_project_analysis_limit_in_days
    date_to = date_to or __get_date_to(repo_analysis_limit_in_days)
    its_date_to = its_date_to or __get_date_to(its_project_analysis_limit_in_days)

    for workspace_id in workspace_ids:
        repo_ids_to_delete = __get_repo_ids_to_delete(g, workspace_id)
        itsp_ids_to_delete = __get_itsp_ids_to_delete(g, workspace_id)

        logger.info(
            "Starting data cleanup for workspace.",
            workspace_id=workspace_id,
            repo_analysis_limit_in_days=repo_analysis_limit_in_days,
            its_project_analysis_limit_in_days=its_project_analysis_limit_in_days,
            date_to=date_to,
            its_date_to=its_date_to,
            repo_ids_to_delete=repo_ids_to_delete,
            itsp_ids_to_delete=itsp_ids_to_delete,
        )

        if cleanup_type in (CleanupType.full, CleanupType.commits):
            if date_to or repo_ids_to_delete:
                __remove_redundant_data(
                    g,
                    workspace_id,
                    date_to or datetime.min,
                    repo_ids_to_delete,
                    CleaningGroup("commits"),
                )
        if cleanup_type in (CleanupType.full, CleanupType.pull_requests):
            if date_to or repo_ids_to_delete:
                __remove_redundant_data(
                    g,
                    workspace_id,
                    date_to or datetime.min,
                    repo_ids_to_delete,
                    CleaningGroup("pull_requests"),
                )
        if cleanup_type in (CleanupType.full, CleanupType.its_projects):
            if its_date_to or itsp_ids_to_delete:
                __remove_redundant_data(
                    g,
                    workspace_id,
                    its_date_to or datetime.min,
                    itsp_ids_to_delete,
                    CleaningGroup("its_projects"),
                )
        if repo_ids_to_delete or itsp_ids_to_delete:
            if cleanup_type in (CleanupType.full, CleanupType.redis):
                __remove_redundant_data_for_redis(
                    g,
                    workspace_id,
                    repo_ids_to_delete,
                    itsp_ids_to_delete,
                )
            if cleanup_type == CleanupType.full:
                __delete_repositories_or_itsp_projects(g, workspace_id, repo_ids_to_delete, itsp_ids_to_delete)


def __get_keys_to_be_deleted(
    g: GitentialContext,
    repo_or_itsp_ids_to_delete: List[int],
    cleaning_group: CleaningGroup,
    date_to: datetime,
):
    # Creating common table expression and returning the commit_ids
    table_ = __get_reference_table(g, cleaning_group)
    if cleaning_group == CleaningGroup.commits:
        return (
            select([table_.table.c.commit_id, table_.table.c.repo_id])
            .where(and_(table_.table.c.atime > date_to, table_.table.c.repo_id.not_in(repo_or_itsp_ids_to_delete)))
            .cte()
        )
    if cleaning_group == CleaningGroup.pull_requests:
        return (
            select([table_.table.c.number, table_.table.c.repo_id])
            .where(and_(table_.table.c.created_at > date_to, table_.table.c.repo_id.not_in(repo_or_itsp_ids_to_delete)))
            .cte()
        )
    if cleaning_group == CleaningGroup.its_projects:
        return (
            select([table_.table.c.id, table_.table.c.itsp_id])
            .where(and_(table_.table.c.created_at > date_to, table_.table.c.itsp_id.not_in(repo_or_itsp_ids_to_delete)))
            .cte()
        )
    return None


def __remove_redundant_data(
    g: GitentialContext,
    workspace_id: int,
    date_to: datetime,
    repo_or_itsp_ids_to_delete: List[int],
    cleaning_group: CleaningGroup,
):
    # table keypair is needed because the uniqueness of each row is determined by a pair of fields
    # in case of commits: repo_id + commit_id
    # in case of pull_requests: pr_id + repo_id
    # in case its_issues: issue_id + itsp_id

    # cte = common table expression
    cte = __get_keys_to_be_deleted(g, repo_or_itsp_ids_to_delete, cleaning_group, date_to)
    for table_name, table_keypair in all_tables_info.get(cleaning_group, {}).items():  # type: ignore[attr-defined]
        table_ = getattr(g.backend, table_name)
        __delete_records(workspace_id, table_, cte, cleaning_group, table_keypair)


def __remove_redundant_data_for_redis(
    g: GitentialContext,
    workspace_id: int,
    repo_ids_to_delete: List[int],
    itsp_ids_to_delete: List[int],
):
    logger.info("Attempting to clean redis data.", workspace_id=workspace_id)
    keys = []
    for rid in repo_ids_to_delete:
        redis_key_1 = f"ws-{workspace_id}:repository-refresh-{rid}"
        g.kvstore.delete_value(name=redis_key_1)
        redis_key_2 = f"ws-{workspace_id}:r-{rid}:extraction"
        g.kvstore.delete_value(name=redis_key_2)
        redis_key_3 = f"ws-{workspace_id}:repository-status-{rid}"
        g.kvstore.delete_value(name=redis_key_3)
        keys.extend([redis_key_1, redis_key_2, redis_key_3])

    for itsp_id in itsp_ids_to_delete:
        redis_key = f"ws-{workspace_id}:itsp-{itsp_id}"
        g.kvstore.delete_value(name=redis_key)
        keys.append(redis_key)

    logger.info("Keys deleted from redis.", keys=keys)


def __get_date_to(number_of_days_diff: Optional[int] = None) -> Optional[datetime]:
    return (
        datetime.utcnow() - timedelta(days=number_of_days_diff)
        if number_of_days_diff and number_of_days_diff > 0
        else None
    )


def __get_repo_ids_to_delete(g: GitentialContext, workspace_id: int) -> List[int]:
    repo_ids_all: List[int] = [r.id for r in g.backend.repositories.all(workspace_id)]
    assigned_repos = {r.repo_id for r in g.backend.project_repositories.all(workspace_id)}

    repos_to_be_deleted = [rid for rid in repo_ids_all if rid not in assigned_repos]
    return repos_to_be_deleted


def __get_itsp_ids_to_delete(g: GitentialContext, workspace_id: int) -> List[int]:
    itsp_ids_all: List[int] = [itsp.id for itsp in g.backend.its_projects.all(workspace_id)]
    assigned_itsp = {itsp.itsp_id for itsp in g.backend.project_its_projects.all(workspace_id)}

    itsp_ids_to_be_deleted = [itsp_id for itsp_id in itsp_ids_all if itsp_id not in assigned_itsp]
    return itsp_ids_to_be_deleted


def __delete_records(workspace_id, table_, cte, cleaning_group, table_keypair):
    logger.info(f"Attempting to delete rows from {table_.table.name} table.", workspace_id=workspace_id)
    schema_name = f"ws_{workspace_id}"
    if cleaning_group == CleaningGroup.commits:
        query = table_.table.delete().where(
            ~exists().where(
                and_(
                    getattr(table_.table.c, table_keypair.cid_column_name) == cte.c.commit_id,
                    getattr(table_.table.c, table_keypair.repo_id_column_name) == cte.c.repo_id,
                )
            )
        )
    if cleaning_group == CleaningGroup.pull_requests:
        query = table_.table.delete().where(
            ~exists().where(
                and_(
                    getattr(table_.table.c, table_keypair.prid_column_name) == cte.c.number,
                    getattr(table_.table.c, table_keypair.repo_id_column_name) == cte.c.repo_id,
                )
            )
        )
    if cleaning_group == CleaningGroup.its_projects:
        query = table_.table.delete().where(
            ~exists().where(
                and_(
                    getattr(table_.table.c, table_keypair.issue_id_column_name) == cte.c.id,
                    getattr(table_.table.c, table_keypair.itsp_id_column_name) == cte.c.itsp_id,
                )
            )
        )
    with table_.engine.connect().execution_options(
        autocommit=True,
        schema_translate_map={None: schema_name},
    ) as conn:
        conn.execute(query)


def __delete_repositories_or_itsp_projects(
    g: GitentialContext, workspace_id: int, repo_ids_to_delete: List[int], itsp_ids_to_delete: List[int]
):
    if repo_ids_to_delete:
        g.backend.repositories.delete_repos_by_id(workspace_id, repo_ids_to_delete)
    if itsp_ids_to_delete:
        g.backend.its_projects.delete_its_projects_by_id(workspace_id, itsp_ids_to_delete)


def __get_reference_table(g: GitentialContext, cleaning_group: CleaningGroup):

    reference_tables = {
        CleaningGroup.commits: g.backend.extracted_commits,
        CleaningGroup.pull_requests: g.backend.pull_requests,
        CleaningGroup.its_projects: g.backend.its_issues,
    }

    return reference_tables.get(cleaning_group)
