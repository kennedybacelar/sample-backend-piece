from datetime import datetime
from typing import cast, List, Tuple, Optional, Dict
from gitential2.core.its import get_itsp_status

from gitential2.datatypes.refresh_statuses import (
    ITSProjectRefreshStatus,
    ProjectRefreshStatus,
    RepositoryRefreshStatus,
    RefreshStatus,
)
from .repositories import list_project_repositories
from .context import GitentialContext


def _get_project_refresh_status_summary(
    repositories: List[RepositoryRefreshStatus],
    its_projects: List[ITSProjectRefreshStatus],
) -> Tuple[RefreshStatus, Optional[Dict[str, str]]]:

    summaries_repo = {repository.repository_name: repository.summary() for repository in repositories}
    summaries_itsp = {itsp.name: itsp.summary() for itsp in its_projects}
    summaries = {**summaries_repo, **summaries_itsp}

    ret_status: RefreshStatus = RefreshStatus.up_to_date
    reason: Optional[Dict[str, str]] = None

    if any(s[0] == RefreshStatus.in_progress for s in summaries.values()):
        ret_status = RefreshStatus.in_progress

    elif any(s[0] == RefreshStatus.error for s in summaries.values()):
        ret_status = RefreshStatus.error
        reason = {n: s[1] for n, s in summaries.items() if s[0] == RefreshStatus.error}

    return ret_status, reason


def _get_last_refreshed_at(repositories: List[RepositoryRefreshStatus]) -> Optional[datetime]:
    ret = None
    for repo_status in repositories:
        if (not ret) or (repo_status.commits_last_run is not None and (repo_status.commits_last_run < ret)):
            ret = repo_status.commits_last_run
    return ret


def get_project_refresh_status(g: GitentialContext, workspace_id: int, project_id: int) -> ProjectRefreshStatus:
    repositories = list_project_repositories(g, workspace_id, project_id=project_id)
    its_project_ids = g.backend.project_its_projects.get_itsp_ids_for_project(workspace_id, project_id)
    project = g.backend.projects.get_or_error(workspace_id=workspace_id, id_=project_id)
    repo_statuses = [get_repo_refresh_status(g, workspace_id, r.id) for r in repositories]
    its_project_statuses = [get_itsp_status(g, workspace_id, itsp_id) for itsp_id in its_project_ids]
    legacy_repo_statuses = [repo_status.to_legacy() for repo_status in repo_statuses]
    status, reason = _get_project_refresh_status_summary(repo_statuses, its_project_statuses)

    return ProjectRefreshStatus(
        workspace_id=workspace_id,
        id=project_id,
        name=project.name,
        done=all(rs.done for rs in legacy_repo_statuses) and all(itsp_st.is_done() for itsp_st in its_project_statuses),
        repos=legacy_repo_statuses,
        repositories=repo_statuses,
        its_projects=its_project_statuses,
        status=status,
        reason=reason,
        last_refreshed_at=_get_last_refreshed_at(repo_statuses),
    )


def _repo_refresh_status_key(workspace_id: int, repository_id: int) -> str:
    return f"ws-{workspace_id}:repository-refresh-{repository_id}"


def get_repo_refresh_status(g: GitentialContext, workspace_id: int, repository_id: int) -> RepositoryRefreshStatus:
    current_dict = cast(dict, g.kvstore.get_value(_repo_refresh_status_key(workspace_id, repository_id)))

    def _get_repository_name():
        repository = g.backend.repositories.get_or_error(workspace_id, repository_id)
        return repository.name

    if current_dict:
        if "workspace_id" not in current_dict:
            current_dict["workspace_id"] = workspace_id
        if "repository_id" not in current_dict:
            current_dict["repository_id"] = repository_id
        if "repository_name" not in current_dict:
            current_dict["repository_name"] = _get_repository_name()
        return RepositoryRefreshStatus(**cast(dict, current_dict))
    else:
        return RepositoryRefreshStatus(
            workspace_id=workspace_id, repository_id=repository_id, repository_name=_get_repository_name()
        )


def persist_repo_refresh_status(
    g: GitentialContext, workspace_id: int, repository_id: int, status: RepositoryRefreshStatus
) -> RepositoryRefreshStatus:
    status_dict = status.dict()
    g.kvstore.set_value(_repo_refresh_status_key(workspace_id, repository_id), status_dict)
    return status


def update_repo_refresh_status(g: GitentialContext, workspace_id: int, repository_id: int, **kwargs):
    current_status = get_repo_refresh_status(g, workspace_id, repository_id)
    current_status_dict = current_status.dict()
    for k, v in kwargs.items():
        current_status_dict[k] = v
    new_status = RepositoryRefreshStatus(**current_status_dict)
    return persist_repo_refresh_status(g, workspace_id, repository_id, new_status)
