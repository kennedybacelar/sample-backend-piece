from typing import cast
from gitential2.core.context import GitentialContext
from gitential2.datatypes.projects import ProjectStatus
from gitential2.datatypes.repositories import RepositoryStatus
from .repositories import list_project_repositories
from .context import GitentialContext


def get_project_status(g: GitentialContext, workspace_id: int, project_id: int) -> ProjectStatus:
    repositories = list_project_repositories(g, workspace_id, project_id=project_id)
    project = g.backend.projects.get_or_error(workspace_id=workspace_id, id_=project_id)
    repo_statuses = [get_repository_status(g, workspace_id, r.id) for r in repositories]
    return ProjectStatus(
        id=project_id,
        name=project.name,
        status="...",
        done=all(rs.done for rs in repo_statuses),
        repos=repo_statuses,
    )


def _repo_status_key(workspace_id: int, repository_id: int) -> str:
    return f"ws-{workspace_id}:repository-status-{repository_id}"


def has_repository_status(g: GitentialContext, workspace_id: int, repository_id: int) -> bool:
    return g.kvstore.get_value(_repo_status_key(workspace_id, repository_id)) is not None


def get_repository_status(g: GitentialContext, workspace_id: int, repository_id: int) -> RepositoryStatus:

    current_status_dict = g.kvstore.get_value(_repo_status_key(workspace_id, repository_id))
    if current_status_dict:
        return RepositoryStatus(**cast(dict, current_status_dict))
    else:
        return init_repository_status(g, workspace_id, repository_id)


def init_repository_status(g: GitentialContext, workspace_id: int, repository_id: int) -> RepositoryStatus:
    repository = g.backend.repositories.get(workspace_id=workspace_id, id_=repository_id)
    if repository:
        initial_status = RepositoryStatus(id=repository_id, name=repository.name).reset()
        return persist_repository_status(g, workspace_id, repository_id, initial_status)
    else:
        raise ValueError(f"No repository find for id {repository_id}")


def persist_repository_status(
    g: GitentialContext, workspace_id: int, repository_id: int, status: RepositoryStatus
) -> RepositoryStatus:
    status_dict = status.dict()
    g.kvstore.set_value(_repo_status_key(workspace_id, repository_id), status_dict)
    return status


def update_repository_status(g: GitentialContext, workspace_id: int, repository_id: int, **kwargs) -> RepositoryStatus:
    current_status = get_repository_status(g, workspace_id, repository_id)
    status_dict = current_status.dict()
    status_dict.update(**kwargs)
    g.kvstore.set_value(_repo_status_key(workspace_id, repository_id), status_dict)
    return RepositoryStatus(**status_dict)


def delete_repository_status(g: GitentialContext, workspace_id: int, repository_id: int):
    g.kvstore.delete_value(_repo_status_key(workspace_id, repository_id))
