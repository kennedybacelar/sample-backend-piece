from typing import List, Optional

from structlog import get_logger

from gitential2.core.legacy import get_devs_assigned_to_active_repos
from gitential2.datatypes.projects import (
    ProjectInDB,
    ProjectCreate,
    ProjectUpdate,
    ProjectCreateWithRepositories,
    ProjectUpdateWithRepositories,
)
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryUpdate  # , RepositoryStatus
from gitential2.datatypes.sprints import Sprint
from .context import GitentialContext
from .refresh_v2 import refresh_project
from ..datatypes.its_projects import ITSProjectUpdate, ITSProjectCreate, ITSProjectInDB

logger = get_logger(__name__)


def list_projects(g: GitentialContext, workspace_id: int) -> List[ProjectInDB]:
    return list(g.backend.projects.all(workspace_id=workspace_id))


def create_project_without_repos(g: GitentialContext, workspace_id: int, project_create: ProjectCreate) -> ProjectInDB:
    return g.backend.projects.create(workspace_id, project_create)


def update_project_without_repos(
    g: GitentialContext, workspace_id: int, project_id: int, project_update: ProjectUpdate
) -> ProjectInDB:
    return g.backend.projects.update(workspace_id=workspace_id, id_=project_id, obj=project_update)


def create_project(
    g: GitentialContext, workspace_id: int, project_create: ProjectCreateWithRepositories
) -> ProjectInDB:
    project = create_project_without_repos(
        g, workspace_id, ProjectCreate(**project_create.dict(exclude={"repos", "its_projects"}))
    )

    _update_project_repos(g, workspace_id=workspace_id, project=project, repos=project_create.repos)
    _update_project_its_projects(
        g, workspace_id=workspace_id, project=project, its_projects=project_create.its_projects
    )
    refresh_project(g, workspace_id=workspace_id, project_id=project.id)

    return project


def get_project(g: GitentialContext, workspace_id: int, project_id: int) -> ProjectInDB:
    return g.backend.projects.get_or_error(workspace_id=workspace_id, id_=project_id)


def update_project(
    g: GitentialContext,
    workspace_id: int,
    project_id: int,
    project_update: ProjectUpdateWithRepositories,
) -> ProjectInDB:
    project = update_project_without_repos(
        g, workspace_id, project_id, ProjectUpdate(**project_update.dict(exclude={"repos", "its_projects"}))
    )

    _update_project_repos(g, workspace_id=workspace_id, project=project, repos=project_update.repos)
    _update_project_its_projects(
        g, workspace_id=workspace_id, project=project, its_projects=project_update.its_projects
    )
    refresh_project(g, workspace_id, project_id)
    _recalculate_active_authors(g, workspace_id)

    return project


def delete_project(g: GitentialContext, workspace_id: int, project_id: int) -> bool:
    g.backend.project_repositories.update_project_repositories(workspace_id, project_id, [])
    g.backend.project_its_projects.update_its_projects(workspace_id, project_id, [])
    g.backend.projects.delete(workspace_id, project_id)
    _recalculate_active_authors(g, workspace_id)
    return True


def _update_project_repos(g: GitentialContext, workspace_id: int, project: ProjectInDB, repos: List[RepositoryCreate]):
    repositories = [
        g.backend.repositories.create_or_update_by_clone_url(workspace_id=workspace_id, obj=r) for r in repos
    ]
    return g.backend.project_repositories.update_project_repositories(
        workspace_id=workspace_id, project_id=project.id, repo_ids=[r.id for r in repositories]
    )


def _update_project_its_projects(
    g: GitentialContext,
    workspace_id: int,
    project: ProjectInDB,
    its_projects: Optional[List[ITSProjectCreate]],
):
    its_p: List[ITSProjectInDB] = (
        [g.backend.its_projects.create_or_update_by_api_url(workspace_id=workspace_id, obj=r) for r in its_projects]
        if its_projects is not None
        else []
    )
    return g.backend.project_its_projects.update_its_projects(
        workspace_id=workspace_id, project_id=project.id, itsp_ids=[r.id for r in its_p]
    )


def update_sprint_by_project_id(g: GitentialContext, workspace_id: int, project_id: int, sprint: Sprint) -> bool:
    return g.backend.projects.update_sprint_by_project_id(
        workspace_id=workspace_id, project_id=project_id, sprint=sprint
    )


def _recalculate_active_authors(g: GitentialContext, workspace_id: int):

    all_assigned_repo_ids = g.backend.project_repositories.get_all_repos_assigned_to_projects(workspace_id)
    devs_assigned_to_active_repos = get_devs_assigned_to_active_repos(g, workspace_id, all_assigned_repo_ids)

    authors_to_be_deactivated = set()
    authors_to_be_activated = set()

    for author in g.backend.authors.all(workspace_id):
        if author.id in devs_assigned_to_active_repos:
            authors_to_be_activated.add(author.id)
        else:
            authors_to_be_deactivated.add(author.id)

    g.backend.authors.change_active_status_authors_by_ids(
        workspace_id=workspace_id, author_ids=authors_to_be_deactivated, active_status=False
    )
    g.backend.authors.change_active_status_authors_by_ids(
        workspace_id=workspace_id, author_ids=authors_to_be_activated, active_status=True
    )


def add_repos_to_project(
    g: GitentialContext, workspace_id: int, project_id: int, repos_to_add: List[RepositoryCreate]
) -> List[int]:
    repositories = [
        g.backend.repositories.create_or_update_by_clone_url(workspace_id=workspace_id, obj=r) for r in repos_to_add
    ]
    return g.backend.project_repositories.add_project_repositories(
        workspace_id=workspace_id, project_id=project_id, repo_ids_to_add=[r.id for r in repositories]
    )


def remove_repos_to_project(
    g: GitentialContext, workspace_id: int, project_id: int, repos_to_remove: List[RepositoryUpdate]
) -> List[int]:
    repositories = [
        g.backend.repositories.create_or_update_by_clone_url(workspace_id=workspace_id, obj=r) for r in repos_to_remove
    ]
    return g.backend.project_repositories.remove_project_repositories(
        workspace_id=workspace_id, project_id=project_id, repo_ids_to_remove=[r.id for r in repositories]
    )


def add_its_projects_to_project(
    g: GitentialContext, workspace_id: int, project_id: int, its_projects_to_add: List[ITSProjectCreate]
) -> List[int]:
    its_projects = [
        g.backend.its_projects.create_or_update_by_api_url(workspace_id=workspace_id, obj=r)
        for r in its_projects_to_add
    ]
    return g.backend.project_its_projects.add_its_projects(
        workspace_id=workspace_id, project_id=project_id, itsp_ids_to_add=[itsp.id for itsp in its_projects]
    )


def remove_its_projects_from_project(
    g: GitentialContext, workspace_id: int, project_id: int, its_projects_to_remove: List[ITSProjectUpdate]
) -> List[int]:
    its_projects = [
        g.backend.its_projects.create_or_update_by_api_url(workspace_id=workspace_id, obj=r)
        for r in its_projects_to_remove
    ]
    return g.backend.project_its_projects.remove_its_projects(
        workspace_id=workspace_id, project_id=project_id, itsp_ids_to_remove=[itsp.id for itsp in its_projects]
    )
