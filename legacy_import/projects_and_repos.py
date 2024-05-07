import sys
from typing import List, Optional
from pydantic import ValidationError

from structlog import get_logger
from gitential2.core import GitentialContext

from gitential2.datatypes.repositories import RepositoryInDB
from gitential2.datatypes.projects import ProjectInDB
from gitential2.datatypes.workspaces import WorkspaceCreate, WorkspaceInDB
from gitential2.datatypes.project_repositories import ProjectRepositoryInDB

logger = get_logger(__name__)


def import_project_and_repos(
    g: GitentialContext,
    workspace_id: int,
    legacy_projects_repos: List[dict],
    legacy_account_repos: List[dict],
    legacy_projects: List[dict],
):
    g.backend.initialize_workspace(workspace_id)
    for project in legacy_projects:
        _import_project(g, project, workspace_id)
    for project_repo in legacy_projects_repos:
        if not g.backend.projects.get(workspace_id, id_=project_repo["project"]["id"]):
            _import_project(g, project_repo["project"], workspace_id)
        if not g.backend.repositories.get(workspace_id, id_=project_repo["repo"]["id"]):
            _import_repo(g, project_repo, workspace_id)
        _create_project_repo(g, project_repo, workspace_id=workspace_id)
    for account_repo in legacy_account_repos:
        if not g.backend.repositories.get(workspace_id, id_=account_repo["repo"]["id"]):
            _import_repo_from_account_repo(g, account_repo, workspace_id)
    g.backend.projects.reset_primary_key_id(workspace_id=workspace_id)
    g.backend.repositories.reset_primary_key_id(workspace_id=workspace_id)
    g.backend.project_repositories.reset_primary_key_id(workspace_id=workspace_id)


def get_repo_name(input_str: str) -> str:
    repo_name = input_str.split("/")[-1]
    if repo_name.endswith(".git"):
        return repo_name[0:-4]
    else:
        return repo_name

    # proto = input_str.split("://")[0]
    # if proto == "ssh":
    #     return input_str.split(":")[1].split("/")[1].split(".")[0]
    # elif proto == "https" and "visualst" in input_str:
    #     return input_str.split("/")[-1]
    # elif proto == "https":
    #     return input_str.split("/")[-2]
    # elif ".git" in input_str:
    #     return "ssh"
    # else:
    #     print("notimplemented repo name gather", input_str)
    #     sys.exit(1)


def get_namespace(input_str: str) -> str:
    proto = input_str.split("://")[0]
    if proto == "ssh":
        return input_str.split(":")[1].split("/")[0]
    elif proto == "https" and "visualst" in input_str:
        return input_str.split("/")[-3]
    elif proto == "https":
        return input_str.split("/")[-2]
    elif (
        "git@github.com" in input_str or "git@gitlab.com" in input_str or ("git@" in input_str and ".git" in input_str)
    ):
        return input_str.split(":")[1].split("/")[0]
    else:
        print("notimplemented namespace gather", input_str)
        sys.exit(1)


def get_integration_type(input_str: str) -> Optional[str]:
    if "bitb" in input_str:
        return "bitbucket"
    elif "github" in input_str:
        return "github"
    elif "gitlab" in input_str:
        return "gitlab"
    elif "visuals" in input_str:
        return "vsts"
    else:
        print("notimplemented integration type", input_str)
        # sys.exit(1)
        return None


def get_clone_protocol(input_str: str) -> str:
    if "://" in input_str:
        tmp = input_str.split("://")[0]
    else:
        tmp = "ssh"
    return tmp


def _import_repo(g: GitentialContext, project_repo: dict, workspace_id: int):
    repo = project_repo["repo"]
    credential_id = project_repo["secret_id"][0] if project_repo.get("secret_id", []) else None
    try:
        repo_create = RepositoryInDB(
            id=repo["id"],
            clone_url=repo["clone_url"],
            protocol=get_clone_protocol(repo["clone_url"]),
            name=get_repo_name(repo["clone_url"]),
            namespace=get_namespace(repo["clone_url"]),
            private=repo["private"] if repo["private"] is not None else False,
            integration_type=get_integration_type(repo["clone_url"])
            if get_clone_protocol(repo["clone_url"]) != "ssh"
            else None,
            integration_name=get_integration_type(repo["clone_url"])
            if get_clone_protocol(repo["clone_url"]) != "ssh"
            else None,
            credential_id=credential_id if get_clone_protocol(repo["clone_url"]) == "ssh" else None,
            created_at=repo["created_at"],
            updated_at=repo["updated_at"],
        )
        logger.info("Importing repo", workspace_id=workspace_id)
        g.backend.repositories.insert(workspace_id, repo["id"], repo_create)
    except ValidationError as e:
        print(f"Failed to import repo {repo['clone_url']}", e)
        sys.exit(1)


def _import_repo_from_account_repo(g: GitentialContext, account_repo: dict, workspace_id: int):
    repo = account_repo["repo"]
    credential_id = account_repo["secret"]["id"] if account_repo.get("secret", {}) else None
    try:
        repo_create = RepositoryInDB(
            id=repo["id"],
            clone_url=repo["clone_url"],
            protocol=get_clone_protocol(repo["clone_url"]),
            name=get_repo_name(repo["clone_url"]),
            namespace=get_namespace(repo["clone_url"]),
            private=repo["private"] if repo["private"] is not None else False,
            integration_type=get_integration_type(repo["clone_url"])
            if get_clone_protocol(repo["clone_url"]) != "ssh"
            else None,
            integration_name=get_integration_type(repo["clone_url"])
            if get_clone_protocol(repo["clone_url"]) != "ssh"
            else None,
            credential_id=credential_id if get_clone_protocol(repo["clone_url"]) == "ssh" else None,
            created_at=repo["created_at"],
            updated_at=repo["updated_at"],
        )
        logger.info("Importing repo", workspace_id=workspace_id)
        g.backend.repositories.insert(workspace_id, repo["id"], repo_create)
    except ValidationError as e:
        print(f"Failed to import repo {repo['clone_url']}", e)
        sys.exit(1)


def _import_project(g: GitentialContext, project: dict, workspace_id: int):
    try:
        project_create = ProjectInDB(
            id=project["id"],
            name=project["name"],
            shareable=project["shareable"],
            pattern=project["pattern"] if project["pattern"] else None,
            created_at=project["created_at"],
            updated_at=project["updated_at"],
            extra=None,
        )
        logger.info("Importing project", workspace_id=workspace_id)
        g.backend.projects.insert(workspace_id, project["id"], project_create)
    except ValidationError as e:
        print(f"Failed to import project {project['name']}", e)


def _create_project_repo(g: GitentialContext, project_repo, workspace_id: int):
    try:
        project_repo_create = ProjectRepositoryInDB(
            id=project_repo["id"],
            project_id=project_repo["project"]["id"],
            repo_id=project_repo["repo"]["id"],
        )
        logger.info("Importing project repo", workspace_id=workspace_id)
        g.backend.project_repositories.insert(workspace_id, project_repo["id"], project_repo_create)
    except ValidationError as e:
        print(f"Failed to import project repo {e}")


def _import_workspace(g: GitentialContext, workspace: dict) -> Optional[WorkspaceInDB]:
    try:
        workspace_create = WorkspaceCreate(
            name=workspace["name"],
            created_by=workspace["owner"]["id"],
            created_at=workspace["created_at"],
            updated_at=workspace["updated_at"],
        )
        logger.info("Importing workspace", workspace_id=workspace["id"])
        return g.backend.workspaces.create(workspace_create)
    except ValidationError as e:
        print(f"Failed to import workspace {workspace['id']}", e)
        return None
