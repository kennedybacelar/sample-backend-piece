from typing import Iterable, Optional, Tuple
from datetime import datetime
from structlog import get_logger

from gitential2.datatypes.deploys import Deploy, DeployCommit, DeployedCommit
from gitential2.datatypes.authors import AuthorAlias
from gitential2.datatypes.extraction import ExtractedCommitId

from .context import GitentialContext
from .authors import developer_map_callback

logger = get_logger(__name__)


def get_all_deploys(g: GitentialContext, workspace_id: int) -> Iterable[Deploy]:
    return g.backend.deploys.all(workspace_id)


def register_deploy(g: GitentialContext, workspace_id: int, deploy: Deploy) -> bool:
    g.backend.deploys.create_or_update(workspace_id=workspace_id, obj=deploy)
    return True


def delete_deploy_by_id(g: GitentialContext, workspace_id: int, deploy_id: str) -> bool:
    g.backend.deploys.delete_deploy_by_id(workspace_id=workspace_id, deploy_id=deploy_id)
    return _delete_deploy_commits_by_deploy_id(g=g, workspace_id=workspace_id, deploy_id=deploy_id)


def _delete_deploy_commits_by_deploy_id(g: GitentialContext, workspace_id: int, deploy_id: str) -> bool:
    g.backend.deploy_commits.delete_deploy_commits_by_deploy_id(workspace_id=workspace_id, deploy_id=deploy_id)
    return True


def recalculate_deploy_commits(g: GitentialContext, workspace_id: int):
    all_deploys = g.backend.deploys.all(workspace_id)
    for deploy in all_deploys:
        for environment in deploy.environments:
            for deployed_commit in deploy.commits:
                _create_or_update_deploy_commits(
                    g=g,
                    workspace_id=workspace_id,
                    deploy_obj=deploy,
                    environment=environment,
                    deployed_commit_obj=deployed_commit,
                )


def _get_repo_id_by_repo_name(
    g: GitentialContext, workspace_id: int, deployed_commit_obj: DeployedCommit
) -> Optional[int]:

    repo_name = deployed_commit_obj.repository_name

    list_of_repo_id_info_by_repo_name = g.backend.repositories.get_repo_id_info_by_repo_name(
        workspace_id=workspace_id, repo_name=repo_name
    )

    if len(list_of_repo_id_info_by_repo_name) == 1:
        repo_id, _, _ = list_of_repo_id_info_by_repo_name[0]
        return repo_id
    for repo_id_mapping_info in list_of_repo_id_info_by_repo_name:
        repo_id, repo_url, repo_namespace = repo_id_mapping_info
        if repo_namespace == deployed_commit_obj.repository_namespace:
            return repo_id
        elif repo_url == deployed_commit_obj.repository_url:
            return repo_id
    return None


def _lookup_commit_info(
    g: GitentialContext, workspace_id: int, deployed_commit_obj: DeployedCommit, repo_id_: int
) -> Optional[Tuple[datetime, Optional[int], str]]:

    reference_search_fields = ["commit_id", "git_ref"]

    for search_field in reference_search_fields:

        _commit_id = getattr(deployed_commit_obj, search_field)

        if _commit_id:
            _id = ExtractedCommitId(
                repo_id=repo_id_,
                commit_id=_commit_id,
            )
            extracted_commit = g.backend.extracted_commits.get(workspace_id=workspace_id, id_=_id)

            if extracted_commit:
                authored_at = extracted_commit.atime
                author_id = developer_map_callback(
                    alias=AuthorAlias(name=extracted_commit.aname, email=extracted_commit.aemail),
                    g=g,
                    workspace_id=workspace_id,
                )
                return authored_at, author_id, _commit_id
    return None


def _create_or_update_deploy_commits(
    g: GitentialContext, workspace_id: int, deploy_obj: Deploy, environment: str, deployed_commit_obj: DeployedCommit
) -> bool:

    repo_name = deployed_commit_obj.repository_name
    repo_id = _get_repo_id_by_repo_name(g=g, workspace_id=workspace_id, deployed_commit_obj=deployed_commit_obj)
    if not repo_id:
        return False

    commit_info = _lookup_commit_info(
        g=g, workspace_id=workspace_id, deployed_commit_obj=deployed_commit_obj, repo_id_=repo_id
    )
    if not commit_info:
        return False

    _authored_at, _author_id, _commit_id = commit_info

    deploy_commit = DeployCommit(
        id=f"{deploy_obj.id}&{environment}&{repo_id}",
        deploy_id=deploy_obj.id,
        environment=environment,
        repo_id=repo_id,
        repository_name=repo_name,
        commit_id=_commit_id,
        deployed_at=deploy_obj.deployed_at,
        authored_at=_authored_at,
        author_id=_author_id,
    )

    g.backend.deploy_commits.create_or_update(workspace_id=workspace_id, obj=deploy_commit)
    return True
