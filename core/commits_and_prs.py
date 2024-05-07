from datetime import datetime
from typing import Optional, List, Tuple
from gitential2.datatypes.calculated import CalculatedCommit, CalculatedCommitId, CalculatedPatch
from gitential2.datatypes.pull_requests import PullRequest

from .context import GitentialContext


# pylint: disable=too-many-arguments
def get_commits(
    g: GitentialContext,
    workspace_id: int,
    project_id: Optional[int] = None,
    team_id: Optional[int] = None,
    repo_ids: Optional[List[int]] = None,
    author_ids: Optional[List[int]] = None,
    developer_id: Optional[int] = None,
    from_: Optional[datetime] = None,
    to_: Optional[datetime] = None,
    is_merge: Optional[bool] = None,
    keywords: Optional[List[str]] = None,
    limit: int = 100,
    offset: int = 0,
) -> Tuple[int, List[CalculatedCommit]]:

    # If no repo is sent as param, all repos of the projects will be considered
    if project_id and not repo_ids:
        repo_ids = g.backend.project_repositories.get_repo_ids_for_project(
            workspace_id=workspace_id, project_id=project_id
        )
    if team_id:
        author_ids = g.backend.team_members.get_team_member_author_ids(workspace_id, team_id)
    if developer_id:
        author_ids = [developer_id]

    total_number_of_commits: int = g.backend.calculated_commits.count(
        workspace_id=workspace_id,
        repository_ids=repo_ids,
        author_ids=author_ids,
        from_=from_,
        to_=to_,
        is_merge=is_merge,
        keywords=keywords,
    )
    commits_list = list(
        g.backend.calculated_commits.select(
            workspace_id=workspace_id,
            repository_ids=repo_ids,
            author_ids=author_ids,
            from_=from_,
            to_=to_,
            is_merge=is_merge,
            keywords=keywords,
            limit=limit,
            offset=offset,
        )
    )

    return total_number_of_commits, commits_list


def get_patches_for_commit(
    g: GitentialContext, workspace_id: int, repo_id: int, commit_hash: str
) -> List[CalculatedPatch]:
    return g.backend.calculated_patches.get_all_for_commit(
        workspace_id=workspace_id, commit_id=CalculatedCommitId(repo_id=repo_id, commit_id=commit_hash)
    )


# def _get_author_id_for_email(g: GitentialContext, workspace_id: int, email: str) -> Optional[int]:
#     for author in g.backend.authors.all(workspace_id):
#         if email in author.all_emails:
#             return author.id
#     return None


def get_pull_requests(
    g: GitentialContext,
    workspace_id: int,
    project_id: Optional[int] = None,
    team_id: Optional[int] = None,
    repo_ids: Optional[List[int]] = None,
    developer_ids: Optional[List[int]] = None,
    developer_id: Optional[int] = None,
    from_: Optional[datetime] = None,
    to_: Optional[datetime] = None,
    limit: int = 100,
    offset: int = 0,
) -> Tuple[int, List[PullRequest]]:

    # If no repo is sent as param, all repos of the projects will be considered
    if project_id and not repo_ids:
        repo_ids = g.backend.project_repositories.get_repo_ids_for_project(
            workspace_id=workspace_id, project_id=project_id
        )
    if team_id:
        developer_ids = g.backend.team_members.get_team_member_author_ids(workspace_id, team_id)
    if developer_id:
        developer_ids = [developer_id]

    prs_count = g.backend.pull_requests.count(
        workspace_id=workspace_id,
        repository_ids=repo_ids,
        developer_ids=developer_ids,
        from_=from_,
        to_=to_,
    )

    prs = g.backend.pull_requests.select(
        workspace_id=workspace_id,
        repository_ids=repo_ids,
        developer_ids=developer_ids,
        from_=from_,
        to_=to_,
        limit=limit,
        offset=offset,
    )

    return prs_count, list(prs)
