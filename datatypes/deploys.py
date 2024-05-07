from datetime import datetime
from typing import Optional, List, Tuple

from .common import CoreModel, ExtraFieldMixin, StringIdModelMixin
from .export import ExportableModel


class DeployedPullRequest(StringIdModelMixin, ExtraFieldMixin, CoreModel):
    repository_name: Optional[str]
    title: Optional[str]
    created_at: datetime
    merged_at: Optional[datetime]


class DeployedCommit(CoreModel):
    repository_name: str
    repository_namespace: Optional[str]
    repository_url: Optional[str]
    commit_id: Optional[str]
    git_ref: Optional[str]


class DeployedIssue(CoreModel):
    repository_name: str
    issue_id: str


class DeployedCommitId(CoreModel):
    repo_id: int
    commit_id: str


class DeployCommit(StringIdModelMixin, CoreModel, ExportableModel):
    deploy_id: str
    environment: str
    repo_id: int
    repository_name: str
    commit_id: str
    deployed_at: datetime
    authored_at: datetime
    author_id: int

    def export_names(self) -> Tuple[str, str]:
        return ("deploy_commit", "deploy_commits")

    def export_fields(self) -> List[str]:
        return [
            "id",
            "deploy_id",
            "environment",
            "repo_id",
            "repository_name",
            "commit_id",
            "deployed_at",
            "authored_at",
            "author_id",
        ]


class Deploy(StringIdModelMixin, ExtraFieldMixin, CoreModel, ExportableModel):
    environments: List[str]
    pull_requests: Optional[List[DeployedPullRequest]]
    commits: List[DeployedCommit]
    issues: Optional[List[DeployedIssue]]
    deployed_at: datetime

    def export_names(self) -> Tuple[str, str]:
        return ("deploy", "deploys")

    def export_fields(self) -> List[str]:
        return [
            "id",
            "environments",
            "pull_requests",
            "commits",
            "issues",
            "deployed_at",
            "extra",
        ]
