from typing import Optional, Tuple, List
from datetime import datetime
from enum import Enum

from gitential2.datatypes.export import ExportableModel
from .common import CoreModel, DateTimeModelMixin, ExtraFieldMixin


class PullRequestState(str, Enum):
    open = "open"
    merged = "merged"
    closed = "closed"

    @classmethod
    def from_gitlab(cls, state):
        if state == "merged":
            return cls.merged
        elif state in ["opened", "locked"]:
            return cls.open
        elif state == "closed":
            return cls.closed
        else:
            raise ValueError("invalid state for MR")

    @classmethod
    def from_github(cls, state, merged_at):
        if merged_at and state == "closed":
            return cls.merged
        elif state in ["opened", "locked", "open"]:
            return cls.open
        elif state == "closed":
            return cls.closed
        else:
            raise ValueError(f'invalid state for Github PR "{state}"')

    @classmethod
    def from_bitbucket(cls, state):
        if state == "OPEN":
            return cls.open
        elif state == "MERGED":
            return cls.merged
        elif state == "SUPERSEDED":
            return cls.closed
        elif state == "DECLINED":
            return cls.closed
        else:
            raise ValueError(f'invalid state for BitBucket PR "{state}"')

    @classmethod
    def from_vsts(cls, state):
        if state == "abandoned":
            return cls.closed
        elif state == "active":
            return cls.open
        elif state == "completed":
            return cls.merged
        else:
            raise ValueError(f'invalid state for VSTS PR "{state}"')


class PullRequestId(CoreModel):
    repo_id: int
    number: int


class PullRequest(ExtraFieldMixin, CoreModel, ExportableModel):
    repo_id: int
    number: int
    title: str
    platform: str
    id_platform: int
    api_resource_uri: str
    state_platform: str
    state: str
    created_at: datetime
    closed_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    merged_at: Optional[datetime] = None
    additions: int
    deletions: int
    changed_files: int
    draft: bool
    user: str  # NOT USED

    user_id_external: Optional[str] = None
    user_name_external: Optional[str] = None
    user_username_external: Optional[str] = None
    user_aid: Optional[int] = None

    commits: int
    merged_by: Optional[str] = None  # NOT USED

    merged_by_id_external: Optional[str] = None
    merged_by_name_external: Optional[str] = None
    merged_by_username_external: Optional[str] = None
    merged_by_aid: Optional[int] = None

    first_reaction_at: Optional[datetime] = None
    first_commit_authored_at: Optional[datetime] = None

    is_bugfix: Optional[bool] = None

    @property
    def id_(self):
        return PullRequestId(repo_id=self.repo_id, number=self.number)

    def export_names(self) -> Tuple[str, str]:
        return ("pull_request", "pull_requests")

    def export_fields(self) -> List[str]:
        return [
            "extra",
            "repo_id",
            "number",
            "title",
            "platform",
            "id_platform",
            "api_resource_uri",
            "state_platform",
            "state",
            "created_at",
            "closed_at",
            "updated_at",
            "merged_at",
            "additions",
            "deletions",
            "changed_files",
            "draft",
            "user",
            "user_id_external",
            "user_name_external",
            "user_username_external",
            "user_aid",
            "commits",
            "merged_by",
            "merged_by_id_external",
            "merged_by_name_external",
            "merged_by_username_external",
            "merged_by_aid",
            "first_reaction_at",
            "first_commit_authored_at",
            "is_bugfix",
        ]


class PullRequestCommitId(CoreModel):
    repo_id: int
    pr_number: int
    commit_id: str


class PullRequestCommit(ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    repo_id: int
    pr_number: int
    commit_id: str
    author_name: str
    author_email: str
    author_date: datetime
    author_login: Optional[str] = None
    committer_name: str
    committer_email: str
    committer_date: datetime
    committer_login: Optional[str] = None

    @property
    def id_(self):
        return PullRequestCommitId(repo_id=self.repo_id, pr_number=self.pr_number, commit_id=self.commit_id)

    def export_names(self) -> Tuple[str, str]:
        return ("pull_request_commit", "pull_request_commits")

    def export_fields(self) -> List[str]:
        return [
            "author_date",
            "repo_id",
            "updated_at",
            "author_name",
            "committer_name",
            "author_email",
            "committer_date",
            "commit_id",
            "committer_login",
            "author_login",
            "committer_email",
            "pr_number",
            "extra",
            "created_at",
        ]


class PullRequestCommentId(CoreModel):
    repo_id: int
    pr_number: int
    comment_type: str
    comment_id: str


class PullRequestComment(ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    repo_id: int
    pr_number: int
    comment_type: str
    comment_id: str

    author_id_external: Optional[str] = None
    author_name_external: Optional[str] = None
    author_username_external: Optional[str] = None
    author_aid: Optional[int] = None

    published_at: Optional[datetime] = None

    content: str
    parent_comment_id: Optional[str] = None
    thread_id: Optional[str] = None
    review_id: Optional[str] = None

    @property
    def id_(self):
        return PullRequestCommentId(
            repo_id=self.repo_id, pr_number=self.pr_number, comment_type=self.comment_type, comment_id=self.comment_id
        )

    def export_names(self) -> Tuple[str, str]:
        return ("pull_request_comment", "pull_request_comments")

    def export_fields(self) -> List[str]:
        return [
            "created_at",
            "updated_at",
            "extra",
            "repo_id",
            "pr_number",
            "comment_type",
            "comment_id",
            "author_id_external",
            "author_name_external",
            "author_username_external",
            "author_aid",
            "published_at",
            "content",
            "parent_comment_id",
            "thread_id",
            "review_id",
        ]


class PullRequestLabelId(CoreModel):
    repo_id: int
    pr_number: int
    name: str


class PullRequestLabel(ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    repo_id: int
    pr_number: int
    name: str
    color: Optional[str] = None
    description: Optional[str] = None
    active: bool = True

    @property
    def id_(self):
        return PullRequestLabelId(repo_id=self.repo_id, pr_number=self.pr_number, name=self.name)

    def export_names(self) -> Tuple[str, str]:
        return ("pull_request_label", "pull_request_labels")

    def export_fields(self) -> List[str]:
        return ["repo_id", "pr_number", "name", "color", "description", "active"]


class PullRequestData(CoreModel):
    pr: PullRequest
    comments: List[PullRequestComment]
    commits: List[PullRequestCommit]
    labels: List[PullRequestLabel]
