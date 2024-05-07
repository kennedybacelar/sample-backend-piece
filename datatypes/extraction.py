from enum import Enum
from typing import Optional, Tuple, List
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel
from gitential2.datatypes.common import CoreModel
from .export import ExportableModel


class LocalGitRepository(BaseModel):
    repo_id: Optional[int] = None
    directory: Path


class ExtractedKind(str, Enum):
    EXTRACTED_COMMIT = "extracted_commit"
    EXTRACTED_PATCH = "extracted_patch"
    EXTRACTED_PATCH_REWRITE = "extracted_patch_rewrite"
    EXTRACTED_COMMIT_BRANCH = "extracted_commit_branch"
    PULL_REQUEST = "pull_request"
    PULL_REQUEST_COMMIT = "pull_request_commit"
    PULL_REQUEST_COMMENT = "pull_request_comment"
    PULL_REQUEST_LABEL = "pull_request_label"
    # ITS
    ITS_ISSUE = "its_issue"
    ITS_ISSUE_CHANGE = "its_issue_change"
    ITS_ISSUE_TIME_IN_STATUS = "its_issue_time_in_status"
    ITS_ISSUE_COMMENT = "its_issue_comment"
    ITS_ISSUE_LINKED_ISSUE = "its_issue_linked_issue"
    ITS_SPRINT = "its_sprint"
    ITS_ISSUE_SPRINT = "its_issue_sprint"
    ITS_ISSUE_WORKLOG = "its_issue_worklog"


class ExtractedCommitId(CoreModel):
    repo_id: int
    commit_id: str


class ExtractedCommit(CoreModel, ExportableModel):
    repo_id: int
    commit_id: str
    atime: datetime
    aemail: str
    aname: str
    ctime: datetime
    cemail: str
    cname: str
    message: str
    nparents: int
    tree_id: str

    @property
    def id_(self):
        return ExtractedCommitId(repo_id=self.repo_id, commit_id=self.commit_id)

    def export_names(self) -> Tuple[str, str]:
        return ("extracted_commit", "extracted_commits")

    def export_fields(self) -> List[str]:
        return [
            "repo_id",
            "commit_id",
            "atime",
            "aemail",
            "aname",
            "ctime",
            "cemail",
            "cname",
            "message",
            "nparents",
            "tree_id",
        ]


class Langtype(Enum):
    UNKNOWN = 0
    PROGRAMMING = 1
    MARKUP = 2
    PROSE = 3
    DATA = 4


class ExtractedPatchId(CoreModel):
    repo_id: int
    commit_id: str
    parent_commit_id: str
    newpath: str


class ExtractedPatch(CoreModel, ExportableModel):
    repo_id: int
    commit_id: str
    parent_commit_id: str
    status: str
    newpath: str
    oldpath: str
    newsize: int
    oldsize: int
    is_binary: bool
    lang: str
    langtype: Langtype

    loc_i: int
    loc_d: int
    comp_i: int
    comp_d: int
    loc_i_std: float
    loc_d_std: float
    comp_i_std: float
    comp_d_std: float

    nhunks: int
    nrewrites: int
    rewrites_loc: int

    @property
    def id_(self):
        return ExtractedPatchId(
            repo_id=self.repo_id, commit_id=self.commit_id, parent_commit_id=self.parent_commit_id, newpath=self.newpath
        )

    def export_names(self) -> Tuple[str, str]:
        return ("extracted_patch", "extracted_patches")

    def export_fields(self) -> List[str]:
        return [
            "repo_id",
            "commit_id",
            "parent_commit_id",
            "status",
            "newpath",
            "oldpath",
            "newsize",
            "oldsize",
            "is_binary",
            "lang",
            "langtype",
            "loc_i",
            "loc_d",
            "comp_i",
            "comp_d",
            "loc_i_std",
            "loc_d_std",
            "comp_i_std",
            "comp_d_std",
            "nhunks",
            "nrewrites",
            "rewrites_loc",
        ]


class ExtractedPatchRewriteId(CoreModel):
    repo_id: int
    commit_id: str
    newpath: str
    rewritten_commit_id: str


class ExtractedPatchRewrite(CoreModel, ExportableModel):
    repo_id: int
    commit_id: str
    atime: datetime
    aemail: str
    newpath: str
    rewritten_commit_id: str
    rewritten_atime: datetime
    rewritten_aemail: str
    loc_d: int

    @property
    def id_(self):
        return ExtractedPatchRewriteId(
            repo_id=self.repo_id,
            commit_id=self.commit_id,
            newpath=self.newpath,
            rewritten_commit_id=self.rewritten_commit_id,
        )

    def export_names(self) -> Tuple[str, str]:
        return ("extracted_patch_rewrite", "extracted_patch_rewrites")

    def export_fields(self) -> List[str]:
        return [
            "repo_id",
            "commit_id",
            "atime",
            "aemail",
            "newpath",
            "rewritten_commit_id",
            "rewritten_atime",
            "rewritten_aemail",
            "loc_d",
        ]


class ExtractedCommitBranchId(CoreModel):
    repo_id: int
    commit_id: str
    branch: str


class ExtractedCommitBranch(CoreModel, ExportableModel):
    repo_id: int
    commit_id: str
    atime: datetime
    branch: str

    @property
    def id_(self):
        return ExtractedCommitBranchId(
            repo_id=self.repo_id,
            commit_id=self.commit_id,
            branch=self.branch,
        )

    def export_names(self) -> Tuple[str, str]:
        return ("extracted_commit_branch", "extracted_commit_branches")

    def export_fields(self) -> List[str]:
        return ["repo_id", "commit_id", "atime", "branch"]
