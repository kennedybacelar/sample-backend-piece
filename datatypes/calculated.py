import math

from typing import Optional, Tuple, List
from datetime import datetime
from pydantic import validator

from .export import ExportableModel
from .common import CoreModel
from .extraction import Langtype


class CalculatedCommitId(CoreModel):
    repo_id: int
    commit_id: str


class CalculatedCommit(CoreModel, ExportableModel):
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

    # additional column - date
    date: datetime
    age: Optional[int]
    # author ids
    aid: int
    cid: int
    # is_merge
    is_merge: bool

    # number of patches
    nfiles: Optional[int]

    # calculated from patch, outlier
    loc_i_c: Optional[int]
    loc_i_inlier: Optional[int]
    loc_i_outlier: Optional[int]
    loc_d_c: Optional[int]
    loc_d_inlier: Optional[int]
    loc_d_outlier: Optional[int]
    comp_i_c: Optional[int]
    comp_i_inlier: Optional[int]
    comp_i_outlier: Optional[int]
    comp_d_c: Optional[int]
    comp_d_inlier: Optional[int]
    comp_d_outlier: Optional[int]
    loc_effort_c: Optional[int]
    uploc_c: Optional[int]

    # work hour estimation
    hours_measured: Optional[float]
    hours_estimated: Optional[float]
    hours: Optional[float]

    # velocity
    velocity_measured: Optional[float]
    velocity: Optional[float]

    is_bugfix: Optional[bool]

    is_pr_exists: Optional[bool]
    is_pr_open: Optional[bool]
    is_pr_closed: Optional[bool]

    @property
    def id_(self):
        return CalculatedCommitId(repo_id=self.repo_id, commit_id=self.commit_id)

    @validator("velocity_measured", "velocity", pre=True)
    def default_calc(cls, value: Optional[float]) -> Optional[float]:
        if value:
            if math.isinf(value):
                return 0
        return value

    def export_names(self) -> Tuple[str, str]:
        return ("calculated_commit", "calculated_commits")

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
            "date",
            "age",
            "aid",
            "cid",
            "is_merge",
            "nfiles",
            "loc_i_c",
            "loc_i_inlier",
            "loc_i_outlier",
            "loc_d_c",
            "loc_d_inlier",
            "loc_d_outlier",
            "comp_i_c",
            "comp_i_inlier",
            "comp_i_outlier",
            "comp_d_c",
            "comp_d_inlier",
            "comp_d_outlier",
            "loc_effort_c",
            "uploc_c",
            "hours_measured",
            "hours_estimated",
            "hours",
            "velocity_measured",
            "velocity",
            "is_bugfix",
            "is_pr_exists",
            "is_pr_open",
            "is_pr_closed",
        ]


class CalculatedPatchId(CoreModel):
    repo_id: int
    commit_id: str
    parent_commit_id: str
    newpath: str


class CalculatedPatch(CoreModel, ExportableModel):
    repo_id: int
    commit_id: str
    parent_commit_id: str
    # author ids
    aid: int
    cid: int

    # author time
    date: datetime

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

    nhunks: int
    nrewrites: int
    rewrites_loc: int

    # calculated
    is_merge: bool
    is_test: bool
    uploc: Optional[int]
    outlier: int
    anomaly: int

    is_collaboration: Optional[bool]
    is_new_code: Optional[bool]

    loc_effort_p: Optional[int]
    is_bugfix: Optional[bool]

    @property
    def id_(self):
        return CalculatedPatchId(
            repo_id=self.repo_id, commit_id=self.commit_id, parent_commit_id=self.parent_commit_id, newpath=self.newpath
        )

    def export_names(self) -> Tuple[str, str]:
        return "calculated_patch", "calculated_patches"

    def export_fields(self) -> List[str]:
        return [
            "repo_id",
            "commit_id",
            "parent_commit_id",
            "aid",
            "cid",
            "date",
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
            "nhunks",
            "nrewrites",
            "rewrites_loc",
            "is_merge",
            "is_test",
            "uploc",
            "outlier",
            "anomaly",
            "is_collaboration",
            "is_new_code",
            "loc_effort_p",
            "is_bugfix",
        ]
