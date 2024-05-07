from enum import Enum

from pydantic import BaseModel


class RefreshStrategy(str, Enum):
    one_by_one = "one_by_one"
    parallel = "parallel"


class RefreshType(str, Enum):
    everything = "everything"
    commits_only = "commits_only"
    prs_only = "prs_only"
    commit_calculations_only = "commit_calculations_only"
    its_only = "its_only"


class RefreshWorkspaceParams(BaseModel):
    workspace_id: int
    strategy: RefreshStrategy = RefreshStrategy.parallel
    refresh_type: RefreshType = RefreshType.everything
    force: bool = False


class MaintainWorkspaceParams(BaseModel):
    workspace_id: int


class RefreshProjectParams(BaseModel):
    workspace_id: int
    project_id: int
    strategy: RefreshStrategy = RefreshStrategy.parallel
    refresh_type: RefreshType = RefreshType.everything
    force: bool = False


class RefreshRepositoryParams(BaseModel):
    workspace_id: int
    repository_id: int
    strategy: RefreshStrategy = RefreshStrategy.parallel
    refresh_type: RefreshType = RefreshType.everything
    force: bool = False


class RefreshITSProjectParams(BaseModel):
    workspace_id: int
    itsp_id: int
    strategy: RefreshStrategy = RefreshStrategy.parallel
    refresh_type: RefreshType = RefreshType.everything
    force: bool = False


class ExtractProjectBranchesParams(BaseModel):
    workspace_id: int
    project_id: int
    strategy: RefreshStrategy = RefreshStrategy.parallel


class ExtractRepositoryBranchesParams(BaseModel):
    workspace_id: int
    repository_id: int
