from typing import Optional, List
from gitential2.datatypes.workspacemember import WorkspaceMemberPublic

from .common import CoreModel, IDModelMixin, DateTimeModelMixin, ExtraFieldMixin


class WorkspaceBase(ExtraFieldMixin, CoreModel):
    name: Optional[str] = None
    created_by: Optional[int] = None


class WorkspaceCreate(WorkspaceBase):
    name: str


class WorkspaceUpdate(WorkspaceBase):
    pass


class WorkspaceInDB(IDModelMixin, DateTimeModelMixin, WorkspaceBase):
    name: str
    created_by: int


class WorkspacePublic(IDModelMixin, DateTimeModelMixin, WorkspaceBase):
    membership: Optional[WorkspaceMemberPublic] = None
    members: Optional[List[WorkspaceMemberPublic]] = None


class WorkspaceDuplicate(WorkspaceCreate):
    id_of_workspace_to_be_duplicated: int
