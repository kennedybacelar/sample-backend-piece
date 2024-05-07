from enum import Enum
from typing import Optional
from .common import IDModelMixin, DateTimeModelMixin, CoreModel
from .users import UserHeader, UserCreate


class MemberInvite(CoreModel):
    email: str

    def user_create(self) -> UserCreate:
        return UserCreate(
            login=self.email.split("@")[0],
            email=self.email,
        )


class WorkspaceRole(Enum):
    owner = 1
    collaborator = 2


class WorkspaceMemberBase(CoreModel):
    user_id: Optional[int] = None
    workspace_id: Optional[int] = None
    role: WorkspaceRole = WorkspaceRole.owner
    primary: bool = False


class WorkspaceMemberCreate(WorkspaceMemberBase):
    user_id: int
    workspace_id: int


class WorkspaceMemberCreateWithEmail(WorkspaceMemberBase):
    email: str
    workspace_id: int


class WorkspaceMemberUpdate(WorkspaceMemberBase):
    pass


class WorkspaceMemberInDB(IDModelMixin, DateTimeModelMixin, WorkspaceMemberBase):
    user_id: int
    workspace_id: int


class WorkspaceMemberPublic(IDModelMixin, DateTimeModelMixin, WorkspaceMemberBase):
    user: Optional[UserHeader]


# class WorkspaceWithPermission(CoreModel):
#     id: int
#     name: str
#     role: WorkspaceRole
#     primary: bool
#     user_id: int
