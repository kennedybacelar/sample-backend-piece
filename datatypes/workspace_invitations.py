from typing import Optional
from enum import Enum
from .common import IDModelMixin, DateTimeModelMixin, CoreModel


class InvitationStatus(str, Enum):
    pending = "pending"
    accepted = "accepted"


class WorkspaceInvitationBase(CoreModel):
    invitation_by: Optional[int] = None
    workspace_id: int
    email: str
    invitation_code: Optional[str] = None
    status: InvitationStatus = InvitationStatus.pending


class WorkspaceInvitationCreate(WorkspaceInvitationBase):
    pass


class WorkspaceInvitationUpdate(WorkspaceInvitationBase):
    pass


class WorkspaceInvitationInDB(IDModelMixin, DateTimeModelMixin, WorkspaceInvitationBase):
    pass
