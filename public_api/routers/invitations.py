from typing import List
from fastapi import APIRouter, Depends
from structlog import get_logger

from gitential2.datatypes.permissions import Entity, Action
from gitential2.datatypes.workspace_invitations import WorkspaceInvitationInDB
from gitential2.datatypes.workspacemember import MemberInvite
from gitential2.core.context import GitentialContext
from gitential2.core.permissions import check_permission

from gitential2.core.workspace_invitations import (
    list_pending_invitations,
    accept_invitation,
    delete_invitation,
    invite_to_workspace,
)
from ..dependencies import current_user, gitential_context

logger = get_logger(__name__)

router = APIRouter(tags=["invitations"])


@router.get("/workspaces/{workspace_id}/invitations", response_model=List[WorkspaceInvitationInDB])
def list_workspace_invitations(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.invitation, Action.read, workspace_id=workspace_id)
    return list_pending_invitations(g, workspace_id=workspace_id)


@router.post("/workspaces/{workspace_id}/invitations")
def invite_to_workspace_(
    invitations: List[MemberInvite],
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.membership, Action.create, workspace_id=workspace_id)
    return invite_to_workspace(
        g, workspace_id=workspace_id, invitation_by=current_user, invitations=[inv.email for inv in invitations]
    )


@router.delete("/workspaces/{workspace_id}/invitations/{invitation_id}")
def delete_workspace_invitation_(
    workspace_id: int,
    invitation_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.invitation, Action.delete, workspace_id=workspace_id)
    delete_invitation(g, workspace_id, invitation_id)


@router.get("/invitations/{invitation_code}/accept")
def accept_invitation_(
    invitation_code: str,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.invitation, Action.update)
    return accept_invitation(g, invitation_code, current_user)
