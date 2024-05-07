from typing import List, Optional, cast
import random
import string
from structlog import get_logger
from gitential2.datatypes.workspaces import WorkspaceInDB
from gitential2.datatypes.users import UserInDB
from gitential2.datatypes.workspacemember import WorkspaceMemberInDB, WorkspaceMemberCreate, WorkspaceRole
from gitential2.datatypes.workspace_invitations import (
    WorkspaceInvitationCreate,
    WorkspaceInvitationInDB,
    WorkspaceInvitationUpdate,
    InvitationStatus,
)
from .workspaces import get_workspace_owner
from .emails import send_email_to_address
from .context import GitentialContext

logger = get_logger(__name__)
Email = str


def list_pending_invitations(g: GitentialContext, workspace_id: int) -> List[WorkspaceInvitationInDB]:
    return [
        invitation
        for invitation in g.backend.workspace_invitations.get_invitations_for_workspace(workspace_id)
        if invitation.status == InvitationStatus.pending
    ]


def invite_to_workspace(
    g: GitentialContext,
    workspace_id: int,
    invitations: List[Email],
    invitation_by: Optional[UserInDB] = None,
) -> List[WorkspaceInvitationInDB]:

    workspace = g.backend.workspaces.get_or_error(workspace_id)
    if invitation_by is None:
        invitation_by = get_workspace_owner(g, workspace.id)

    ret = []

    existing_invitations = {invitation.email: invitation for invitation in list_pending_invitations(g, workspace.id)}
    for email in invitations:
        if email in existing_invitations:
            logger.info(
                "Already invited, skipping", email=email, invitation=existing_invitations[email], workspace=workspace
            )
            ret.append(existing_invitations[email])
        else:
            invitation = _create_invitation(g, workspace_id, email, cast(UserInDB, invitation_by))
            ret.append(invitation)
            logger.info("Invitation to workspace created", email=email, invitation=invitation, workspace=workspace)
            send_invitation_email(g, invitation)

    return []


def _create_invitation(
    g: GitentialContext, workspace_id: int, email: str, invitation_by: UserInDB
) -> WorkspaceInvitationInDB:
    create = WorkspaceInvitationCreate(
        invitation_by=invitation_by.id,
        workspace_id=workspace_id,
        email=email,
        invitation_code=_generate_invitation_code(),
        status=InvitationStatus.pending,
    )
    return g.backend.workspace_invitations.create(create)


def send_invitation_email(g: GitentialContext, invitation: WorkspaceInvitationInDB):
    workspace = g.backend.workspaces.get_or_error(invitation.workspace_id)
    invitation_sender = g.backend.users.get_or_error(invitation.invitation_by) if invitation.invitation_by else None
    invitation_url = (
        g.settings.web.frontend_url.strip("/")
        + "/?invitation_code="
        + (invitation.invitation_code or "missing-invitation-code")
    )
    return send_email_to_address(
        g,
        email=invitation.email,
        template_name="invite_member",
        invitation_sender=invitation_sender,
        invitation_url=invitation_url,
        workspace=workspace,
        invitation=invitation,
    )


def _generate_invitation_code() -> str:
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(64))


def accept_invitation(
    g: GitentialContext, invitation_code: str, current_user: UserInDB
) -> Optional[WorkspaceMemberInDB]:

    invitation = g.backend.workspace_invitations.get_invitation_by_code(invitation_code)

    if not invitation:
        logger.info("Invitation not found", invitation_code=invitation_code, current_user=current_user)
        return None
    else:
        invitation = cast(WorkspaceInvitationInDB, invitation)

    workspace = g.backend.workspaces.get_or_error(invitation.workspace_id)
    existing_membership = g.backend.workspace_members.get_for_workspace_and_user(
        invitation.workspace_id, current_user.id
    )
    if invitation.status == InvitationStatus.accepted and existing_membership:
        return existing_membership
    elif invitation.status == InvitationStatus.accepted and not existing_membership:
        return _create_workspace_member(g, workspace, current_user)

    elif invitation.status == InvitationStatus.pending and existing_membership:
        _update_invitation_status_to_accepted(g, invitation)
        return existing_membership
    else:
        _update_invitation_status_to_accepted(g, invitation)
        return _create_workspace_member(g, workspace, current_user)


def _update_invitation_status_to_accepted(
    g: GitentialContext, invitation: WorkspaceInvitationInDB
) -> WorkspaceInvitationInDB:
    update = WorkspaceInvitationUpdate(**invitation.dict())
    update.status = InvitationStatus.accepted
    return g.backend.workspace_invitations.update(invitation.id, update)


def _create_workspace_member(g: GitentialContext, workspace: WorkspaceInDB, user: UserInDB) -> WorkspaceMemberInDB:
    return g.backend.workspace_members.create(
        WorkspaceMemberCreate(
            workspace_id=workspace.id,
            user_id=user.id,
            role=WorkspaceRole.collaborator,
            primary=False,
        )
    )


def delete_invitation(g: GitentialContext, workspace_id: int, invitation_id: int) -> int:
    invitation = g.backend.workspace_invitations.get(invitation_id)
    if invitation:
        if invitation.workspace_id == workspace_id:
            return g.backend.workspace_invitations.delete(invitation_id)

    return 0
