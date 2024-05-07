from typing import Optional

from gitential2.exceptions import PermissionException
from gitential2.datatypes.workspaces import WorkspaceCreate, WorkspaceInDB, WorkspaceDuplicate

from gitential2.datatypes.subscriptions import SubscriptionType
from gitential2.datatypes.users import UserInDB
from gitential2.datatypes.workspacemember import WorkspaceRole
from gitential2.datatypes.workspacemember import WorkspaceMemberCreate
from .permissions import check_is_admin

from .subscription import get_current_subscription
from .context import GitentialContext


def check_permission_for_workspace_creation(g: GitentialContext, current_user: UserInDB, primary=False):
    if not primary and g.license.is_cloud and not check_is_admin(g, current_user, False):
        sub = get_current_subscription(g, current_user.id)
        if sub.subscription_type == SubscriptionType.free:
            raise PermissionException("Users with FREE subscription can only have one workspace")


def create_workspace(
    g: GitentialContext,
    workspace: WorkspaceCreate,
    current_user: UserInDB,
    primary=False,
    is_permission_check_on: bool = True,
    workspace_duplicate: Optional[WorkspaceDuplicate] = None,
) -> WorkspaceInDB:
    if is_permission_check_on:
        check_permission_for_workspace_creation(g, current_user, primary)

    workspace.created_by = current_user.id
    workspace_in_db = g.backend.workspaces.create(workspace)
    g.backend.workspace_members.create(
        WorkspaceMemberCreate(
            workspace_id=workspace_in_db.id, user_id=current_user.id, role=WorkspaceRole.owner, primary=primary
        )
    )
    g.backend.initialize_workspace(workspace_id=workspace_in_db.id, workspace_duplicate=workspace_duplicate)
    return workspace_in_db


def duplicate_workspace(
    g: GitentialContext,
    workspace_duplicate: WorkspaceDuplicate,
    current_user: UserInDB,
    is_permission_check_on: bool = True,
) -> WorkspaceInDB:
    workspace_create = WorkspaceCreate(name=workspace_duplicate.name)
    created_workspace: WorkspaceInDB = create_workspace(
        g=g,
        workspace=workspace_create,
        workspace_duplicate=workspace_duplicate,
        current_user=current_user,
        is_permission_check_on=is_permission_check_on,
    )
    return created_workspace
