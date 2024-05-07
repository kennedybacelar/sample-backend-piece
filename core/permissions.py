from gitential2.datatypes.permissions import Entity, Action
from gitential2.datatypes.users import UserInDB
from gitential2.datatypes.workspacemember import WorkspaceRole
from gitential2.exceptions import PermissionException
from .context import GitentialContext
from .workspaces import get_accessible_workspaces
from ..license import is_on_prem_installation


def _return(has_permission: bool, raise_exc: bool = True):
    if raise_exc and not has_permission:
        raise PermissionException("Access Forbidden")
    return has_permission


def check_permission(
    g: GitentialContext,
    current_user: UserInDB,
    entity: Entity,
    action: Action,
    raise_exc=True,
    enable_when_on_prem=False,
    **kwargs,
) -> bool:
    if not current_user:
        return _return(False, raise_exc)
    if not current_user.is_active:
        return _return(False, raise_exc)
    if current_user.is_admin:
        return _return(True, raise_exc)
    if enable_when_on_prem and is_on_prem_installation():
        return _return(True, raise_exc)
    if "workspace_id" in kwargs:
        acessible_workspaces = get_accessible_workspaces(g, current_user=current_user)
        try:
            workspace = [ws for ws in acessible_workspaces if ws.id == kwargs["workspace_id"]][0]
            if workspace.membership:
                return _return(
                    workspace.membership.role == WorkspaceRole.owner
                    or (
                        workspace.membership.role == WorkspaceRole.collaborator
                        and (
                            entity
                            in [
                                Entity.project,
                                Entity.team,
                                Entity.repository,
                                Entity.dashboard,
                                Entity.chart,
                                Entity.thumbnail,
                            ]
                            or action in [Action.read]
                        )
                    ),
                    raise_exc,
                )
        except IndexError:
            return _return(False, raise_exc)

    return _return(True, raise_exc)


# pylint: disable=unused-argument
def check_is_admin(g: GitentialContext, current_user: UserInDB, raise_exc=True):
    if current_user and current_user.is_active and current_user.is_admin:
        return _return(True, raise_exc)
    return _return(False, raise_exc)
