from typing import List
from structlog import get_logger
from gitential2.datatypes.workspaces import WorkspaceInDB
from gitential2.datatypes.workspacemember import WorkspaceMemberCreate, WorkspaceRole
from gitential2.core import GitentialContext


logger = get_logger(__name__)


def import_legacy_workspaces(g: GitentialContext, legacy_workspaces: List[dict], legacy_workspace_members: List[dict]):
    user_login_id_map = {user.login: user.id for user in g.backend.users.all()}
    for legacy_workspace in legacy_workspaces:
        members = [member for member in legacy_workspace_members if member["account"]["id"] == legacy_workspace["id"]]
        _import_legacy_workspace(g, legacy_workspace, members, user_login_id_map)
    g.backend.workspaces.reset_primary_key_id()


def _import_legacy_workspace(g: GitentialContext, legacy_ws: dict, members: List[dict], user_login_id_map: dict):
    def _find_owner_from_members(members: List[dict]):
        for m in members:
            if m["role"] == 1:
                return m["user"]["id"]
        return None

    created_by = (
        legacy_ws.get("owner_id") or _find_owner_from_members(members) or user_login_id_map.get(legacy_ws["name"])
    )

    if created_by:
        workspace_create = WorkspaceInDB(
            id=legacy_ws["id"],
            name=legacy_ws["name"],
            created_by=created_by,
            created_at=legacy_ws["created_at"],
            updated_at=legacy_ws["updated_at"],
        )
        logger.info(
            "Importing workspace",
            name=legacy_ws["name"],
            created_by=created_by,
            id=legacy_ws["id"],
            collab_count=len(members),
        )

        g.backend.workspaces.insert(legacy_ws["id"], workspace_create)
        for m in members:
            workspace_member_create = WorkspaceMemberCreate(
                user_id=m["user"]["id"],
                workspace_id=m["account"]["id"],
                primary=bool(m["primary"]),
                role=WorkspaceRole.owner if m["role"] == 1 else WorkspaceRole.collaborator,
            )
            g.backend.workspace_members.create(workspace_member_create)
    else:
        logger.warn("skipping workspace", workspace=legacy_ws, members=members)
