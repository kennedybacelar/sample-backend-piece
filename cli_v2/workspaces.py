from datetime import datetime
from typing import Optional, List

import typer
from structlog import get_logger

from gitential2.core.users import get_user
from .common import get_context
from ..backends.sql.cleanup import perform_data_cleanup
from ..core.api_keys import delete_api_keys_for_workspace
from ..core.workspace_common import duplicate_workspace
from ..datatypes import UserInDB, WorkspaceMemberInDB
from ..datatypes.cli_v2 import ResetType, CleanupType
from ..datatypes.workspaces import WorkspaceDuplicate
from ..exceptions import SettingsException

app = typer.Typer()
logger = get_logger(__name__)


@app.command("reset")
def reset_workspace(workspace_id: int, reset_type: ResetType = typer.Option("full", "--type", "-t")):
    """
    DANGER ZONE!!! Workspace reset!
    \n
    By running this command, you can reset a workspace to its original state when it was created.
    It will truncate all the tables in the databases' workspace schema by running the following command
    template for all tables:
    \b
    'TRUNCATE TABLE <schema_name>.<table_name> RESTART IDENTITY CASCADE;'
    """

    g = get_context()
    workspace = g.backend.workspaces.get(id_=workspace_id) if workspace_id else None

    confirm_res = typer.confirm("Are you really sure you want to reset the workspace?")
    if confirm_res:
        if workspace:
            if reset_type in (ResetType.full, ResetType.sql_only):
                logger.info("Starting to truncate all of the tables for workspace!", workspace_id=workspace.id)
                g.backend.reset_workspace(workspace_id=workspace_id)
            if reset_type in (ResetType.full, ResetType.redis_only):
                logger.info("Starting to remove all data from Redis related to workspace!", workspace_id=workspace.id)
                g.kvstore.delete_values_for_workspace(workspace_id=workspace_id)
        else:
            logger.exception("Failed to reset workspace! Workspace not found by the provided workspace id!")


@app.command("duplicate")
def duplicate_workspace_(source_workspace_id: int, user_id: int, new_workspace_name: str):
    """
    With this command you can duplicate a workspace.

    \b
    You need to provide three arguments:
    SOURCE_WORKSPACE_ID: The id of the workspace you want to duplicate.
    USER_ID: The name of the duplicated workspace. It can not be an already existing workspace name.
    NEW_WORKSPACE_NAME: The id of the user
    """

    g = get_context()
    workspace_duplicate = WorkspaceDuplicate(
        id_of_workspace_to_be_duplicated=source_workspace_id, name=new_workspace_name
    )
    user: Optional[UserInDB] = get_user(g, user_id)

    all_workspace_names = [workspace.name for workspace in list(g.backend.workspaces.all())]
    if new_workspace_name in all_workspace_names:
        raise SettingsException("Can not duplicate workspace! Workspace name already exists!")
    if not user:
        raise SettingsException("Can not duplicate workspace! Wrong user id! User not exists!")

    duplicate_workspace(g=g, workspace_duplicate=workspace_duplicate, current_user=user, is_permission_check_on=False)


@app.command("delete-api-keys-for-workspace")
def delete_keys_for_workspace(workspace_id: int):
    g = get_context()
    delete_api_keys_for_workspace(g, workspace_id)


@app.command("purge")
def purge_workspace(workspace_id: int):
    """
    DANGER ZONE!!! Workspace purge! This command will purge everything about a specific workspace.

    \b
    WARNING!
    Primary workspaces can not be deleted!

    \b
    This command will delete the following:
       - rows from workspace_members PostgreSQL table related to workspace
       - rows from workspace_api_keys PostgreSQL table related to workspace
       - rows from workspace_invitations PostgreSQL table related to workspace
       - rows from schema_revisions table related to workspace
       - rows from workspaces PostgreSQL table related to workspace
       - workspace PostgreSQL schema (ws_<wid>)
       - keys from Redis related to workspace
    \b
    You need to provide only one argument:
    WORKSPACE_ID: The id of the workspace you want to delete
    """

    g = get_context()
    workspace = g.backend.workspaces.get(id_=workspace_id) if workspace_id else None
    workspace_members: List[WorkspaceMemberInDB] = g.backend.workspace_members.get_for_workspace(
        workspace_id=workspace_id
    )

    confirm_res = typer.confirm("Are you really sure you want to purge the workspace?")
    if confirm_res:
        if workspace:
            is_workspace_not_primary: bool = all(not member.primary for member in workspace_members)
            if is_workspace_not_primary:
                logger.info("Starting to purge workspace!", workspace_id=workspace.id)
                g.backend.delete_workspace_sql(workspace_id)
                g.kvstore.delete_values_for_workspace(workspace_id=workspace_id)
            else:
                logger.exception("Failed to purge workspace! Can not purge primary workspace!")
        else:
            logger.exception("Failed to purge workspace! Workspace not found by the provided workspace id!")


@app.command("cleanup")
def perform_workspace_cleanup(
    workspace_id: int = typer.Argument(None),
    cleanup_type: CleanupType = typer.Option("full", "--type", "-t"),
    date_to: Optional[datetime] = None,
    its_date_to: Optional[datetime] = None,
):

    """
    \b
    This command will delete all redundant data from the PostgreSQL database and also from Redis.
    Data is considered to be redundant if it is not used for anything. Like when a project is deleted and
    the commits for a repo just lies there without any reason. The same thing happens with the Redis too.
    """

    g = get_context()
    workspace = g.backend.workspaces.get(id_=workspace_id) if workspace_id else None

    if workspace_id and not workspace:
        logger.exception(
            "Failed to cleanup workspace! Workspace not exists for given workspace id!", workspace_id=workspace_id
        )
    else:
        confirm_res = typer.confirm("Are you sure you want to perform cleanup process on workspace(s)?")
        if confirm_res:
            workspace_ids = [workspace.id] if workspace else [w.id for w in g.backend.workspaces.all()]
            perform_data_cleanup(
                g=g,
                workspace_ids=workspace_ids,
                cleanup_type=cleanup_type,
                date_to=date_to,
                its_date_to=its_date_to,
            )
