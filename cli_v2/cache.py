from typing import Optional, List

import typer
from structlog import get_logger

from .common import get_context
from ..core.credentials import get_workspace_creator_user_id
from ..core.its import refresh_cache_of_its_projects_for_user_or_users
from ..core.repositories import refresh_cache_of_repositories_for_user_or_users
from ..core.users import reset_cache_for_user
from ..datatypes.cli_v2 import CacheRefreshType

app = typer.Typer()
logger = get_logger(__name__)


@app.command("refresh")
def refresh_cache(
    workspace_id: Optional[int] = None,
    user_id: Optional[int] = None,
    force_refresh: bool = False,
    refresh_type: CacheRefreshType = typer.Option("everything", "--type", "-t"),
):
    """
    Refresh cache.
    \n
    By running this command, you can refresh the cache of either repositories or issue tracking system projects
    or both. You can provide a specific workspace id or user id. Please keep in mind that cache is for a user
    and not for a workspace so if a workspace id is provided, the id of the workspace creator user will be used.
    \n
    If force refresh boolean is provided, the whole cache will be deleted and re-requested again from the providers.
    \n
    There is a possibility to not provide any workspace id or user id. In this case, the refresh will be applied for
    every user in the database.
    """

    g = get_context()

    if refresh_type in [CacheRefreshType.everything, CacheRefreshType.repos]:
        refresh_cache_of_repositories_for_user_or_users(
            g=g, refresh_cache=True, force_refresh_cache=force_refresh, user_id=user_id, workspace_id=workspace_id
        )

    if refresh_type in [CacheRefreshType.everything, CacheRefreshType.its_projects]:
        refresh_cache_of_its_projects_for_user_or_users(
            g=g, refresh_cache=True, force_refresh_cache=force_refresh, user_id=user_id, workspace_id=workspace_id
        )


@app.command("reset")
def reset_cache(
    workspace_id: Optional[int] = None,
    user_id: Optional[int] = None,
    reset_type: CacheRefreshType = typer.Option("everything", "--type", "-t"),
):
    """
    DANGER ZONE!!! - Reset cache.
    \n
    By running this command, you can reset the cache of either repositories or issue tracking system projects
    or both. You can provide a specific workspace id or user id. Please keep in mind that cache is for a user
    and not for a workspace so if a workspace id is provided, the id of the workspace creator user will be used.
    \n
    The whole cache will be deleted with Redis values!
    \n
    There is a possibility to not provide any workspace id or user id. In this case, the reset will be applied for
    every user in the database.
    """

    g = get_context()

    if not user_id and not workspace_id:
        logger.info("No user id of workspace id was provided. Starting to reset the cache for all users.")
        user_ids: List[int] = [u.id for u in g.backend.users.all()]
        for uid in user_ids:
            reset_cache_for_user(g=g, reset_type=reset_type, user_id=uid)
    else:
        user = g.backend.users.get(
            get_workspace_creator_user_id(g=g, workspace_id=workspace_id) if workspace_id else user_id
        )
        if not user:
            logger.exception("Provided ID was incorrect. Can not make a cache reset.")
        else:
            reset_cache_for_user(g=g, reset_type=reset_type, user_id=user.id)
