import traceback
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from enum import Enum
from functools import partial
from typing import List, Union, Optional, Tuple

from dateutil.parser import parse as parse_date_str
from structlog import get_logger
from structlog.threadlocal import tmp_bind

from gitential2.core.authors import developer_map_callback
from gitential2.core.context import GitentialContext
from gitential2.datatypes.extraction import ExtractedKind
from gitential2.datatypes.its import ITSIssueAllData, ITSIssueHeader
from gitential2.datatypes.its_projects import ITSProjectCreate, ITSProjectInDB
from gitential2.datatypes.refresh import RefreshStrategy, RefreshType
from gitential2.datatypes.refresh_statuses import ITSProjectRefreshPhase, ITSProjectRefreshStatus
from gitential2.datatypes.userinfos import UserInfoInDB
from gitential2.settings import IntegrationType
from gitential2.utils import find_first, is_string_not_empty, get_user_id_or_raise_exception, is_list_not_empty
from .credentials import (
    get_fresh_credential,
    list_credentials_for_workspace,
    get_update_token_callback,
    get_workspace_creator_user_id,
    list_credentials_for_user,
)
from ..datatypes import CredentialInDB
from ..datatypes.user_its_projects_cache import UserITSProjectCacheCreate, UserITSProjectCacheInDB, UserITSProjectGroup

logger = get_logger(__name__)


class ITSProjectCacheOrderByOptions(str, Enum):
    api_url = "api_url"
    name = "name"
    namespace = "namespace"
    key = "key"
    integration_type = "integration_type"
    integration_name = "integration_name"
    integration_id = "integration_id"
    credential_id = "credential_id"


class ITSProjectOrderByDirections(str, Enum):
    asc = "ASC"
    desc = "DESC"


ISSUE_SOURCES = [IntegrationType.jira, IntegrationType.vsts]
SKIP_REFRESH_MSG = "Skipping ITS Project refresh"
DEFAULT_ITS_PROJECTS_LIMIT: int = 15
# TODO: For the new react front-end the MAX_ITS_PROJECTS_LIMIT has to be limited to a much smaller number, like 100.
MAX_ITS_PROJECTS_LIMIT: int = 20000
DEFAULT_ITS_PROJECTS_OFFSET: int = 0
DEFAULT_ITS_PROJECTS_ORDER_BY_OPTION: ITSProjectCacheOrderByOptions = ITSProjectCacheOrderByOptions.name
DEFAULT_ITS_PROJECTS_ORDER_BY_DIRECTION: ITSProjectOrderByDirections = ITSProjectOrderByDirections.asc


def list_available_its_projects(g: GitentialContext, workspace_id: int) -> List[ITSProjectCreate]:
    refresh_cache_of_its_projects_for_user_or_users(g=g, workspace_id=workspace_id)
    user_id: int = get_workspace_creator_user_id(g=g, workspace_id=workspace_id)
    its_projects_from_cache = _get_its_projects_cache(g, user_id)
    return its_projects_from_cache


def get_available_its_projects_paginated(
    g: GitentialContext,
    workspace_id: int,
    custom_user_id: Optional[int] = None,
    refresh_cache: Optional[bool] = False,
    force_refresh_cache: Optional[bool] = False,
    limit: Optional[int] = DEFAULT_ITS_PROJECTS_LIMIT,
    offset: Optional[int] = DEFAULT_ITS_PROJECTS_OFFSET,
    order_by_option: Optional[ITSProjectCacheOrderByOptions] = DEFAULT_ITS_PROJECTS_ORDER_BY_OPTION,
    order_by_direction: Optional[ITSProjectOrderByDirections] = DEFAULT_ITS_PROJECTS_ORDER_BY_DIRECTION,
    integration_type: Optional[str] = None,
    namespace: Optional[str] = None,
    credential_id: Optional[int] = None,
    search_pattern: Optional[str] = None,
) -> Tuple[int, int, int, List[ITSProjectCreate]]:
    # Making is_at_least_one_id_is_needed True in order to make sure that
    # The argument user_id for get_its_projects_cache_paginated is not None
    user_id_validated = get_user_id_or_raise_exception(
        g=g, is_at_least_one_id_is_needed=True, user_id=custom_user_id, workspace_id=workspace_id
    )

    refresh_cache_of_its_projects_for_user_or_users(
        g=g,
        user_id=custom_user_id or user_id_validated,
        refresh_cache=refresh_cache or False,
        force_refresh_cache=force_refresh_cache or False,
    )

    limit = (
        limit
        if (limit and 0 < limit < MAX_ITS_PROJECTS_LIMIT)
        else DEFAULT_ITS_PROJECTS_LIMIT
        if (limit and 0 > limit)
        else MAX_ITS_PROJECTS_LIMIT
    )
    offset = offset if offset and -1 < offset else DEFAULT_ITS_PROJECTS_OFFSET

    total_count, its_projects = g.backend.user_its_projects_cache.get_its_projects_cache_paginated(
        user_id=custom_user_id or user_id_validated,  # type: ignore[arg-type]
        limit=limit,
        offset=offset,
        order_by_option=order_by_option.value if order_by_option else ITSProjectCacheOrderByOptions.name.value,
        order_by_direction_is_asc=order_by_direction == ITSProjectOrderByDirections.asc,
        integration_type=integration_type,
        namespace=namespace,
        credential_id=credential_id,
        search_pattern=search_pattern,
    )

    return total_count, limit, offset, its_projects


def list_project_its_projects(g: GitentialContext, workspace_id: int, project_id: int) -> List[ITSProjectInDB]:
    ret = []
    for itsp_id in g.backend.project_its_projects.get_itsp_ids_for_project(
        workspace_id=workspace_id, project_id=project_id
    ):
        itsp = g.backend.its_projects.get(workspace_id=workspace_id, id_=itsp_id)
        if itsp:
            ret.append(itsp)
    return ret


def refresh_its_project(
    g: GitentialContext,
    workspace_id: int,
    itsp_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
    date_from: Optional[datetime] = None,
    force: bool = False,
):
    itsp = g.backend.its_projects.get_or_error(workspace_id=workspace_id, id_=itsp_id)
    integration = g.integrations.get(itsp.integration_name)

    with tmp_bind(
        logger, workspace_id=workspace_id, itsp_id=itsp_id, itsp_name=itsp.name, integration_name=itsp.integration_name
    ) as log:

        if (not integration) or (not hasattr(integration, "list_recently_updated_issues")):
            log.warning(SKIP_REFRESH_MSG, reason="integration not configured or missing implementation")
            return

        if _is_refresh_already_running(g, workspace_id, itsp_id) and not force:
            log.warning(SKIP_REFRESH_MSG, reason="already running")
            return

        log.info(
            "Starting ITS project refresh",
            strategy=strategy,
            refresh_type=refresh_type,
        )
        update_itsp_status(g, workspace_id, itsp_id, phase=ITSProjectRefreshPhase.running)
        try:
            token = _get_fresh_token_for_itsp(g, workspace_id, itsp)
            if token:
                if force and date_from is None:
                    date_from = datetime(2000, 1, 1)
                recently_updated_issues: List[ITSIssueHeader] = get_recently_updated_issues(
                    g, workspace_id, itsp_id, date_from=date_from, itsp=itsp
                )
                count_processed_items = 0
                update_itsp_status(
                    g,
                    workspace_id,
                    itsp_id,
                    count_recently_updated_items=len(recently_updated_issues),
                    count_processed_items=count_processed_items,
                )
                for ih in recently_updated_issues:
                    if force or _is_issue_new_or_updated(g, workspace_id, ih):
                        collect_and_save_data_for_issue(g, workspace_id, itsp=itsp, issue_id_or_key=ih.api_id)
                    else:
                        log.info("Issue is up-to-date", issue_api_id=ih.api_id, issue_key=ih.key)
                    count_processed_items += 1
                    update_itsp_status(g, workspace_id, itsp_id, count_processed_items=count_processed_items)
                    # TODO: increment count in status
            else:
                log.info(SKIP_REFRESH_MSG, workspace_id=workspace_id, itsp_id=itsp.id, reason="no fresh credential")

            update_itsp_status(g, workspace_id, itsp_id, phase=ITSProjectRefreshPhase.done)
        except:  # pylint: disable=bare-except
            update_itsp_status(
                g,
                workspace_id,
                itsp_id,
                phase=ITSProjectRefreshPhase.done,
                is_error=True,
                error_msg=traceback.format_exc(limit=1),
            )
            log.exception("Failed to refresh ITS Project")


def refresh_cache_of_its_projects_for_user_or_users(
    g: GitentialContext,
    user_id: Optional[int] = None,
    workspace_id: Optional[int] = None,
    refresh_cache: Optional[bool] = False,
    force_refresh_cache: Optional[bool] = False,
):
    """
    If workspace id is provided, we get the user id from the workspace creator.
    Otherwise, we use the optionally provided user id.
    If none of the above is provided, then we get all the user ids from the database and make the repo cache for them.
    """

    user_id_validated = get_user_id_or_raise_exception(
        g=g, is_at_least_one_id_is_needed=False, user_id=user_id, workspace_id=workspace_id
    )

    if user_id_validated:
        _refresh_its_projects_cache_for_user(
            g=g, user_id=user_id_validated, refresh_cache=refresh_cache, force_refresh_cache=force_refresh_cache
        )
    else:
        user_ids: List[int] = [u.id for u in g.backend.users.all()]
        user_ids_success: List[int] = []
        for uid in user_ids:
            result = _refresh_its_projects_cache_for_user(
                g=g, user_id=uid, refresh_cache=refresh_cache, force_refresh_cache=force_refresh_cache
            )
            if result:
                user_ids_success.append(uid)
        logger.info("Refresh repo cache for every user ended", user_ids_success=user_ids_success)


def _refresh_its_projects_cache_for_user(
    g: GitentialContext,
    user_id: int,
    workspace_id: Optional[int] = None,
    refresh_cache: Optional[bool] = False,
    force_refresh_cache: Optional[bool] = False,
):
    """
    Repositories cache can be refreshed either by providing user_id or workspace_id.
    If the workspace id is provided, we get the user_id by requesting the creator id of the workspace,
    otherwise, we will use the user_id.
    If none of the above is provided an exception will be raised.
    """

    logger.info(
        "Starting to refresh ITS projects cache for user.",
        user_id=user_id,
        workspace_id=workspace_id,
        refresh_cache=refresh_cache,
        force_refresh_cache=force_refresh_cache,
    )

    # Just needed because of the mypy check.
    refresh_cache_c = refresh_cache or False
    force_refresh_cache_c = force_refresh_cache or False

    credentials_for_user: List[CredentialInDB] = (
        list_credentials_for_user(g=g, user_id=user_id)
        if user_id
        else list_credentials_for_workspace(g=g, workspace_id=workspace_id)
        # This check only needed because of pylint. Otherwise, we raise an exception if there was no
        # workspace_id or user_id provided.
        if workspace_id
        else []
    )

    its_projects_for_credential = partial(
        _refresh_its_projects_cache_for_credential,
        g,
        user_id,
        refresh_cache_c,
        force_refresh_cache_c,
    )
    with ThreadPoolExecutor() as executor:
        executor.map(its_projects_for_credential, credentials_for_user)

    return True


def _refresh_its_projects_cache_for_credential(
    g: GitentialContext,
    user_id: int,
    refresh_cache: bool,
    force_refresh_cache: bool,
    credential: CredentialInDB,
):
    refresh_in_progress_key = (
        f"its-projects-cache-refresh-in-progress--user--{user_id}--integration-type--{credential.integration_type}"
    )
    is_in_progress = g.kvstore.get_value(refresh_in_progress_key)
    if (
        credential.integration_type in ISSUE_SOURCES
        and credential.integration_name in g.integrations
        and not is_in_progress
    ):
        try:
            credential_fresh: Optional[CredentialInDB] = get_fresh_credential(g, credential_id=credential.id)
            if credential_fresh:
                integration = g.integrations[credential_fresh.integration_name]
                token = credential_fresh.to_token_dict(fernet=g.fernet)

                userinfo: UserInfoInDB = (
                    find_first(
                        lambda ui: ui.integration_name
                        == credential_fresh.integration_name,  # pylint: disable=cell-var-from-loop
                        g.backend.user_infos.get_for_user(credential_fresh.owner_id),
                    )
                    if credential_fresh.owner_id
                    else None
                )

                refresh = _get_itsp_last_refresh_date(
                    g, user_id, credential_fresh.integration_type or credential.integration_type
                )

                # if there is no saved last refresh time or if it is expired,
                # we need to get the list again
                if (
                    refresh_cache
                    or force_refresh_cache
                    or not isinstance(refresh, datetime)
                    or (
                        isinstance(refresh, datetime)
                        and (g.current_time() - timedelta(hours=g.settings.cache.its_projects_cache_life_hours))
                        > refresh
                    )
                ):
                    if force_refresh_cache:
                        delete_count: int = g.backend.user_its_projects_cache.delete_cache_for_user(user_id=user_id)
                        g.kvstore.delete_value(
                            name=f"itsp_cache_for_user_last_refresh_datetime--{credential.integration_type}--{user_id}"
                        )
                        logger.info(
                            "force_refresh_cache was set. ITS Projects cache for user deleted.",
                            number_of_deleted_rows=delete_count,
                            user_id=user_id,
                        )

                    new_its_projects = integration.list_available_its_projects(
                        token=token,
                        update_token=get_update_token_callback(g, credential_fresh),
                        provider_user_id=userinfo.sub if userinfo else None,
                    )
                    _save_its_projects_to_cache(g, user_id, new_its_projects)
                    _save_its_projects_last_refresh_date(
                        g, user_id, credential_fresh.integration_type or credential.integration_type
                    )
                    g.kvstore.delete_value(refresh_in_progress_key)
                else:
                    g.kvstore.delete_value(refresh_in_progress_key)
            else:
                g.kvstore.delete_value(refresh_in_progress_key)
                logger.error(
                    "Cannot get fresh credential!",
                    credential_id=credential.id,
                    owner_id=credential.owner_id,
                    integration_name=credential.integration_name,
                )
        except Exception:  # pylint: disable=broad-except
            g.kvstore.delete_value(refresh_in_progress_key)
            logger.exception(
                "Error during collecting ITS projects",
                integration_name=credential.integration_name,
                credential_id=credential.id,
            )
    elif is_in_progress:
        logger.info(
            "ITS Projects cache refresh is currently in progress for user with integration type.",
            user_id=user_id,
            integration_type=credential.integration_type,
        )


def update_itsp_status(
    g: GitentialContext, workspace_id: int, itsp_id: int, phase: Optional[ITSProjectRefreshPhase] = None, **kwargs
):

    status = get_itsp_status(g, workspace_id, itsp_id)
    status_dict = status.dict()

    if phase == ITSProjectRefreshPhase.scheduled:
        status_dict["finished_at"] = None
        status_dict["started_at"] = None
        status_dict["scheduled_at"] = g.current_time()
        status_dict["phase"] = ITSProjectRefreshPhase.scheduled
    elif phase == ITSProjectRefreshPhase.running:
        status_dict["finished_at"] = None
        status_dict["started_at"] = g.current_time()
        status_dict["phase"] = ITSProjectRefreshPhase.running
    elif phase == ITSProjectRefreshPhase.done:
        status_dict["finished_at"] = g.current_time()
        status_dict["phase"] = ITSProjectRefreshPhase.done
        if not kwargs.get("is_error", False):
            status_dict["last_successful_at"] = status_dict["started_at"]
            status_dict["is_error"] = False
            status_dict["error_msg"] = None

    status_dict.update(**kwargs)
    return set_itsp_status(g, workspace_id, itsp_id, status_dict)


def _get_itsp_status_key(workspace_id: int, itsp_id: int) -> str:
    return f"ws-{workspace_id}:itsp-{itsp_id}"


def get_itsp_status(g: GitentialContext, workspace_id: int, itsp_id: int) -> ITSProjectRefreshStatus:
    key = _get_itsp_status_key(workspace_id, itsp_id)
    status_dict = g.kvstore.get_value(key)
    if status_dict and isinstance(status_dict, dict):
        return ITSProjectRefreshStatus(**status_dict)
    else:
        itsp = g.backend.its_projects.get_or_error(workspace_id, itsp_id)
        default_values = {
            "scheduled_at": None,
            "started_at": None,
            "finished_at": None,
            "last_successful_at": None,
            "phase": ITSProjectRefreshPhase.unknown,
            "count_recently_updated_items": 0,
            "count_processed_items": 0,
            "is_error": False,
            "error_msg": None,
        }
        status_dict = {"workspace_id": workspace_id, "id": itsp.id, "name": itsp.name, **default_values}
        g.kvstore.set_value(key, status_dict)
        return ITSProjectRefreshStatus(**status_dict)


def set_itsp_status(g: GitentialContext, workspace_id: int, itsp_id: int, status_dict: dict) -> ITSProjectRefreshStatus:
    key = _get_itsp_status_key(workspace_id, itsp_id)
    g.kvstore.set_value(key, status_dict)
    return ITSProjectRefreshStatus(**status_dict)


def get_recently_updated_issues(
    g: GitentialContext,
    workspace_id: int,
    itsp_id: int,
    date_from: Optional[datetime] = None,
    itsp: Optional[ITSProjectInDB] = None,
) -> List[ITSIssueHeader]:
    itsp = itsp or g.backend.its_projects.get_or_error(workspace_id=workspace_id, id_=itsp_id)
    integration = g.integrations.get(itsp.integration_name)

    with tmp_bind(
        logger, workspace_id=workspace_id, itsp_id=itsp_id, itsp_name=itsp.name, integration_name=itsp.integration_name
    ) as log:
        if (not integration) or (not hasattr(integration, "list_recently_updated_issues")):
            log.warning(SKIP_REFRESH_MSG, reason="integration not configured or missing implementation")
            return []

        token = _get_fresh_token_for_itsp(g, workspace_id, itsp)
        if token:
            date_from_c = date_from or _get_last_successful_refresh_run(g, workspace_id, itsp_id)
            if not date_from_c:
                log.info("Getting all issues in the project")
                return integration.list_all_issues_for_project(
                    token,
                    itsp,
                    date_from=_get_time_restriction_date(g),
                )
            log.info("Getting issues changed since", date_from=date_from)
            return integration.list_recently_updated_issues(
                token,
                itsp,
                date_from=(_get_time_restriction_date(g) if not date_from and date_from_c else None) or date_from_c,
            )
        else:
            log.info(SKIP_REFRESH_MSG, workspace_id=workspace_id, itsp_id=itsp.id, reason="no fresh credential")
            return []


def _is_issue_new_or_updated(g: GitentialContext, workspace_id: int, issue_header: ITSIssueHeader) -> bool:
    existing_ih = g.backend.its_issues.get_header(workspace_id, issue_header.id)
    if (
        existing_ih
        and existing_ih.updated_at
        and issue_header.updated_at
        and existing_ih.updated_at.replace(tzinfo=timezone.utc) == issue_header.updated_at.astimezone(timezone.utc)
    ):
        return False
    return True


def _get_fresh_token_for_itsp(g: GitentialContext, workspace_id: int, itsp: ITSProjectInDB) -> Optional[dict]:
    token = None
    credential = get_fresh_credential(
        g,
        credential_id=itsp.credential_id,
        workspace_id=workspace_id,
        integration_name=itsp.integration_name,
    )
    if credential:
        token = credential.to_token_dict(g.fernet)
    return token


def _is_refresh_already_running(g: GitentialContext, workspace_id: int, itsp_id: int) -> bool:
    status = get_itsp_status(g, workspace_id, itsp_id)
    return status.phase == ITSProjectRefreshPhase.running


def _get_last_successful_refresh_run(g: GitentialContext, workspace_id: int, itsp_id) -> Optional[datetime]:
    status = get_itsp_status(g, workspace_id, itsp_id)
    return status.last_successful_at


def _get_time_restriction_date(g: GitentialContext) -> Optional[datetime]:
    limit_in_days = g.settings.extraction.its_project_analysis_limit_in_days
    return datetime.utcnow() - timedelta(days=limit_in_days) if limit_in_days else None


def collect_and_save_data_for_issue(
    g: GitentialContext,
    workspace_id: int,
    itsp: Union[ITSProjectInDB, int],
    issue_id_or_key: str,
    token: Optional[dict] = None,
):
    dev_map_callback = partial(developer_map_callback, g=g, workspace_id=workspace_id)
    itsp = (
        itsp
        if isinstance(itsp, ITSProjectInDB)
        else g.backend.its_projects.get_or_error(workspace_id=workspace_id, id_=itsp)
    )

    with tmp_bind(
        logger,
        workspace_id=workspace_id,
        itsp_id=itsp.id,
        itsp_name=itsp.name,
        integration_name=itsp.integration_name,
        issue_id_or_key=issue_id_or_key,
    ) as log:

        token = token or _get_fresh_token_for_itsp(g, workspace_id, itsp)
        if not token:
            log.info("Skipping issue data collection: no fresh credential")
            return

        integration = g.integrations.get(itsp.integration_name)
        if not integration:
            log.warning("Skipping issue data collection: integration not configured")
            return

        log.info("Starting collection of issue data")
        issue_data = integration.get_all_data_for_issue(token, itsp, issue_id_or_key, dev_map_callback)
        _save_collected_issue_data(g, workspace_id, issue_data)
        log.info(
            "Issue data saved",
            issue_id=issue_data.issue.id,
        )


def get_available_its_project_groups(g: GitentialContext, workspace_id: int) -> List[UserITSProjectGroup]:
    user_id: int = get_workspace_creator_user_id(g=g, workspace_id=workspace_id)
    itsp_groups = g.backend.its_projects.get_its_projects_groups_with_cache(workspace_id=workspace_id, user_id=user_id)
    if not is_list_not_empty(itsp_groups):
        refresh_cache_of_its_projects_for_user_or_users(
            g=g,
            user_id=user_id,
            refresh_cache=True,
        )
        itsp_groups = g.backend.its_projects.get_its_projects_groups_with_cache(
            workspace_id=workspace_id, user_id=user_id
        )
    return itsp_groups


def _save_collected_issue_data(g: GitentialContext, workspace_id: int, issue_data: ITSIssueAllData):
    output = g.backend.output_handler(workspace_id)
    output.write(ExtractedKind.ITS_ISSUE, issue_data.issue)
    for change in issue_data.changes:
        output.write(ExtractedKind.ITS_ISSUE_CHANGE, change)
    for time_in_status in issue_data.times_in_statuses:
        output.write(ExtractedKind.ITS_ISSUE_TIME_IN_STATUS, time_in_status)
    for comment in issue_data.comments:
        output.write(ExtractedKind.ITS_ISSUE_COMMENT, comment)
    for linked_issue in issue_data.linked_issues:
        output.write(ExtractedKind.ITS_ISSUE_LINKED_ISSUE, linked_issue)
    for sprint in issue_data.sprints:
        output.write(ExtractedKind.ITS_SPRINT, sprint)
    for issue_sprint in issue_data.issue_sprints:
        output.write(ExtractedKind.ITS_ISSUE_SPRINT, issue_sprint)
    for worklog in issue_data.worklogs:
        output.write(ExtractedKind.ITS_ISSUE_WORKLOG, worklog)


def _get_itsp_last_refresh_kvstore_key(user_id: int, integration_type: str):
    return f"itsp_cache_for_user_last_refresh_datetime--{integration_type}--{user_id}"


def _get_itsp_last_refresh_date(g: GitentialContext, user_id: int, integration_type: str) -> Optional[datetime]:
    result = None
    redis_key = _get_itsp_last_refresh_kvstore_key(user_id, integration_type)
    refresh_raw = g.kvstore.get_value(redis_key)
    if is_string_not_empty(refresh_raw):
        try:
            result = parse_date_str(refresh_raw).replace(tzinfo=timezone.utc)
        except ValueError:
            logger.debug(f"Last refresh date is invalid for user_id: {user_id}")
    return result


def _save_itsp_last_refresh_date(g: GitentialContext, user_id: int, integration_type: str):
    refresh_save = str(datetime.utcnow())
    redis_key = _get_itsp_last_refresh_kvstore_key(user_id, integration_type)
    g.kvstore.set_value(redis_key, refresh_save)


def _list_available_its_projects(g, integration, token, credential, userinfo):
    return integration.list_available_its_projects(
        token=token,
        update_token=get_update_token_callback(g, credential),
        provider_user_id=userinfo.sub if userinfo else None,
    )


def _save_its_projects_to_cache(g: GitentialContext, user_id: int, its_projects: List[ITSProjectCreate]):
    save_to_cache: List[UserITSProjectCacheCreate] = [
        UserITSProjectCacheCreate(
            user_id=user_id,
            name=itsp.name,
            namespace=itsp.namespace,
            private=itsp.private,
            api_url=itsp.api_url,
            key=itsp.key,
            integration_type=itsp.integration_type,
            integration_name=itsp.integration_name,
            integration_id=itsp.integration_id,
            credential_id=itsp.credential_id,
            extra=itsp.extra,
        )
        for itsp in its_projects
    ]
    g.backend.user_its_projects_cache.insert_its_projects_cache_for_user(save_to_cache)


def _save_its_projects_last_refresh_date(g: GitentialContext, user_id: int, integration_type: str):
    refresh_save = str(datetime.utcnow())
    redis_key = _get_itsp_last_refresh_kvstore_key(user_id, integration_type)
    g.kvstore.set_value(redis_key, refresh_save)


def _get_its_projects_cache(g: GitentialContext, user_id: int) -> List[ITSProjectCreate]:
    cache: List[UserITSProjectCacheInDB] = g.backend.user_its_projects_cache.get_all_its_project_for_user(user_id)
    return [
        ITSProjectCreate(
            name=itsp.name,
            namespace=itsp.namespace,
            private=itsp.private,
            api_url=itsp.api_url,
            key=itsp.key,
            integration_type=itsp.integration_type,
            integration_name=itsp.integration_name,
            integration_id=itsp.integration_id,
            credential_id=itsp.credential_id,
            extra=itsp.extra,
        )
        for itsp in cache
    ]
