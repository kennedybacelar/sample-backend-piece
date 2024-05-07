from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import partial
from typing import List, Optional, Tuple, Union

from dateutil.parser import parse as parse_date_str
from sqlalchemy import exc
from structlog import get_logger

from gitential2.datatypes.credentials import CredentialInDB
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryInDB, GitProtocol
from gitential2.datatypes.userinfos import UserInfoInDB
from gitential2.integrations import REPOSITORY_SOURCES
from gitential2.utils import (
    levenshtein,
    find_first,
    is_list_not_empty,
    is_string_not_empty,
    get_user_id_or_raise_exception,
)
from .context import GitentialContext
from .credentials import (
    get_fresh_credential,
    list_credentials_for_workspace,
    get_update_token_callback,
    list_credentials_for_user,
    get_workspace_creator_user_id,
)
from ..datatypes.user_repositories_cache import (
    UserRepositoryCacheInDB,
    UserRepositoryCacheCreate,
    UserRepositoryGroup,
)
from ..exceptions import SettingsException

logger = get_logger(__name__)


class RepoCacheOrderByOptions(str, Enum):
    name = "name"
    namespace = "namespace"
    protocol = "protocol"
    clone_url = "clone_url"
    integration_type = "integration_type"
    integration_name = "integration_name"


class RepoCacheOrderByDirections(str, Enum):
    asc = "ASC"
    desc = "DESC"


DEFAULT_REPOS_LIMIT: int = 15
# TODO: For the new react front-end the MAX_REPOS_LIMIT has to be limited to a much smaller number, like 100.
MAX_REPOS_LIMIT: int = 20000
DEFAULT_REPOS_OFFSET: int = 0
DEFAULT_REPOS_ORDER_BY_OPTION: RepoCacheOrderByOptions = RepoCacheOrderByOptions.name
DEFAULT_REPOS_ORDER_BY_DIRECTION: RepoCacheOrderByDirections = RepoCacheOrderByDirections.asc


def get_repository(g: GitentialContext, workspace_id: int, repository_id: int) -> Optional[RepositoryInDB]:
    return g.backend.repositories.get(workspace_id, repository_id)


def get_available_repositories_for_workspace(
    g: GitentialContext, workspace_id: int, user_organization_name_list: Optional[List[str]]
) -> List[RepositoryCreate]:
    def _merge_repo_lists(first: List[RepositoryCreate], second: List[RepositoryCreate]):
        existing_clone_urls = [r.clone_url for r in first]
        new_repos = [r for r in second if r.clone_url not in existing_clone_urls]
        return first + new_repos

    # Get all already used repositories
    results: List[RepositoryCreate] = [RepositoryCreate(**r.dict()) for r in list_repositories(g, workspace_id)]

    collected_results = _get_available_repositories_for_workspace_credentials(
        g, workspace_id, user_organization_name_list
    )
    results = _merge_repo_lists(collected_results, results)

    logger.debug(
        "list_of_all_user_repositories",
        number_of_all_user_repositories=len(results),
        repo_clone_urls=[
            repo.dict().get("clone_url", None)
            for repo in results
            if repo is not None and is_string_not_empty(repo.clone_url)
        ]
        if is_list_not_empty(results)
        else "No repos found!",
    )

    return results


def _get_available_repositories_for_workspace_credentials(
    g: GitentialContext,
    workspace_id: int,
    user_organization_name_list: Optional[List[str]],
) -> List[RepositoryCreate]:
    refresh_cache_of_repositories_for_user_or_users(
        g=g, workspace_id=workspace_id, user_organization_name_list=user_organization_name_list
    )
    user_id: int = get_workspace_creator_user_id(g=g, workspace_id=workspace_id)
    repos_from_cache = _get_repos_cache(g, user_id)
    return repos_from_cache


def get_available_repositories_paginated(
    g: GitentialContext,
    workspace_id: int,
    custom_user_id: Optional[int] = None,
    refresh_cache: Optional[bool] = False,
    force_refresh_cache: Optional[bool] = False,
    user_organization_name_list: Optional[List[str]] = None,
    limit: Optional[int] = DEFAULT_REPOS_LIMIT,
    offset: Optional[int] = DEFAULT_REPOS_OFFSET,
    order_by_option: Optional[RepoCacheOrderByOptions] = DEFAULT_REPOS_ORDER_BY_OPTION,
    order_by_direction: Optional[RepoCacheOrderByDirections] = DEFAULT_REPOS_ORDER_BY_DIRECTION,
    integration_type: Optional[str] = None,
    namespace: Optional[str] = None,
    credential_id: Optional[int] = None,
    search_pattern: Optional[str] = None,
) -> Tuple[int, int, int, List[RepositoryCreate]]:
    # Making is_at_least_one_id_is_needed True in order to make sure that
    # The argument user_id for _get_user_repositories_by_query is not None
    user_id_validated = get_user_id_or_raise_exception(
        g=g, user_id=custom_user_id, workspace_id=workspace_id, is_at_least_one_id_is_needed=True
    )

    refresh_cache_of_repositories_for_user_or_users(
        g=g,
        user_id=user_id_validated,
        user_organization_name_list=user_organization_name_list,
        refresh_cache=refresh_cache or False,
        force_refresh_cache=force_refresh_cache or False,
    )

    limit = (
        limit
        if (limit and 0 < limit < MAX_REPOS_LIMIT)
        else DEFAULT_REPOS_LIMIT
        if (limit and 0 > limit)
        else MAX_REPOS_LIMIT
    )
    offset = offset if offset and -1 < offset else DEFAULT_REPOS_OFFSET

    total_count, repositories = _get_user_repositories_by_query(
        g=g,
        workspace_id=workspace_id,
        user_id=user_id_validated,  # type: ignore[arg-type]
        limit=limit,
        offset=offset,
        order_by_option=order_by_option or RepoCacheOrderByOptions.name,
        order_by_direction=order_by_direction or RepoCacheOrderByDirections.asc,
        integration_type=integration_type,
        namespace=namespace,
        credential_id=credential_id,
        search_pattern=search_pattern,
    )

    return total_count, limit, offset, repositories


def _get_user_repositories_by_query(
    g: GitentialContext,
    workspace_id: int,
    user_id: int,
    limit: int,
    offset: int,
    order_by_option: RepoCacheOrderByOptions,
    order_by_direction: RepoCacheOrderByDirections,
    integration_type: Optional[str] = None,
    namespace: Optional[str] = None,
    credential_id: Optional[int] = None,
    search_pattern: Optional[str] = None,
) -> Tuple[int, List[RepositoryCreate]]:
    query: str = _get_query_of_get_repositories(
        workspace_id=workspace_id,
        user_id=user_id,
        limit=limit,
        offset=offset,
        order_by_option=order_by_option,
        order_by_direction=order_by_direction,
        integration_type=integration_type,
        namespace=namespace,
        credential_id=credential_id,
        search_pattern=search_pattern,
    )

    try:
        logger.info("Executing query to get list of repositories paginated result.", query=query)
        rows = g.backend.execute_query(query).all()
    except exc.SQLAlchemyError as se:
        raise SettingsException(
            "Exception while trying to run query to get list of repositories paginated result!"
        ) from se

    def get_extra_with_min_info(row):
        result = {}
        if "repo_provider_id" in row and row["repo_provider_id"]:
            if row["integration_type"] in ["github", "gitlab"]:
                result["id"] = int(row["repo_provider_id"])
            elif row["integration_type"] == "bitbucket":
                result["uuid"] = row["repo_provider_id"]
            elif row["integration_type"] == "vsts":
                result["id"] = row["repo_provider_id"]
        return result

    repositories = (
        [
            RepositoryCreate(
                clone_url=row["clone_url"],
                protocol=row["protocol"],
                name=row["name"],
                namespace=row["namespace"],
                private=row["private"],
                integration_type=row["integration_type"],
                integration_name=row["integration_name"],
                credential_id=row["credential_id"],
                extra=get_extra_with_min_info(row),
            )
            for row in rows
            if "clone_url" in row and row["clone_url"]
        ]
        if is_list_not_empty(rows)
        else []
    )

    total_count = rows[0]["total_count"] if is_list_not_empty(rows) else 0

    return total_count, repositories


def _get_query_of_get_repositories(
    workspace_id: int,
    user_id: int,
    limit: int,
    offset: int,
    order_by_option: RepoCacheOrderByOptions,
    order_by_direction: RepoCacheOrderByDirections,
    integration_type: Optional[str] = None,
    namespace: Optional[str] = None,
    credential_id: Optional[int] = None,
    search_pattern: Optional[str] = None,
) -> str:
    def get_filter(column_name: str, filter_value: Union[str, int, None]) -> Union[str, None]:
        return f"{column_name} = '{filter_value}'" if filter_value else None

    def get_filters(is_user_id: bool):
        name_filter: Optional[str] = (
            f"name ILIKE '{search_pattern.replace('%', '%%')}'" if is_string_not_empty(search_pattern) else None
        )
        integration_type_filter: Optional[str] = get_filter("integration_type", integration_type)
        namespace_filter: Optional[str] = get_filter("namespace", namespace)
        credential_id_filter: Optional[str] = get_filter("credential_id", credential_id)
        user_id_filter: Optional[str] = f"user_id = {user_id}" if is_user_id else None
        filters: str = " AND ".join(
            [
                f
                for f in [name_filter, integration_type_filter, namespace_filter, credential_id_filter, user_id_filter]
                if is_string_not_empty(f)
            ]
        )
        return f"WHERE {filters}" if is_string_not_empty(filters) else ""

    get_repo_uuid = "CAST(r.extra::json -> 'uuid' AS TEXT)"
    get_repo_id = "CAST(r.extra::json -> 'id' AS TEXT)"
    repo_provider_id = f"COALESCE({get_repo_uuid}, {get_repo_id})"
    repo_provider_id_trimmed = f"TRIM(BOTH '\"' FROM {repo_provider_id})"

    # noinspection SqlResolve
    query = (
        "WITH repo_selection AS "
        "    ("
        "        SELECT "
        "            clone_url, "
        "            repo_provider_id, "
        "            protocol, "
        "            name, "
        "            namespace, "
        "            private, "
        "            integration_type, "
        "            integration_name, "
        "            credential_id "
        "        FROM public.user_repositories_cache "
        f"           {get_filters(True)} "
        "        UNION "
        "        SELECT "
        "            clone_url, "
        f"           {repo_provider_id_trimmed} AS repo_provider_id, "
        "            protocol, "
        "            name, "
        "            namespace, "
        "            private, "
        "            integration_type, "
        "            integration_name, "
        "            credential_id "
        f"        FROM ws_{workspace_id}.repositories r "
        f"           {get_filters(False)} "
        "    )"
        "SELECT * FROM ("
        "    TABLE repo_selection "
        f"   ORDER BY {order_by_option} {order_by_direction} "
        f"   LIMIT {limit} "
        f"   OFFSET {offset}) sub "
        "RIGHT JOIN (SELECT COUNT(*) FROM repo_selection) c(total_count) ON TRUE;"
    )

    return query


def refresh_cache_of_repositories_for_user_or_users(
    g: GitentialContext,
    user_id: Optional[int] = None,
    workspace_id: Optional[int] = None,
    refresh_cache: Optional[bool] = False,
    force_refresh_cache: Optional[bool] = False,
    user_organization_name_list: Optional[List[str]] = None,
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
        _refresh_repos_cache_for_user(
            g=g,
            user_id=user_id_validated,
            refresh_cache=refresh_cache,
            force_refresh_cache=force_refresh_cache,
            user_organization_name_list=user_organization_name_list,
        )
    else:
        user_ids: List[int] = [u.id for u in g.backend.users.all()]
        user_ids_success: List[int] = []
        for uid in user_ids:
            result = _refresh_repos_cache_for_user(
                g=g,
                user_id=uid,
                refresh_cache=refresh_cache,
                force_refresh_cache=force_refresh_cache,
                user_organization_name_list=user_organization_name_list,
            )
            if result:
                user_ids_success.append(uid)
        logger.info("Refresh repo cache for every user ended", user_ids_success=user_ids_success)


def _refresh_repos_cache_for_user(
    g: GitentialContext,
    user_id: int,
    workspace_id: Optional[int] = None,
    refresh_cache: Optional[bool] = False,
    force_refresh_cache: Optional[bool] = False,
    user_organization_name_list: Optional[List[str]] = None,
) -> bool:
    """
    Repositories cache can be refreshed either by providing user_id or workspace_id.
    If the workspace id is provided, we get the user_id by requesting the creator id of the workspace,
    otherwise, we will use the user_id.
    If none of the above is provided an exception will be raised.
    """

    logger.info(
        "Starting to refresh repos cache for user.",
        user_id=user_id,
        workspace_id=workspace_id,
        refresh_cache=refresh_cache,
        force_refresh_cache=force_refresh_cache,
        user_organization_name_list=user_organization_name_list,
    )

    # Just needed because of the mypy check.
    refresh_cache_c = refresh_cache or False
    force_refresh_cache_c = force_refresh_cache or False

    credentials_for_user: List[CredentialInDB] = (
        list_credentials_for_user(g, user_id)
        if user_id
        else list_credentials_for_workspace(g, workspace_id)
        # This check only needed because of pylint. Otherwise, we raise an exception if there was no
        # workspace_id or user_id provided.
        if workspace_id
        else []
    )
    repos_for_credential = partial(
        _refresh_repos_cache_for_credential,
        g,
        user_id,
        refresh_cache_c,
        force_refresh_cache_c,
        user_organization_name_list,
    )
    with ThreadPoolExecutor() as executor:
        executor.map(repos_for_credential, credentials_for_user)

    return True


def _refresh_repos_cache_for_credential(
    g: GitentialContext,
    user_id: int,
    refresh_cache: bool,
    force_refresh_cache: bool,
    user_organization_name_list: Optional[List[str]],
    credential: CredentialInDB,
):
    refresh_in_progress_key = (
        f"repos-cache-refresh-in-progress--user--{user_id}--integration-type--{credential.integration_type}"
    )
    is_in_progress = g.kvstore.get_value(refresh_in_progress_key)
    if (
        credential.integration_type in REPOSITORY_SOURCES
        and credential.integration_name in g.integrations
        and not is_in_progress
    ):
        g.kvstore.set_value(refresh_in_progress_key, True)
        try:
            credential_fresh = get_fresh_credential(g, credential_id=credential.id)
            if credential_fresh:
                # with acquire_credential(g, credential_id=credential_fresh.id) as credential:
                integration = g.integrations[credential_fresh.integration_name]
                token = credential_fresh.to_token_dict(fernet=g.fernet)
                userinfo: UserInfoInDB = (
                    find_first(
                        lambda ui: ui.integration_name
                        == credential.integration_name,  # pylint: disable=cell-var-from-loop
                        g.backend.user_infos.get_for_user(credential.owner_id),
                    )
                    if credential.owner_id
                    else None
                )

                refresh = _get_repos_last_refresh_date(
                    g=g, user_id=user_id, integration_type=credential.integration_type
                )
                if (
                    (refresh_cache and isinstance(refresh, datetime))
                    or (
                        isinstance(refresh, datetime)
                        and (g.current_time() - timedelta(hours=g.settings.cache.repo_cache_life_hours)) > refresh
                    )
                ) and not force_refresh_cache:
                    repos_newly_created: List[RepositoryCreate] = integration.get_newest_repos_since_last_refresh(
                        token=token,
                        update_token=get_update_token_callback(g, credential),
                        last_refresh=refresh,
                        provider_user_id=userinfo.sub if userinfo else None,
                        user_organization_names=user_organization_name_list,
                    )
                    _save_repos_to_repos_cache(g=g, user_id=user_id, repo_list=repos_newly_created)
                    _save_repos_last_refresh_date(g=g, user_id=user_id, integration_type=credential.integration_type)

                    logger.debug(
                        "Saved new repositories to cache.",
                        integration_type=credential.integration_type,
                        new_repos=[getattr(r, "clone_url", None) for r in repos_newly_created]
                        if is_list_not_empty(repos_newly_created)
                        else [],
                    )
                    g.kvstore.delete_value(refresh_in_progress_key)
                elif force_refresh_cache or not isinstance(refresh, datetime):
                    if force_refresh_cache:
                        delete_count: int = g.backend.user_repositories_cache.delete_cache_for_user(user_id=user_id)
                        g.kvstore.delete_value(
                            name=f"repository_cache_for_user_last_refresh_datetime--{credential.integration_type}--{user_id}"
                        )
                        logger.info(
                            "force_refresh_cache was set. Repos cache for user deleted.",
                            number_of_deleted_rows=delete_count,
                            user_id=user_id,
                        )

                    # no last refresh date found -> list all available repositories
                    repos_all = integration.list_available_private_repositories(
                        token=token,
                        update_token=get_update_token_callback(g, credential),
                        provider_user_id=userinfo.sub if userinfo else None,
                        user_organization_name_list=user_organization_name_list,
                    )
                    _save_repos_to_repos_cache(g=g, user_id=user_id, repo_list=repos_all)
                    _save_repos_last_refresh_date(g=g, user_id=user_id, integration_type=credential.integration_type)
                    logger.debug(
                        "Available private repositories for user is saved to repos cache.",
                        integration_name=credential_fresh.integration_name,
                        number_of_collected_private_repositories=len(repos_all),
                    )
                    g.kvstore.delete_value(refresh_in_progress_key)
                else:
                    g.kvstore.delete_value(refresh_in_progress_key)

            else:
                g.kvstore.delete_value(refresh_in_progress_key)
                logger.error(
                    "Cannot get fresh credential",
                    credential_id=credential.id,
                    owner_id=credential.owner_id,
                    integration_name=credential.integration_name,
                )
        except Exception:  # pylint: disable=broad-except
            g.kvstore.delete_value(refresh_in_progress_key)
            logger.exception(
                "Error during collecting repositories",
                integration_name=credential.integration_name,
                credential_id=credential.id,
                user_id=user_id,
            )
    elif is_in_progress:
        logger.info(
            "Repository cache refresh is currently in progress for user with integration type.",
            user_id=user_id,
            integration_type=credential.integration_type,
        )


def list_repositories(g: GitentialContext, workspace_id: int) -> List[RepositoryInDB]:
    all_projects = g.backend.projects.all(workspace_id)
    project_ids = [project.id for project in all_projects]

    repos = {}

    # HACK: Needed for ssh repositories
    # For some strange reason the ssh repos are not connected to a project in the project repositories table.
    for repo in g.backend.repositories.all(workspace_id=workspace_id):
        if repo.credential_id is not None and repo.protocol == GitProtocol.ssh:
            repos[repo.id] = repo

    for project_id in project_ids:
        for repo_id in g.backend.project_repositories.get_repo_ids_for_project(
            workspace_id=workspace_id, project_id=project_id
        ):
            if repo_id not in repos:
                repository = g.backend.repositories.get(workspace_id=workspace_id, id_=repo_id)
                if repository:
                    repos[repo_id] = repository
    return list(repos.values())


def list_project_repositories(g: GitentialContext, workspace_id: int, project_id: int) -> List[RepositoryInDB]:
    ret = []
    for repo_id in g.backend.project_repositories.get_repo_ids_for_project(
        workspace_id=workspace_id, project_id=project_id
    ):
        repository = g.backend.repositories.get(workspace_id=workspace_id, id_=repo_id)
        if repository:
            ret.append(repository)
    return ret


def search_public_repositories(g: GitentialContext, workspace_id: int, search: str) -> List[RepositoryCreate]:
    results: List[RepositoryCreate] = []

    for credential_ in list_credentials_for_workspace(g, workspace_id):
        if credential_.integration_type in REPOSITORY_SOURCES and credential_.integration_name in g.integrations:
            userinfo: UserInfoInDB = find_first(
                lambda ui: ui.integration_name == credential_.integration_name,  # pylint: disable=cell-var-from-loop
                g.backend.user_infos.get_for_user(credential_.owner_id),
            )
            try:
                # with acquire_credential(g, credential_id=credential_.id) as credential:
                credential = get_fresh_credential(g, credential_id=credential_.id)
                if credential:
                    integration = g.integrations[credential.integration_name]
                    token = credential.to_token_dict(fernet=g.fernet)
                    results += integration.search_public_repositories(
                        query=search,
                        token=token,
                        update_token=get_update_token_callback(g, credential),
                        provider_user_id=userinfo.sub if userinfo else None,
                    )
                else:
                    logger.error(
                        "Cannot get fresh credential",
                        credential_id=credential_.id,
                        owner_id=credential_.owner_id,
                        integration_name=credential_.integration_name,
                    )
            except:  # pylint: disable=bare-except
                logger.exception("Error during public repo search")

    return sorted(results, key=lambda i: levenshtein(search, i.name))


def create_repositories(
    g: GitentialContext, workspace_id: int, repository_creates: List[RepositoryCreate]
) -> List[RepositoryInDB]:
    return [
        g.backend.repositories.create_or_update_by_clone_url(workspace_id, repository_create)
        for repository_create in repository_creates
    ]


def delete_repositories(g: GitentialContext, workspace_id: int, repository_ids: List[int]):
    for project in g.backend.projects.all(workspace_id):
        g.backend.project_repositories.remove_repo_ids_from_project(workspace_id, project.id, repository_ids)

    for repo_id in repository_ids:
        g.backend.repositories.delete(workspace_id, repo_id)

    return True


def get_available_repo_groups(g: GitentialContext, workspace_id: int) -> List[UserRepositoryGroup]:
    user_id: int = get_workspace_creator_user_id(g=g, workspace_id=workspace_id)
    repo_groups = g.backend.repositories.get_repo_groups_with_repo_cache(workspace_id=workspace_id, user_id=user_id)
    if not is_list_not_empty(repo_groups):
        user_id_validated = get_user_id_or_raise_exception(
            g=g, workspace_id=workspace_id, is_at_least_one_id_is_needed=True
        )
        refresh_cache_of_repositories_for_user_or_users(g=g, user_id=user_id_validated, refresh_cache=True)
        repo_groups = g.backend.repositories.get_repo_groups_with_repo_cache(workspace_id=workspace_id, user_id=user_id)
    return repo_groups


def _save_repos_to_repos_cache(g: GitentialContext, user_id: int, repo_list: List[RepositoryCreate]):
    def get_repo_provider_id(repo: RepositoryCreate) -> Optional[str]:
        result = None
        if isinstance(repo.extra, dict):
            if "id" in repo.extra:
                result = str(repo.extra["id"])
            elif "uuid" in repo.extra:
                result = repo.extra["uuid"]
        return result

    repos_to_cache: List[UserRepositoryCacheCreate] = [
        UserRepositoryCacheCreate(
            user_id=user_id,
            repo_provider_id=get_repo_provider_id(repo),
            clone_url=repo.clone_url,
            protocol=repo.protocol,
            name=repo.name,
            namespace=repo.namespace,
            private=repo.private,
            integration_type=repo.integration_type,
            integration_name=repo.integration_name,
            credential_id=repo.credential_id,
            extra=repo.extra,
        )
        for repo in repo_list
    ]
    g.backend.user_repositories_cache.insert_repositories_cache_for_user(repos_to_cache)


def _get_repos_last_refresh_kvstore_key(user_id: int, integration_type: str):
    return f"repository_cache_for_user_last_refresh_datetime--{integration_type}--{user_id}"


def _get_repos_last_refresh_date(g: GitentialContext, user_id: int, integration_type: str) -> Optional[datetime]:
    result = None
    redis_key = _get_repos_last_refresh_kvstore_key(user_id, integration_type)
    refresh_raw = g.kvstore.get_value(redis_key)
    if is_string_not_empty(refresh_raw):
        try:
            result = parse_date_str(refresh_raw).replace(tzinfo=timezone.utc)
        except ValueError:
            logger.debug(f"Last refresh date is invalid for user_id: {user_id}")
    return result


def _save_repos_last_refresh_date(g: GitentialContext, user_id: int, integration_type: str):
    refresh_save = str(datetime.utcnow())
    redis_key = _get_repos_last_refresh_kvstore_key(user_id, integration_type)
    g.kvstore.set_value(redis_key, refresh_save)


def _get_repos_cache(g: GitentialContext, user_id: int) -> List[RepositoryCreate]:
    collected_repositories_cache: List[
        UserRepositoryCacheInDB
    ] = g.backend.user_repositories_cache.get_all_repositories_for_user(user_id)
    return [
        RepositoryCreate(
            clone_url=repo.clone_url,
            protocol=repo.protocol,
            name=repo.name,
            namespace=repo.namespace,
            private=repo.private,
            integration_type=repo.integration_type,
            integration_name=repo.integration_name,
            extra=repo.extra,
        )
        for repo in collected_repositories_cache
    ]
