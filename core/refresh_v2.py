import traceback
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Callable, Optional
from sqlalchemy import exc

from structlog import get_logger
from gitential2.datatypes.authors import AuthorAlias
from gitential2.datatypes.refresh import RefreshStrategy, RefreshType
from gitential2.datatypes.refresh_statuses import RefreshCommitsPhase, ITSProjectRefreshPhase, RepositoryRefreshStatus
from gitential2.datatypes.repositories import GitRepositoryState, RepositoryInDB
from gitential2.datatypes.extraction import LocalGitRepository
from gitential2.datatypes.credentials import CredentialInDB

from gitential2.utils.tempdir import TemporaryDirectory
from gitential2.extraction.repository import extract_incremental_local, clone_repository, extract_branches
from gitential2.exceptions import LockError

from .calculations import recalculate_repository_values
from .context import GitentialContext
from .authors import (
    fix_author_aliases,
    get_or_create_optional_author_for_alias,
    fix_author_names,
    force_filling_of_author_names,
)
from .tasks import schedule_task
from .credentials import acquire_credential, get_fresh_credential, get_update_token_callback
from .repositories import list_project_repositories
from .refresh_statuses import get_repo_refresh_status, update_repo_refresh_status
from .its import get_itsp_status, list_project_its_projects, refresh_its_project, update_itsp_status
from .deploys import recalculate_deploy_commits

logger = get_logger(__name__)


def refresh_workspace(
    g: GitentialContext,
    workspace_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
    force: bool = False,
):
    projects = g.backend.projects.all(workspace_id)
    if strategy == RefreshStrategy.parallel:
        for p in projects:
            schedule_task(
                g,
                task_name="refresh_project",
                params={
                    "workspace_id": workspace_id,
                    "project_id": p.id,
                    "strategy": strategy,
                    "refresh_type": refresh_type,
                    "force": force,
                },
            )
    else:
        for p in projects:
            refresh_project(
                g, workspace_id=workspace_id, project_id=p.id, strategy=strategy, refresh_type=refresh_type, force=force
            )


def maintain_workspace(
    g: GitentialContext,
    workspace_id: int,
):
    refresh_all_repositories(g, workspace_id)
    refresh_all_its_projects(g, workspace_id)
    fix_author_names(g, workspace_id)
    fix_author_aliases(g, workspace_id)
    recalculate_deploy_commits(g, workspace_id)
    force_filling_of_author_names(g, workspace_id)


def refresh_all_repositories(g: GitentialContext, workspace_id: int):
    projects = g.backend.projects.all(workspace_id)
    repositories = []
    repositories_processed = []
    refresh_interval = timedelta(minutes=g.settings.refresh.interval_minutes)

    for project in projects:
        repositories += list_project_repositories(g, workspace_id, project.id)

    for repository in repositories:
        if repository.id not in repositories_processed:
            logger.debug(
                "Running maintanance task for repository", workspace_id=workspace_id, repository_id=repository.id
            )
            repository_status = get_repo_refresh_status(g, workspace_id, repository.id)

            if repository_status.prs_last_run and g.current_time() - repository_status.prs_last_run > refresh_interval:
                logger.info(
                    "Scheduling repository prs refresh",
                    workspace_id=workspace_id,
                    repository_id=repository.id,
                    repository_name=repository.name,
                )
                schedule_task(
                    g,
                    task_name="refresh_repository",
                    params={
                        "workspace_id": workspace_id,
                        "repository_id": repository.id,
                        "strategy": RefreshStrategy.one_by_one,
                        "refresh_type": RefreshType.prs_only,
                        "force": False,
                    },
                )
            if (
                repository_status.commits_last_run
                and g.current_time() - repository_status.commits_last_run > refresh_interval
            ):
                logger.info(
                    "Scheduling repository commits refresh",
                    workspace_id=workspace_id,
                    repository_id=repository.id,
                    repository_name=repository.name,
                )
                schedule_task(
                    g,
                    task_name="refresh_repository",
                    params={
                        "workspace_id": workspace_id,
                        "repository_id": repository.id,
                        "strategy": RefreshStrategy.one_by_one,
                        "refresh_type": RefreshType.commits_only,
                        "force": False,
                    },
                )

            repositories_processed.append(repository.id)


def refresh_all_its_projects(g: GitentialContext, workspace_id: int):
    projects = g.backend.projects.all(workspace_id)
    its_projects = []
    its_projects_processed = []
    refresh_interval = timedelta(minutes=g.settings.refresh.interval_minutes)

    for project in projects:
        its_projects += list_project_its_projects(g, workspace_id, project.id)

    for itsp in its_projects:
        if itsp.id not in its_projects_processed:
            logger.debug("Running maintanance task for its project", workspace_id=workspace_id, itsp_id=itsp.id)
            itsp_status = get_itsp_status(g, workspace_id, itsp.id)
            if (
                itsp_status.started_at and g.current_time() - itsp_status.started_at > refresh_interval
            ) or itsp_status.phase == ITSProjectRefreshPhase.unknown:
                logger.info(
                    "Scheduling its project refresh",
                    workspace_id=workspace_id,
                    repository_id=itsp.id,
                    repository_name=itsp.name,
                )
                update_itsp_status(g, workspace_id, itsp.id, phase=ITSProjectRefreshPhase.scheduled)

                schedule_task(
                    g,
                    task_name="refresh_its_project",
                    params={
                        "workspace_id": workspace_id,
                        "itsp_id": itsp.id,
                        "strategy": RefreshStrategy.parallel,
                        "refresh_type": RefreshType.its_only,
                        "force": False,
                    },
                )
            its_projects_processed.append(itsp.id)


def refresh_project(
    g: GitentialContext,
    workspace_id: int,
    project_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
    force: bool = False,
):
    if refresh_type in [
        RefreshType.everything,
        RefreshType.prs_only,
        RefreshType.commits_only,
        RefreshType.commit_calculations_only,
    ]:
        for repo_id in g.backend.project_repositories.get_repo_ids_for_project(workspace_id, project_id):
            commits_refresh_scheduled = refresh_type in [RefreshType.everything, RefreshType.commits_only]
            prs_refresh_scheduled = refresh_type in [RefreshType.everything, RefreshType.prs_only]
            update_repo_refresh_status(
                g,
                workspace_id,
                repo_id,
                commits_refresh_scheduled=commits_refresh_scheduled,
                prs_refresh_scheduled=prs_refresh_scheduled,
            )
            if strategy == RefreshStrategy.parallel:
                schedule_task(
                    g,
                    task_name="refresh_repository",
                    params={
                        "workspace_id": workspace_id,
                        "repository_id": repo_id,
                        "strategy": strategy,
                        "refresh_type": refresh_type,
                        "force": force,
                    },
                )
            else:
                refresh_repository(g, workspace_id, repo_id, strategy, refresh_type, force=force)
    if refresh_type in [RefreshType.everything, RefreshType.its_only]:
        for itsp_id in g.backend.project_its_projects.get_itsp_ids_for_project(workspace_id, project_id):
            if strategy == RefreshStrategy.parallel:
                itsp_status = get_itsp_status(g, workspace_id, itsp_id)
                if itsp_status.phase not in [ITSProjectRefreshPhase.running, ITSProjectRefreshPhase.scheduled]:
                    update_itsp_status(g, workspace_id, itsp_id, phase=ITSProjectRefreshPhase.scheduled)
                    schedule_task(
                        g,
                        task_name="refresh_its_project",
                        params={
                            "workspace_id": workspace_id,
                            "itsp_id": itsp_id,
                            "strategy": strategy,
                            "refresh_type": refresh_type,
                            "force": force,
                        },
                    )
            else:
                refresh_its_project(g, workspace_id, itsp_id, strategy, refresh_type, force=force)


def refresh_repository(
    g: GitentialContext,
    workspace_id: int,
    repository_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
    force: bool = False,
):
    repository = g.backend.repositories.get_or_error(workspace_id, repository_id)
    logger.info(
        "Repository refresh started",
        workspace_id=workspace_id,
        repository_id=repository_id,
        repository_name=repository.name,
        strategy=strategy,
        refresh_type=refresh_type,
        force=force,
    )
    # Delegating tasks
    if strategy == RefreshStrategy.parallel and refresh_type == RefreshType.everything:
        schedule_task(
            g,
            task_name="refresh_repository",
            params={
                "workspace_id": workspace_id,
                "repository_id": repository_id,
                "strategy": strategy,
                "refresh_type": RefreshType.commits_only,
                "force": force,
            },
        )
        schedule_task(
            g,
            task_name="refresh_repository",
            params={
                "workspace_id": workspace_id,
                "repository_id": repository_id,
                "strategy": strategy,
                "refresh_type": RefreshType.prs_only,
                "force": force,
            },
        )
        logger.info("Delegated tasks, finishing")
        return

    if refresh_type in [RefreshType.commits_only, RefreshType.everything]:
        refresh_repository_commits(g, workspace_id, repository_id, force)
    if refresh_type in [RefreshType.prs_only, RefreshType.everything]:
        refresh_repository_pull_requests(g, workspace_id, repository_id, force)
    if refresh_type == RefreshType.commit_calculations_only:
        recalculate_repository_values(g, workspace_id, repository_id)


def refresh_repository_commits(g: GitentialContext, workspace_id: int, repository_id: int, force: bool = False):
    repository = g.backend.repositories.get_or_error(workspace_id, repository_id)
    refresh_status = get_repo_refresh_status(g, workspace_id, repository_id)
    _update_state = partial(update_repo_refresh_status, g=g, workspace_id=workspace_id, repository_id=repository_id)
    if (
        refresh_status.commits_in_progress
        and refresh_status.commits_started
        and g.current_time() - refresh_status.commits_started < timedelta(hours=8)
        and not force
    ):
        logger.info(
            "Skipping commits refresh, another job is already in progress",
            workspace_id=workspace_id,
            repository_id=repository_id,
        )
        _update_state(
            commits_refresh_scheduled=False,
        )
        return
    if (
        refresh_status.commits_last_successful_run
        and g.current_time() - refresh_status.commits_last_successful_run < timedelta(minutes=30)
        and not force
    ):
        logger.info(
            "Skipping commits refresh, last successful refresh was not at least 30 minute ago",
            workspace_id=workspace_id,
            repository_id=repository_id,
        )
        _update_state(
            commits_refresh_scheduled=False,
        )
        return

    with TemporaryDirectory() as workdir:
        try:
            local_repo = _refresh_repository_commits_clone_phase(
                g, workspace_id, repository, workdir, _update_state, force
            )
            if local_repo:
                _refresh_repository_commits_extract_phase(g, workspace_id, repository, local_repo, _update_state, force)
                _refresh_repository_commits_persist_phase(g, workspace_id, repository_id, _update_state)

            _update_state(
                commits_phase=RefreshCommitsPhase.done,
                commits_in_progress=False,
                commits_error=False,
                commits_error_msg="",
                commits_last_successful_run=g.current_time(),
                commits_last_run=g.current_time(),
            )
        except exc.DBAPIError as err:
            # If error includes a .connection_invalidated
            # attribute it means this connection experienced a "disconnect"
            if err.connection_invalidated:
                # Rerun persist phase
                _refresh_repository_commits_persist_phase(g, workspace_id, repository_id, _update_state)
                _update_state(
                    commits_phase=RefreshCommitsPhase.done,
                    commits_in_progress=False,
                    commits_error=False,
                    commits_error_msg="",
                    commits_last_successful_run=g.current_time(),
                    commits_last_run=g.current_time(),
                )
            else:
                raise err
        except LockError:
            logger.warning("Failed to acquire lock, maybe rescheduling")
            raise
        except:  # pylint: disable=bare-except
            logger.exception(
                "Unexpected error with commits extraction.",
                workspace_id=workspace_id,
                repository_id=repository_id,
                repository_name=repository.name,
            )
            _update_state(
                commits_phase=RefreshCommitsPhase.done,
                commits_in_progress=False,
                commits_error=True,
                commits_error_msg=traceback.format_exc(limit=1),
                commits_last_run=g.current_time(),
            )


def _refresh_repository_commits_clone_phase(
    g: GitentialContext,
    workspace_id: int,
    repository: RepositoryInDB,
    workdir: TemporaryDirectory,
    _update_state: Callable,
    force: bool,
) -> Optional[LocalGitRepository]:
    logger.info(
        "Cloning repository",
        workspace_id=workspace_id,
        repository_id=repository.id,
        repository_name=repository.name,
    )

    # with acquire_credential(
    #     g,
    #     credential_id=repository.credential_id,
    #     workspace_id=workspace_id,
    #     integration_name=repository.integration_name,
    #     blocking_timeout_seconds=30,
    # ) as credential:
    credential = get_fresh_credential(
        g,
        credential_id=repository.credential_id,
        workspace_id=workspace_id,
        integration_name=repository.integration_name,
    )
    if credential:
        _update_state(
            commits_in_progress=True,
            commits_refresh_scheduled=False,
            commits_error=False,
            commits_started=g.current_time(),
            commits_phase=RefreshCommitsPhase.cloning,
        )

        if _should_skip_refresh_clone_phase(g, credential, workspace_id, repository, force):
            return None

        local_repo = clone_repository(
            repository,
            destination_path=workdir.path,
            credentials=credential.to_repository_credential(g.fernet) if credential else None,
        )
        return local_repo
    return None


def _should_skip_refresh_clone_phase(
    g: GitentialContext, credential: CredentialInDB, workspace_id: int, repository: RepositoryInDB, force: bool
) -> bool:

    try:
        integration = g.integrations.get(repository.integration_name)
        if hasattr(integration, "last_push_at_repository") and not force and integration is not None:

            token = credential.to_token_dict(g.fernet)
            update_token = get_update_token_callback(g, credential)
            last_push_at_repository: Optional[datetime] = integration.last_push_at_repository(
                repository=repository, token=token, update_token=update_token
            )
            current_state = get_repo_refresh_status(g, workspace_id, repository.id)

            if not _has_remote_repository_been_updated_after_last_project_refresh(
                last_push_at_repository, current_state
            ):
                logger.info(
                    "Remote repository has not been updated after last successful refresh - Skipping commits refresh.",
                    workspace_id=workspace_id,
                    repository_id=repository.id,
                )
                return True
        return False
    except Exception as error:  # pylint: disable=broad-except
        logger.exception(
            "Unexpected error with _should_skip_refresh_clone_phase.",
            workspace_id=current_state.workspace_id,
            repository_id=current_state.repository_id,
            repository_name=current_state.repository_name,
            error=error,
        )
        return False


def _has_remote_repository_been_updated_after_last_project_refresh(
    last_push_at_remote_repository: Optional[datetime], current_state: RepositoryRefreshStatus
) -> bool:
    try:
        if last_push_at_remote_repository:
            last_push_at_remote_repository = last_push_at_remote_repository.astimezone(timezone.utc)
        repo_last_successful_refresh = current_state.commits_last_successful_run

        logger.info(
            workspace_id=current_state.workspace_id,
            repository_id=current_state.repository_id,
            repository_name=current_state.repository_name,
            repo_last_successful_refresh=repo_last_successful_refresh,
            last_push_at_remote_repository=last_push_at_remote_repository,
        )

        if last_push_at_remote_repository and repo_last_successful_refresh:
            return repo_last_successful_refresh < last_push_at_remote_repository
        return True
    except Exception as error:  # pylint: disable=broad-except
        logger.exception(
            "Unexpected error with has_remote_repository_been_updated_after_last_project_refresh.",
            workspace_id=current_state.workspace_id,
            repository_id=current_state.repository_id,
            repository_name=current_state.repository_name,
            error=error,
        )
        return True


def _refresh_repository_commits_extract_phase(
    g: GitentialContext,
    workspace_id: int,
    repository: RepositoryInDB,
    local_repo: LocalGitRepository,
    _update_state: Callable,
    force: bool,
):
    _update_state(
        commits_phase=RefreshCommitsPhase.extract,
    )

    commits_we_already_have = (
        g.backend.get_commit_ids_for_repository(workspace_id, repository.id) if not force else set()
    )
    previous_state = get_previous_extraction_state(g, workspace_id, repository.id) if not force else None

    logger.info(
        "Extracting commits from",
        workspace_id=workspace_id,
        repository_id=repository.id,
        repository_name=repository.name,
        commits_we_already_have=len(commits_we_already_have),
    )
    output = g.backend.output_handler(workspace_id)

    extraction_state = extract_incremental_local(
        local_repo,
        output=output,
        settings=g.settings,
        previous_state=previous_state,
        commits_we_already_have=commits_we_already_have,
    )
    set_extraction_state(g, workspace_id, repository.id, extraction_state)


def _refresh_repository_commits_persist_phase(
    g: GitentialContext, workspace_id: int, repository_id: int, _update_state: Callable
):
    _update_state(
        commits_phase=RefreshCommitsPhase.persist,
    )
    recalculate_repository_values(g, workspace_id, repository_id)


def _extraction_state_key(workspace_id: int, repository_id: int) -> str:
    return f"ws-{workspace_id}:r-{repository_id}:extraction"


def get_previous_extraction_state(
    g: GitentialContext, workspace_id: int, repository_id: int
) -> Optional[GitRepositoryState]:
    previous_state = g.kvstore.get_value(_extraction_state_key(workspace_id, repository_id))
    return GitRepositoryState(**previous_state) if previous_state and isinstance(previous_state, dict) else None


def set_extraction_state(g: GitentialContext, workspace_id: int, repository_id: int, state: GitRepositoryState):
    g.kvstore.set_value(_extraction_state_key(workspace_id, repository_id), state.dict())


def refresh_repository_pull_requests(g: GitentialContext, workspace_id: int, repository_id: int, force: bool = False):
    repository = g.backend.repositories.get_or_error(workspace_id, repository_id)
    prs_we_already_have = g.backend.pull_requests.get_prs_updated_at(workspace_id, repository_id) if not force else []
    _update_state = partial(update_repo_refresh_status, g=g, workspace_id=workspace_id, repository_id=repository_id)
    _author_callback_partial = partial(_author_callback, g=g, workspace_id=workspace_id)

    def _end_processing_no_error():
        current_time = g.current_time()
        _update_state(
            prs_last_successful_run=current_time,
            prs_last_run=current_time,
            prs_error=False,
            prs_in_progress=False,
            prs_refresh_scheduled=False,
            prs_error_msg="",
        )

    try:
        current_state = get_repo_refresh_status(g, workspace_id, repository_id)
        if (
            current_state.prs_in_progress
            and (not current_state.prs_started or (g.current_time() - current_state.prs_started) < timedelta(hours=4))
            and not force
        ):
            logger.info(
                "Skipping PR refresh, another process in progress",
                workspace_id=workspace_id,
                repository_id=repository_id,
                repository_name=repository.name,
            )
            return
        # with acquire_credential(
        #     g,
        #     credential_id=repository.credential_id,
        #     workspace_id=workspace_id,
        #     integration_name=repository.integration_name,
        #     blocking_timeout_seconds=30,
        # ) as credential:
        credential = get_fresh_credential(
            g,
            credential_id=repository.credential_id,
            workspace_id=workspace_id,
            integration_name=repository.integration_name,
        )
        if credential:
            _update_state(prs_in_progress=True, prs_refresh_scheduled=False, prs_started=g.current_time())

            integration = g.integrations.get(repository.integration_name)
            if not integration:
                logger.info(
                    "Skipping PR refresh: no integration", workspace_id=workspace_id, repository_id=repository_id
                )
                _end_processing_no_error()
                return

            output = g.backend.output_handler(workspace_id)

            if hasattr(integration, "collect_pull_requests"):
                token = credential.to_token_dict(g.fernet)

                collection_result = integration.collect_pull_requests(
                    repository=repository,
                    token=token,
                    update_token=get_update_token_callback(g, credential),
                    output=output,
                    author_callback=_author_callback_partial,
                    prs_we_already_have=prs_we_already_have,
                    limit=200,
                    repo_analysis_limit_in_days=g.settings.extraction.repo_analysis_limit_in_days,
                )
                logger.info(
                    "collect_pull_requests results",
                    repository_name=repository.name,
                    repository_id=repository.id,
                    workspace_id=workspace_id,
                    result=collection_result,
                )
                _end_processing_no_error()
            else:
                logger.info(
                    "Skipping PR refresh: collect_pull_requests not implemented",
                    workspace_id=workspace_id,
                    repository_id=repository_id,
                    integration=repository.integration_name,
                )
                _end_processing_no_error()
        else:
            logger.info("Skipping PR refresh: no credential", workspace_id=workspace_id, repository_id=repository_id)
            _end_processing_no_error()
            return
    except LockError:
        logger.warning("Failed to acquire lock, maybe rescheduling")
        raise
    except:  # pylint: disable=bare-except
        logger.exception(
            "Unexpected error with PR extraction.",
            workspace_id=workspace_id,
            repository_id=repository_id,
            repository_name=repository.name,
        )

        _update_state(
            prs_in_progress=False,
            prs_error=True,
            prs_error_msg=traceback.format_exc(limit=1),
            prs_last_run=g.current_time(),
        )


def extract_project_branches(
    g: GitentialContext,
    workspace_id: int,
    project_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
):
    for repo in list_project_repositories(g=g, workspace_id=workspace_id, project_id=project_id):
        if strategy == RefreshStrategy.parallel:
            schedule_task(
                g,
                task_name="extract_repository_branches",
                params={
                    "workspace_id": workspace_id,
                    "repository_id": repo.id,
                },
            )
        else:
            extract_repository_branches(g, workspace_id, repo.id)


def extract_repository_branches(g: GitentialContext, workspace_id: int, repository_id: int):
    repo = g.backend.repositories.get_or_error(workspace_id, repository_id)
    with TemporaryDirectory() as workdir:
        with acquire_credential(
            g,
            credential_id=repo.credential_id,
            workspace_id=workspace_id,
            integration_name=repo.integration_name,
            blocking_timeout_seconds=30,
        ) as credential:
            local_repo = clone_repository(
                repo,
                destination_path=workdir.path,
                credentials=credential.to_repository_credential(g.fernet) if credential else None,
            )
            output = g.backend.output_handler(workspace_id)
            extract_branches(g.settings, local_repo, output)


def _author_callback(
    alias: AuthorAlias,
    g: GitentialContext,
    workspace_id: int,
) -> Optional[int]:
    author = get_or_create_optional_author_for_alias(g, workspace_id, alias)
    if author:
        return author.id
    else:
        return None
