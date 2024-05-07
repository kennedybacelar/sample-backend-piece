import typer
from structlog import get_logger
from gitential2.datatypes.refresh import RefreshType, RefreshStrategy
from gitential2.core.refresh_v2 import refresh_workspace, refresh_project, refresh_repository
from gitential2.core.tasks import schedule_task, configure_celery

from .common import get_context

app = typer.Typer()
logger = get_logger(__name__)


@app.command("workspace")
def refresh_workspace_(
    workspace_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
    force: bool = False,
    schedule: bool = False,
):
    g = get_context()
    configure_celery(g.settings)

    if schedule:
        schedule_task(
            g,
            task_name="refresh_workspace",
            params={"workspace_id": workspace_id, "strategy": strategy, "refresh_type": refresh_type, "force": force},
        )
    else:
        refresh_workspace(g, workspace_id, strategy=strategy, refresh_type=refresh_type, force=force)


@app.command("project")
def refresh_project_(
    workspace_id: int,
    project_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
    force: bool = False,
    schedule: bool = False,
):
    g = get_context()
    configure_celery(g.settings)
    if schedule:
        schedule_task(
            g,
            task_name="refresh_project",
            params={
                "workspace_id": workspace_id,
                "project_id": project_id,
                "strategy": strategy,
                "refresh_type": refresh_type,
                "force": force,
            },
        )
    else:
        refresh_project(g, workspace_id, project_id, strategy=strategy, refresh_type=refresh_type, force=force)


@app.command("repository")
def refresh_repository_(
    workspace_id: int,
    repository_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
    force: bool = False,
    schedule: bool = False,
):
    g = get_context()
    configure_celery(g.settings)
    if schedule:
        schedule_task(
            g,
            task_name="refresh_repository",
            params={
                "workspace_id": workspace_id,
                "repository_id": repository_id,
                "strategy": strategy,
                "refresh_type": refresh_type,
                "force": force,
            },
        )
    refresh_repository(g, workspace_id, repository_id, strategy=strategy, refresh_type=refresh_type, force=force)
