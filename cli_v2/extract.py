import typer
from structlog import get_logger

from gitential2.core.refresh_v2 import extract_project_branches
from gitential2.core.tasks import configure_celery, schedule_task
from gitential2.datatypes.refresh import RefreshStrategy

from gitential2.cli_v2.common import get_context

app = typer.Typer()

logger = get_logger(__name__)


@app.command("branches")
def extract_branches(
    workspace_id: int,
    project_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
):
    g = get_context()
    configure_celery(g.settings)
    if strategy == RefreshStrategy.parallel:
        schedule_task(
            g,
            task_name="extract_project_branches",
            params={
                "workspace_id": workspace_id,
                "project_id": project_id,
                "strategy": strategy,
            },
        )
    else:
        extract_project_branches(g, workspace_id, project_id, strategy=strategy)
