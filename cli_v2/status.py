from typing import Optional
import typer
from structlog import get_logger

from gitential2.core.refresh_v2 import get_repo_refresh_status

from .common import get_context, print_results, OutputFormat

app = typer.Typer()
logger = get_logger(__name__)


@app.command("repository")
def repository_status_(
    workspace_id: int,
    repository_id: int,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    status = get_repo_refresh_status(g, workspace_id, repository_id)
    print_results([status], format_=format_, fields=fields)


@app.command("project")
def project_status_(
    workspace_id: int,
    project_id: int,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    statuses = [
        get_repo_refresh_status(g, workspace_id, repo_id)
        for repo_id in g.backend.project_repositories.get_repo_ids_for_project(workspace_id, project_id)
    ]

    print_results(statuses, format_=format_, fields=fields)
