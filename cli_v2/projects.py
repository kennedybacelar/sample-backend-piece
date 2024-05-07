from typing import Optional
import typer
from structlog import get_logger

from gitential2.core.projects import list_projects
from .common import get_context, print_results, OutputFormat

app = typer.Typer()
logger = get_logger(__name__)


@app.command("list")
def list_projects_(
    workspace_id: int,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    results = list(list_projects(g, workspace_id))
    print_results(results, format_=format_, fields=fields)
