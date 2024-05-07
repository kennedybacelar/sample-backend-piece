from typing import Optional
from datetime import datetime
from structlog import get_logger
import typer

from gitential2.core.its import refresh_its_project
from .common import get_context

app = typer.Typer()
logger = get_logger(__name__)


@app.command("refresh")
def refresh_its_project_(
    workspace_id: int,
    itsp_id: int,
    date_from: Optional[datetime] = None,
    force: bool = False,
):
    g = get_context()
    refresh_its_project(g, workspace_id, itsp_id, date_from=date_from, force=force)
