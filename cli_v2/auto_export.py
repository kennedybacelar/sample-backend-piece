from datetime import datetime
from typing import Optional, List
from pathlib import Path
from structlog import get_logger
import typer
from gitential2.core.export import create_auto_export, process_auto_export_for_all_workspaces
from .common import get_context, print_results, OutputFormat


logger = get_logger(__name__)
app = typer.Typer()


@app.command("create")
def create_auto_export_(
    workspace_id: int,
    emails: List[str],
    weekday_numbers: str = typer.Option("0,1,2,3,4", "--weekday-numbers"),
    tempo_access_token: Optional[str] = typer.Option(None, "--tempo-access-token"),
    date_from: Optional[datetime] = typer.Option(datetime.min, "--date-from"),
    aws_s3_location: Path = typer.Option(Path("Exports/production-cloud"), "--aws-s3-location"),
):
    """Create an entry in the list of workspace export schedules.

    Example usage:
    g2 auto-export create 1 john@example.com jane@example.com --tempo-access-token secret123 --date-from 2023-01-01 --weekday-numbers 2,5 --aws-s3-location exports/

    Make sure that the --aws-s3-location is an existing folder in the bucket specified in settings.yml file.
    """
    weekday_numbers_list_int = [int(x) for x in weekday_numbers.split(",")]
    for n in weekday_numbers_list_int:
        if not 0 <= int(n) <= 6:  # casting into int again to shut mypy errors
            raise ValueError(f"Invalid weekday number: {n}. Must be between 0 and 6.")

    g = get_context()
    workspace = g.backend.workspaces.get(id_=workspace_id)
    if workspace:
        auto_export_schedule = create_auto_export(
            g,
            workspace_id,
            emails,
            weekday_numbers=weekday_numbers_list_int,
            date_from=date_from,
            tempo_access_token=tempo_access_token,
            aws_s3_location=aws_s3_location,
        )
        print("Auto export successfully scheduled")
        print_results([auto_export_schedule], format_=OutputFormat.json)
    else:
        logger.info(f"Workspace {workspace_id} not found")
        raise typer.Exit(code=1)


@app.command("delete")
def delete_auto_export_schedule(workspace_id: int):
    """Deletes any existing workspace auto-export schedule for the given workspace_id"""
    g = get_context()

    count_ws_deleted = g.backend.auto_export.delete_rows_for_workspace(workspace_id)
    print(f"{count_ws_deleted} workspace(s) deleted")


@app.command("run")
def trigger_auto_export_for_all_workspaces():
    g = get_context()
    process_auto_export_for_all_workspaces(g)
