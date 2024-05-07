from pathlib import Path
from typing import Optional, List

import typer
from structlog import get_logger

from gitential2.core.credentials import acquire_credential
from gitential2.core.repositories import (
    list_project_repositories,
    list_repositories,
    get_available_repositories_for_workspace,
)
from gitential2.extraction.repository import clone_repository
from .common import get_context, print_results, OutputFormat

app = typer.Typer()
logger = get_logger(__name__)


@app.command("list")
def list_repositories_(
    workspace_id: int,
    project_id: Optional[int] = None,
    available: bool = False,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
    organizations_name_list: Optional[List[str]] = None,
):
    g = get_context()
    results: list = []
    if available:
        results = get_available_repositories_for_workspace(g, workspace_id, organizations_name_list)
    elif project_id:
        results = list_project_repositories(g, workspace_id, project_id)
    else:
        results = list_repositories(g, workspace_id)

    print_results(results, format_=format_, fields=fields)


@app.command("clone")
def clone_repository_(workspace_id: int, repo_id: int, destination_path: Path):
    g = get_context()
    repository = g.backend.repositories.get_or_error(workspace_id, repo_id)
    with acquire_credential(
        g,
        credential_id=repository.credential_id,
        workspace_id=workspace_id,
        integration_name=repository.integration_name,
        blocking_timeout_seconds=30,
    ) as credential:
        clone_repository(
            repository,
            destination_path,
            credentials=credential.to_repository_credential(g.fernet) if credential else None,
        )
