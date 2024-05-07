from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import cast, List, Optional
import typer
import boto3
from structlog import get_logger
from gitential2.export.exporters import Exporter, CSVExporter, JSONExporter, SQLiteExporter, XlsxExporter
from gitential2.backends.base.repositories import BaseWorkspaceScopedRepository

from .common import get_context, validate_directory_exists
from ..utils import get_schema_name

app = typer.Typer()

logger = get_logger(__name__)


class ExportFormat(str, Enum):
    csv = "csv"
    json = "json"
    sqlite = "sqlite"
    xlsx = "xlsx"


@app.command("full-workspace")
def export_full_workspace(
    workspace_id: int,
    destination_directory: Path = Path("/tmp"),
    export_format: ExportFormat = ExportFormat.csv,
    date_from: datetime = datetime.min,
    upload_to_aws_s3: bool = typer.Option(False, "--upload-to-aws-s3"),
    aws_s3_location: Path = Path("export-test/"),
    prefix: Optional[str] = None,
):
    validate_directory_exists(destination_directory)

    g = get_context()
    data_to_export = [
        ("projects", g.backend.projects),
        ("repositories", g.backend.repositories),
        ("project_repositories", g.backend.project_repositories),
        ("authors", g.backend.authors),
        ("teams", g.backend.teams),
        ("team_members", g.backend.team_members),
        ("calculated_commits", g.backend.calculated_commits),
        ("calculated_patches", g.backend.calculated_patches),
        ("extracted_commits", g.backend.extracted_commits),
        ("extracted_patches", g.backend.extracted_patches),
        ("extracted_patch_rewrites", g.backend.extracted_patch_rewrites),
        ("pull_requests", g.backend.pull_requests),
        ("pull_request_commits", g.backend.pull_request_commits),
        ("pull_request_comments", g.backend.pull_request_comments),
        ("pull_request_labels", g.backend.pull_request_labels),
        # its related tables
        ("project_its_projects", g.backend.project_its_projects),
        ("its_projects", g.backend.its_projects),
        ("its_issues", g.backend.its_issues),
        ("its_issue_changes", g.backend.its_issue_changes),
        ("its_issue_times_in_statuses", g.backend.its_issue_times_in_statuses),
        ("its_issue_comments", g.backend.its_issue_comments),
        ("its_issue_linked_issues", g.backend.its_issue_linked_issues),
        ("its_sprints", g.backend.its_sprints),
        ("its_issue_sprints", g.backend.its_issue_sprints),
        ("its_issue_worklogs", g.backend.its_issue_worklogs),
    ]

    def _date_filter(name, obj, date_from):
        skip_date_filter = [
            "projects",
            "repositories",
            "project_repositories",
            "authors",
            "teams",
            "team_members",
            "its_issue_sprints",
            "its_issue_linked_issues",
        ]
        if name in skip_date_filter or date_from == datetime.min:
            return True
        elif hasattr(obj, "created_at") and getattr(obj, "created_at", datetime.min) >= date_from:
            return True
        elif hasattr(obj, "started_at") and (getattr(obj, "started_at", datetime.min) or datetime.min) >= date_from:
            return True
        elif hasattr(obj, "date") and getattr(obj, "date", datetime.min) >= date_from:
            return True
        else:
            return False

    exporter = _get_exporter(export_format, destination_directory, workspace_id, prefix)

    for name, backend_repository in data_to_export:
        backend_repository = cast(BaseWorkspaceScopedRepository, backend_repository)
        logger.info("exporting", datatype=name, workspace_id=workspace_id, format=export_format)

        for obj in backend_repository.iterate_all(workspace_id=workspace_id):
            if _date_filter(name, obj, date_from):
                exporter.export_object(obj)

    exporter.close()

    if upload_to_aws_s3:
        _upload_to_aws_s3(exporter.get_files(), aws_s3_location)


@app.command("repositories")
def export_repositories(
    workspace_id: int,
    destination_directory: Path = Path("/tmp"),
    export_format: ExportFormat = ExportFormat.csv,
    upload_to_aws_s3: bool = typer.Option(False, "--upload-to-aws-s3"),
    aws_s3_location: Path = Path("export-test/"),
):
    validate_directory_exists(destination_directory)
    g = get_context()
    exporter = _get_exporter(export_format, destination_directory, workspace_id)
    for repository in g.backend.repositories.all(workspace_id=workspace_id):
        exporter.export_object(repository)
    exporter.close()

    if upload_to_aws_s3:
        _upload_to_aws_s3(exporter.get_files(), aws_s3_location)


@app.command("deploys")
def export_deploys(
    workspace_id: int,
    destination_directory: Path = Path(""),
    export_format: ExportFormat = ExportFormat.xlsx,
):
    validate_directory_exists(destination_directory)
    g = get_context()
    exporter = _get_exporter(export_format, destination_directory, workspace_id)
    for deploy in g.backend.deploys.all(workspace_id=workspace_id):
        exporter.export_object(deploy)
    exporter.close()


@app.command("deploy_commits")
def export_deploy_commits(
    workspace_id: int,
    destination_directory: Path = Path(""),
    export_format: ExportFormat = ExportFormat.xlsx,
):
    validate_directory_exists(destination_directory)
    g = get_context()
    exporter = _get_exporter(export_format, destination_directory, workspace_id)
    for deploy_commit in g.backend.deploy_commits.all(workspace_id=workspace_id):
        exporter.export_object(deploy_commit)
    exporter.close()


def _upload_to_aws_s3(list_of_files_to_be_uploaded_to_s3: List[str], aws_s3_location: Path):

    g = get_context()

    client = boto3.client(
        "s3",
        aws_access_key_id=g.settings.connections.s3.aws_access_key_id,
        aws_secret_access_key=g.settings.connections.s3.aws_secret_access_key,
    )

    for file in list_of_files_to_be_uploaded_to_s3:
        filename_into_s3_bucket = file.split("/")[-1]
        upload_file_key = Path.joinpath(aws_s3_location, filename_into_s3_bucket)
        client.upload_file(file, g.settings.connections.s3.bucket_name, str(upload_file_key))


def _get_exporter(
    export_format: ExportFormat, destination_directory: Path, workspace_id: int, prefix: Optional[str] = None
) -> Exporter:
    schema_name = get_schema_name(workspace_id)
    prefix = prefix or f"{schema_name}_"
    if export_format == ExportFormat.csv:
        return CSVExporter(destination_directory, prefix)
    elif export_format == ExportFormat.json:
        return JSONExporter(destination_directory, prefix)
    elif export_format == ExportFormat.sqlite:
        return SQLiteExporter(destination_directory, prefix)
    elif export_format == ExportFormat.xlsx:
        return XlsxExporter(destination_directory, prefix)
    raise ValueError("Invalid export format")
