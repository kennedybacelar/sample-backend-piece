from typing import Optional, List
from pathlib import Path
import base64
import tempfile
from concurrent.futures import ThreadPoolExecutor
from structlog import get_logger
from pydantic.datetime_parse import parse_datetime
from cryptography.fernet import Fernet
from gitential2.core.emails import send_email_to_address
from gitential2.datatypes.refresh import RefreshStrategy
from gitential2.core.context import GitentialContext
from gitential2.datatypes import AutoExportCreate, AutoExportInDB
from gitential2.core.refresh_v2 import refresh_workspace


logger = get_logger(__name__)


def encrypting_tempo_access_token(g: GitentialContext, tempo_access_token: str) -> Optional[str]:
    key = g.settings.connections.s3.secret_key
    if key:
        encoded_key = base64.urlsafe_b64encode(key.encode())
        f = Fernet(encoded_key)
        encoded_tempo_access_token = f.encrypt(tempo_access_token.encode())
        encoded_tempo_access_token_str = base64.urlsafe_b64encode(encoded_tempo_access_token).decode("utf-8")
        return encoded_tempo_access_token_str
    logger.info("s3.secret_key not found")
    return None


def decrypting_tempo_access_token(g: GitentialContext, encrypted_tempo_access_token: str) -> Optional[str]:
    key = g.settings.connections.s3.secret_key
    if key:
        encoded_key = base64.urlsafe_b64encode(key.encode())
        f = Fernet(encoded_key)
        decoded_tempo_access_token = base64.urlsafe_b64decode(encrypted_tempo_access_token.encode())
        decrypted_tempo_access_token = f.decrypt(decoded_tempo_access_token).decode()
        return decrypted_tempo_access_token
    logger.info("s3.secret_key not found")
    return None


def create_auto_export(
    g: GitentialContext,
    workspace_id: int,
    emails: List[str],
    **kwargs,
) -> Optional[AutoExportInDB]:

    extra = dict(kwargs)
    if extra.get("tempo_access_token"):
        extra["tempo_access_token"] = encrypting_tempo_access_token(g, extra["tempo_access_token"])
    return g.backend.auto_export.create(AutoExportCreate(workspace_id=workspace_id, emails=emails, extra=extra))


def auto_export_workspace(g: GitentialContext, workspace_to_export: AutoExportInDB):

    # pylint: disable=import-outside-toplevel,cyclic-import
    from gitential2.cli_v2.export import export_full_workspace, ExportFormat
    from gitential2.cli_v2.jira import lookup_tempo_worklogs

    logger.info("Auto export process started for workspace", workspace_id=workspace_to_export.workspace_id)
    refresh_workspace(g=g, workspace_id=workspace_to_export.workspace_id, strategy=RefreshStrategy.one_by_one)
    if workspace_to_export.extra:
        export_params = workspace_to_export.extra
        export_params["date_from"] = parse_datetime(export_params["date_from"])
        if export_params.get("tempo_access_token"):
            logger.info("Running lookup tempo JIRA", workspace_id=workspace_to_export.workspace_id)
            encrypted_tempo_access_token = decrypting_tempo_access_token(g, export_params["tempo_access_token"])
            if encrypted_tempo_access_token:
                lookup_tempo_worklogs(
                    g=g,
                    workspace_id=workspace_to_export.workspace_id,
                    tempo_access_token=encrypted_tempo_access_token,
                    force=True,
                    date_from=export_params["date_from"],
                    rewrite_existing_worklogs=False,
                )
        with tempfile.TemporaryDirectory() as tmp_dir:
            logger.info(f"Export file temporarily stored in {tmp_dir}", workspace_id=workspace_to_export.workspace_id)
            export_full_workspace(
                workspace_id=workspace_to_export.workspace_id,
                export_format=ExportFormat.xlsx,
                date_from=export_params["date_from"],
                destination_directory=Path(tmp_dir),
                upload_to_aws_s3=True,
                aws_s3_location=Path(export_params["aws_s3_location"]),
                prefix=_get_prefix_filename(g, workspace_to_export),
            )
        _send_workspace_export_data_via_email(
            g, workspace_to_export.workspace_id, workspace_to_export.emails, str(export_params["aws_s3_location"])
        )


def process_auto_export_for_all_workspaces(  # type: ignore[return]
    g: GitentialContext,
) -> bool:
    workspaces_to_be_exported = g.backend.auto_export.all()
    with ThreadPoolExecutor() as executor:
        for workspace_to_export in workspaces_to_be_exported:
            if workspace_to_export.extra:
                if g.current_time().weekday() in workspace_to_export.extra.get("weekday_numbers", []):
                    executor.submit(auto_export_workspace, g, workspace_to_export)


def _get_s3_upload_url(g: GitentialContext, file_path_str: str) -> str:
    bucket_name = g.settings.connections.s3.bucket_name
    return f"https://s3.console.aws.amazon.com/s3/buckets/{bucket_name}?prefix={file_path_str}/&showversions=false"


def _get_prefix_filename(g: GitentialContext, workspace_to_export: AutoExportInDB):
    return f"{g.current_time().strftime('%Y%m%d')}_ws_{workspace_to_export.workspace_id}_auto_"


def _send_workspace_export_data_via_email(
    g: GitentialContext, workspace_id: int, recipient_list: list, file_path_str: str
):
    logger.info("Starting Email dispatch process")
    s3_upload_url = _get_s3_upload_url(g, file_path_str)
    for recipient in recipient_list:
        send_email_to_address(g, recipient, "export_workspace", workspace_id=workspace_id, s3_upload_url=s3_upload_url)
    logger.info(msg="Email dispatch complete")
