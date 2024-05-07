from datetime import datetime
from functools import partial
from typing import Optional, cast

import typer
from structlog import get_logger

from gitential2.core.authors import developer_map_callback
from gitential2.core.context import GitentialContext
from gitential2.core.credentials import get_update_token_callback, get_fresh_credential
from gitential2.datatypes.credentials import CredentialInDB
from gitential2.datatypes.its_projects import ITSProjectInDB
from gitential2.datatypes.userinfos import UserInfoInDB
from gitential2.datatypes.repositories import RepositoryInDB
from gitential2.integrations.vsts import VSTSIntegration
from gitential2.settings import IntegrationType
from .common import get_context, print_results, OutputFormat

app = typer.Typer()
logger = get_logger(__name__)


def _get_vsts_credential(g: GitentialContext, workspace_id: int, integration_name="vsts") -> Optional[CredentialInDB]:
    return get_fresh_credential(g, workspace_id=workspace_id, integration_name=integration_name)


@app.command("get-project-process-id")
def _get_project_process_id(
    workspace_id: int,
    namespace: str,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):

    all_projects = list_available_projects(workspace_id=workspace_id, internal_call=True)
    for single_project in all_projects:
        if namespace == single_project.namespace:
            print_results([single_project.extra], format_=format_, fields=fields)
            break


@app.command("list-available-projects")
def list_available_projects(  # pylint: disable=inconsistent-return-statements
    workspace_id: int,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
    internal_call: bool = False,
):
    g = get_context()
    vsts_credential: Optional[CredentialInDB] = _get_vsts_credential(g, workspace_id)
    vsts_integration = g.integrations.get("vsts")

    if vsts_credential and vsts_credential.owner_id:
        for single_user in g.backend.user_infos.get_for_user(vsts_credential.owner_id):
            if single_user.integration_type == IntegrationType.vsts:
                userinfo: UserInfoInDB = single_user
                break

    if vsts_credential and vsts_integration:
        vsts_integration = cast(VSTSIntegration, vsts_integration)
        token = vsts_credential.to_token_dict(g.fernet)
        its_projects = vsts_integration.list_available_its_projects(
            token,
            update_token=get_update_token_callback(g, vsts_credential),
            provider_user_id=userinfo.sub if userinfo else None,
        )
        if internal_call:
            return its_projects
        print_results(its_projects, format_=format_, fields=fields)


@app.command("list-workitems-for-project")
def list_wit_projects(
    workspace_id: int,
    namespace: str,
    team: str,
    process_id: str = typer.Option(None, "--process-id"),
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):

    its_project_mock = ITSProjectInDB(
        name=team,
        namespace=namespace,
        id=10,
        integration_type="",
        integration_name="",
        integration_id="",
        extra={"process_id": process_id},
    )

    g = get_context()
    vsts_credential: Optional[CredentialInDB] = _get_vsts_credential(g, workspace_id)
    vsts_integration = g.integrations.get("vsts")

    if vsts_credential and vsts_integration:
        vsts_integration = cast(VSTSIntegration, vsts_integration)
        token = vsts_credential.to_token_dict(g.fernet)
        work_items = vsts_integration.list_all_issues_for_project(token=token, its_project=its_project_mock)
        print_results(work_items, format_=format_, fields=fields)


@app.command("list-recent-workitems")
def list_recent_wit_projects(
    workspace_id: int,
    namespace: str,
    team: str,
    date_from: datetime = typer.Option(None, "--date-from"),
    process_id: str = typer.Option(None, "--process-id"),
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):

    its_project_mock = ITSProjectInDB(
        name=team,
        namespace=namespace,
        id=10,
        integration_type="",
        integration_name="",
        integration_id="",
        extra={"process_id": process_id},
    )

    g = get_context()
    vsts_credential: Optional[CredentialInDB] = _get_vsts_credential(g, workspace_id)
    vsts_integration = g.integrations.get("vsts")

    if vsts_credential and vsts_integration:
        vsts_integration = cast(VSTSIntegration, vsts_integration)
        token = vsts_credential.to_token_dict(g.fernet)
        recent_work_items = vsts_integration.list_recently_updated_issues(
            token=token, its_project=its_project_mock, date_from=date_from
        )
        print_results(recent_work_items, format_=format_, fields=fields)


@app.command("list-all-data-issue")
def list_all_data_for_issue(
    workspace_id: int,
    namespace: str,
    team: str,
    issue_id_or_key: str = typer.Option(None, "--issue-id"),
    process_id: str = typer.Option(None, "--process-id"),
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):

    its_project_mock = ITSProjectInDB(
        name=team,
        namespace=namespace,
        id=10,
        integration_type="",
        integration_name="",
        integration_id="",
        extra={"process_id": process_id},
    )

    g = get_context()
    vsts_credential: Optional[CredentialInDB] = _get_vsts_credential(g, workspace_id)
    vsts_integration = g.integrations.get("vsts")
    dev_map_callback = partial(developer_map_callback, g=g, workspace_id=workspace_id)

    if vsts_credential and vsts_integration:
        vsts_integration = cast(VSTSIntegration, vsts_integration)
        token = vsts_credential.to_token_dict(g.fernet)
        all_data_issue = vsts_integration.get_all_data_for_issue(
            token=token,
            its_project=its_project_mock,
            issue_id_or_key=issue_id_or_key,
            developer_map_callback=dev_map_callback,
        )
        print_results([all_data_issue], format_=format_, fields=fields)


# pylint: disable=missing-param-doc
@app.command("raw-data-issue")
def list_raw_data_for_issues_per_project(
    workspace_id: int,
    namespace: str,
    team: str,
    date_from: datetime = typer.Option(None, "--date-from"),
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):

    """This function returns a raw json object as extracted from VSTS api.
    The parameter namespace is described below.

    Sample of cli command: $ g2 vsts raw-data-issue 1 Org_1/Proj_1 Team1

    Args:
        namespace (str): Organization/Project
    """

    its_project_mock = ITSProjectInDB(
        name=team,
        namespace=namespace,
        id=10,
        integration_type="",
        integration_name="",
        integration_id="",
    )

    g = get_context()
    vsts_credential: Optional[CredentialInDB] = _get_vsts_credential(g, workspace_id)
    vsts_integration = g.integrations.get("vsts")

    if vsts_credential and vsts_integration:
        vsts_integration = cast(VSTSIntegration, vsts_integration)
        token = vsts_credential.to_token_dict(g.fernet)
        # pylint: disable=protected-access
        issues_per_project = vsts_integration._raw_fetching_all_issues_per_project(
            token=token, its_project=its_project_mock, date_from=date_from
        )
        print_results(issues_per_project, format_=format_, fields=fields)


@app.command("issue-updates")
def get_its_issue_updates_(
    workspace_id: int,
    namespace: str,
    issue_id_or_key: str,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):

    its_project_mock = ITSProjectInDB(
        name="test",
        namespace=namespace,
        id=10,
        integration_type="",
        integration_name="",
        integration_id="",
    )

    g = get_context()

    dev_map_callback = partial(developer_map_callback, g=g, workspace_id=workspace_id)

    vsts_credential: Optional[CredentialInDB] = _get_vsts_credential(g, workspace_id)
    vsts_integration = g.integrations.get("vsts")

    if vsts_credential and vsts_integration:
        vsts_integration = cast(VSTSIntegration, vsts_integration)
        token = vsts_credential.to_token_dict(g.fernet)
        issue_updates = vsts_integration.get_its_issue_updates(
            token=token,
            its_project=its_project_mock,
            issue_id_or_key=issue_id_or_key,
            developer_map_callback=dev_map_callback,
        )
        print_results([issue_updates], format_=format_, fields=fields)


@app.command("single-issue")
def list_all_data_single_issue(
    workspace_id: int,
    namespace: str,
    team: str,
    issue_id_or_key: str = typer.Option(None, "--issue-id"),
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):

    its_project_mock = ITSProjectInDB(
        name=team,
        namespace=namespace,
        id=10,
        integration_type="",
        integration_name="",
        integration_id="",
    )

    g = get_context()
    vsts_credential: Optional[CredentialInDB] = _get_vsts_credential(g, workspace_id)
    vsts_integration = g.integrations.get("vsts")

    if vsts_credential and vsts_integration:
        vsts_integration = cast(VSTSIntegration, vsts_integration)
        token = vsts_credential.to_token_dict(g.fernet)
        # pylint: disable=protected-access
        single_work_item = vsts_integration._get_single_work_item_all_data(
            token=token, its_project=its_project_mock, issue_id_or_key=issue_id_or_key
        )
        print_results([single_work_item], format_=format_, fields=fields)


@app.command("linked-issues")
def list_all_linked_issues(
    workspace_id: int,
    namespace: str,
    team: str,
    issue_id_or_key: str = typer.Option(None, "--issue-id"),
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):

    its_project_mock = ITSProjectInDB(
        name=team,
        namespace=namespace,
        id=10,
        integration_type="",
        integration_name="",
        integration_id="",
    )

    g = get_context()
    vsts_credential: Optional[CredentialInDB] = _get_vsts_credential(g, workspace_id)
    vsts_integration = g.integrations.get("vsts")

    if vsts_credential and vsts_integration:
        vsts_integration = cast(VSTSIntegration, vsts_integration)
        token = vsts_credential.to_token_dict(g.fernet)
        # pylint: disable=protected-access
        linked_issues = vsts_integration._get_linked_issues(
            token=token, its_project=its_project_mock, issue_id_or_key=issue_id_or_key
        )
        print_results(linked_issues, format_=format_, fields=fields)


@app.command("single-repo-raw-data")
def single_repo(  # pylint: disable=missing-raises-doc
    workspace_id: int,
    namespace: Optional[str] = None,
    name: Optional[str] = None,
    repo_id: Optional[int] = None,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    """This function returns the raw data for a single repository hosted on the Azure DevOps platform.
    Please ensure that the user associated with the provided workspace has access to the desired repository.

    If you wish to fetch raw data for a specific repository existing in the project instead of a random repository,
    you can pass the parameter 'repo_id' when calling the CLI function.

    Raises:
        SystemExit: if neither a valid combinatio of namespace + name nor a repo_id belonging to a vsts repository is passed,
        then the function is terminated.

    Args:
        workspace_id (int):
        namespace (str): organization/project
        name (str): repo_name

    Example:
        workspace_id (int): 1
        namespace (str): Neo-3/repo_1
        name (str): frontend_repo
    """

    g = get_context()

    if name and namespace:
        repository = RepositoryInDB(
            id=1,
            clone_url="foo",
            protocol="https",
            name=name,
            namespace=namespace,
            private=False,
        )
    elif repo_id:
        repository = g.backend.repositories.get_or_error(workspace_id, repo_id)
        if repository.integration_type != "vsts":
            logger.exception("Given repository is not a VSTS repository", workspace_id=workspace_id, repo_id=repo_id)
            raise typer.Exit(1)
    else:
        logger.exception(
            "Not enough parameters given for the function execution",
            workspace_id=workspace_id,
            namespace=namespace,
            name=name,
            repo_id=repo_id,
        )
        raise typer.Exit(1)

    vsts_credential: Optional[CredentialInDB] = _get_vsts_credential(g, workspace_id)
    vsts_integration = g.integrations.get("vsts")

    if vsts_credential and vsts_credential.owner_id:
        for single_user in g.backend.user_infos.get_for_user(vsts_credential.owner_id):
            if single_user.integration_type == IntegrationType.vsts:
                break

    if vsts_credential and vsts_integration:
        vsts_integration = cast(VSTSIntegration, vsts_integration)
        token = vsts_credential.to_token_dict(g.fernet)
        raw_single_repo_data = vsts_integration.get_raw_single_repo_data(
            repository=repository,
            token=token,
            update_token=get_update_token_callback(g, vsts_credential),
        )

        print_results([raw_single_repo_data], format_=format_, fields=fields)
