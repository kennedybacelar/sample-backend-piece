from datetime import datetime
from typing import Optional, cast
from functools import partial
import sys
import logging

from structlog import get_logger
import typer
import requests
from gitential2.datatypes.authors import AuthorAlias, AuthorInDB

from gitential2.datatypes.credentials import CredentialInDB
from gitential2.datatypes.userinfos import UserInfoInDB
from gitential2.core.context import GitentialContext
from gitential2.core.credentials import get_update_token_callback, get_fresh_credential
from gitential2.core.authors import developer_map_callback, get_or_create_optional_author_for_alias
from gitential2.integrations.jira import JiraIntegration
from gitential2.settings import IntegrationType
from .common import get_context, print_results, OutputFormat

app = typer.Typer()
logger = get_logger(__name__)

# log = logging.getLogger("authlib")
# log.addHandler(logging.StreamHandler(sys.stdout))
# log.setLevel(logging.DEBUG)


def _get_jira_credential(g: GitentialContext, workspace_id: int, integration_name="jira") -> Optional[CredentialInDB]:
    return get_fresh_credential(g, workspace_id=workspace_id, integration_name=integration_name)


@app.command("list-accessible-resources")
def list_accessible_resources(workspace_id: int):
    log = logging.getLogger("authlib")
    log.addHandler(logging.StreamHandler(sys.stdout))
    log.setLevel(logging.DEBUG)

    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    if jira_credential and jira_integration:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        # jira_integration.list_accessible_resources(token, get_update_token_callback(g, jira_credential))
        # token = jira_integration.refresh_token(token, get_update_token_callback(g, jira_credential))
        # print(token)
        sites = jira_integration.list_accessible_resources(token)
        print(sites)


@app.command("list-available-projects")
def list_available_projects(
    workspace_id: int,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")

    if jira_credential and jira_credential.owner_id:
        for single_user in g.backend.user_infos.get_for_user(jira_credential.owner_id):
            if single_user.integration_type == IntegrationType.jira:
                userinfo: UserInfoInDB = single_user
                break

    if jira_credential and jira_integration:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        update_token = get_update_token_callback(g, jira_credential)
        its_projects = jira_integration.list_available_its_projects(
            token,
            update_token=update_token,
            provider_user_id=userinfo.sub if userinfo else None,
        )
        print_results(its_projects, format_=format_, fields=fields)


# @app.command("list-boards")
# def list_boards(
#     workspace_id: int,
#     itsp_id: int,
#     # format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
#     # fields: Optional[str] = None,
# ):
#     g = get_context()
#     jira_credential = _get_jira_credential(g, workspace_id)
#     jira_integration = g.integrations.get("jira")
#     its_project = g.backend.its_projects.get(workspace_id, itsp_id)
#     print(its_project)
#     if jira_credential and jira_integration and its_project:
#         jira_integration = cast(JiraIntegration, jira_integration)
#         token = jira_credential.to_token_dict(g.fernet)
#         jira_integration.list_boards(token, its_project.api_url)


@app.command("list-all-issues")
def list_all_issues(
    workspace_id: int,
    itsp_id: int,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    its_project = g.backend.its_projects.get(workspace_id, itsp_id)
    if jira_credential and jira_integration and its_project:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        issue_headers = jira_integration.list_all_issues_for_project(token, its_project)
        print_results(issue_headers, format_=format_, fields=fields)


@app.command("list-recently-updated-issues")
def list_recently_updated_issues(
    workspace_id: int,
    itsp_id: int,
    date_from: datetime,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    its_project = g.backend.its_projects.get(workspace_id, itsp_id)
    if jira_credential and jira_integration and its_project:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        issue_headers = jira_integration.list_recently_updated_issues(token, its_project, date_from=date_from)
        print_results(issue_headers, format_=format_, fields=fields)


@app.command("get-issue")
def get_issue(
    workspace_id: int,
    itsp_id: int,
    issue_id_or_key: str,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    its_project = g.backend.its_projects.get(workspace_id, itsp_id)
    dev_map_callback = partial(developer_map_callback, g=g, workspace_id=workspace_id)

    if jira_credential and jira_integration and its_project:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        issue = jira_integration.get_all_data_for_issue(token, its_project, issue_id_or_key, dev_map_callback)
        print_results([issue], format_=format_, fields=fields)


@app.command("get-single-issue-raw-data")
def get_single_issue_raw_data(
    workspace_id: int,
    itsp_id: int,
    issue_id_or_key: str,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):

    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    its_project = g.backend.its_projects.get(workspace_id, itsp_id)

    if jira_credential and jira_integration and its_project:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        # pylint: disable=protected-access
        issue = jira_integration._get_single_issue_raw_data(
            token=token, its_project=its_project, issue_id_or_key=issue_id_or_key
        )
        print_results([issue], format_=format_, fields=fields)


@app.command("get-linked-issues")
def get_linked_issues(
    workspace_id: int,
    itsp_id: int,
    issue_id_or_key: str,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):

    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    its_project = g.backend.its_projects.get(workspace_id, itsp_id)

    if jira_credential and jira_integration and its_project:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        # pylint: disable=protected-access
        issue_dict = jira_integration._get_single_issue_raw_data(
            token=token, its_project=its_project, issue_id_or_key=issue_id_or_key
        )
        linked_issues = jira_integration._get_linked_issues_for_issue(
            its_project=its_project, issue_id_or_key=issue_id_or_key, issue_dict=issue_dict
        )
        print_results(linked_issues, format_=format_, fields=fields)


@app.command("get-all-data-for-issue")
def get_all_data_for_issue(
    workspace_id: int,
    itsp_id: int,
    issue_id_or_key: str,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):

    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    its_project = g.backend.its_projects.get(workspace_id, itsp_id)
    dev_map_callback = partial(developer_map_callback, g=g, workspace_id=workspace_id)

    if jira_credential and jira_integration and its_project:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        all_data_for_issue = jira_integration.get_all_data_for_issue(
            token=token,
            its_project=its_project,
            issue_id_or_key=issue_id_or_key,
            developer_map_callback=dev_map_callback,
        )
        print_results([all_data_for_issue], format_=format_, fields=fields)


@app.command("lookup-tempo")
def lookup_tempo(
    workspace_id: int,
    tempo_access_token: str = typer.Argument("", envvar="TEMPO_ACCESS_TOKEN"),
    force: bool = False,
    date_from: datetime = datetime.min,
    rewrite_existing_worklogs: bool = typer.Option(False, "--rewrite-existing-worklogs"),
):
    """
    Arguments:
        workspace_id: workspace_id
        tempo_access_token: environment variable - bearer token
        force: determines if authors will be calculated
        date_from (datetime, optional): determines from what date the worklogs will be considered
        rewrite_existing_worklogs: It forces the update of the time logged in the worklogs entries - users may have updated them.
    """
    g = get_context()
    lookup_tempo_worklogs(g, workspace_id, tempo_access_token, force, date_from, rewrite_existing_worklogs)


# pylint: disable=too-complex
def lookup_tempo_worklogs(
    g: GitentialContext,
    workspace_id: int,
    tempo_access_token: str,
    force,
    date_from: datetime,
    rewrite_existing_worklogs: bool,
):
    worklogs_for_issue = {}
    _author_callback_partial = partial(_author_callback, g=g, workspace_id=workspace_id)

    for worklog in g.backend.its_issue_worklogs.iterate_desc(workspace_id):

        # date_from (created_at field) is never None, at table creation it has the default value set as dt.datetime.utcnow
        if worklog.created_at < date_from:  # type: ignore[operator]
            break

        if not worklog.author_dev_id or force:
            author = None
            tempo_worklog = None

            jira_issue_id = worklog.extra.get("issueId") if worklog.extra else None
            if not jira_issue_id:
                continue

            if jira_issue_id not in worklogs_for_issue:
                worklogs_for_issue[jira_issue_id] = (
                    _get_tempo_worklogs_for_issue(tempo_access_token, jira_issue_id) or {}
                )

            results_worklogs_for_issue = worklogs_for_issue[jira_issue_id].get("results", [])

            # We need this if-condition because sometimes all worklogs are removed from an issue and the issue keeps on its_issue_worklogs table.
            # We have to make to make sure of removing those entries properly.

            if results_worklogs_for_issue:
                for wl in worklogs_for_issue[jira_issue_id].get("results", []):
                    if str(wl["jiraWorklogId"]) == worklog.api_id:
                        tempo_worklog = wl
                        break

            if not tempo_worklog:
                g.backend.its_issue_worklogs.delete(workspace_id, worklog.id)
                continue

            if rewrite_existing_worklogs:
                worklog.time_spent_seconds = tempo_worklog["timeSpentSeconds"]
                worklog.time_spent_display_str = _from_seconds_to_day_hour_min(tempo_worklog["timeSpentSeconds"])

            # email information is available through an extra api call at - rest/api/2/user?accountId=accountId
            author = _author_callback_partial(AuthorAlias(name=tempo_worklog["author"]["displayName"]))
            if author:
                print(worklog.created_at, worklog.api_id, jira_issue_id, author.id, author.name)
                worklog.author_dev_id = author.id
                worklog.author_name = author.name
                worklog.author_email = author.email

                g.backend.its_issue_worklogs.update(workspace_id, worklog.id, worklog)
            else:
                print(worklog.created_at, worklog.api_id, jira_issue_id, tempo_worklog)
            print("-------------------------------------------------------")


def _get_tempo_worklogs_for_issue(tempo_access_token: str, jira_issue_id) -> Optional[dict]:
    response = requests.get(
        f"https://api.tempo.io/core/3/worklogs?issue={jira_issue_id}",
        headers={"Authorization": f"Bearer {tempo_access_token}"},
        timeout=300,
    )
    if response.status_code == 200:
        return response.json()
    return None


def _author_callback(
    alias: AuthorAlias,
    g: GitentialContext,
    workspace_id: int,
) -> Optional[AuthorInDB]:
    author = get_or_create_optional_author_for_alias(g, workspace_id, alias)
    if author:
        return author
    else:
        return None


def _from_seconds_to_day_hour_min(delta_time_in_seconds: int) -> str:
    """
    This function convert total delta time in seconds to a str in the following format:
    Example: 1d 2h 30m
    Remind: The day considered has 8 hours = 28800 seconds
    """
    time_dimension_unit_reference = {0: "d", 1: "h", 2: "m"}
    day_hour_minute = (
        delta_time_in_seconds // 28800,
        (delta_time_in_seconds % 28800) // 3600,
        (delta_time_in_seconds // 60) % 60,
    )
    ret = []
    for index, time_dimension in enumerate(day_hour_minute):
        if time_dimension:
            ret.append(f"{time_dimension}{time_dimension_unit_reference[index]}")

    return " ".join(ret)
