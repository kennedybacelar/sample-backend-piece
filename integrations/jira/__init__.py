from datetime import datetime, timedelta
from typing import Callable, Tuple, List, Dict, cast, Optional
from pydantic import BaseModel, Field
from structlog import get_logger

from gitential2.datatypes.its_projects import ITSProjectCreate, ITSProjectInDB
from gitential2.datatypes.its import (
    ITSIssue,
    ITSIssueChange,
    ITSIssueChangeType,
    ITSIssueComment,
    ITSIssueHeader,
    ITSIssueAllData,
    ITSIssueSprint,
    ITSIssueWorklog,
    ITSSprint,
    its_issue_status_category_from_str,
    ITSIssueLinkedIssue,
)
from gitential2.datatypes.userinfos import UserInfoCreate

from ..base import BaseIntegration, ITSProviderMixin, OAuthLoginMixin
from .common import (
    get_rest_api_base_url_from_project_api_url,
    get_db_issue_id,
    get_all_pages_from_paginated,
    format_datetime_for_jql,
)
from .transformations import (
    transform_dict_to_issue,
    transform_dict_to_issue_header,
    transform_dicts_to_issue_changes,
    transform_dicts_to_issue_comments,
    transform_changes_to_times_in_statuses,
    transform_to_its_ITSIssueLinkedIssue,
    transform_to_its_Sprint_and_IssueSprint,
    transform_to_its_worklog,
)

logger = get_logger(__name__)


_OAUTH_SCOPES = [
    # Personal data reporting API
    # Report user accounts that an app is storing personal data for.
    "report:personal-data",
    #
    # General
    "read:me, offline_access",
    #
    # Get projects paginated
    # GET /rest/api/3/project/search
    "read:issue-type:jira, read:project:jira, read:project.property:jira, read:user:jira, read:application-role:jira, read:avatar:jira, read:group:jira, read:issue-type-hierarchy:jira, read:project-category:jira, read:project-version:jira, read:project.component:jira",
    #
    # Get changelogs
    # GET /rest/api/3/issue/{issueIdOrKey}/changelog
    "read:issue-meta:jira, read:avatar:jira, read:issue.changelog:jira",
    #
    # Get issue
    # GET /rest/api/3/issue/{issueIdOrKey}
    "read:issue-meta:jira, read:issue-security-level:jira, read:issue.vote:jira, read:issue.changelog:jira, read:avatar:jira, read:issue:jira, read:status:jira, read:user:jira, read:field-configuration:jira",
    #
    # Search for issues using JQL (GET)
    # GET /rest/api/3/search
    "read:issue-details:jira, read:audit-log:jira, read:avatar:jira, read:field-configuration:jira, read:issue-meta:jira",
    #
    # Get changelogs
    # GET /rest/api/3/issue/{issueIdOrKey}/changelog
    "read:issue-meta:jira, read:avatar:jira, read:issue.changelog:jira",
    #
    # Get comments
    # GET /rest/api/3/issue/{issueIdOrKey}/comment
    "read:comment:jira, read:comment.property:jira, read:group:jira, read:project:jira, read:project-role:jira, read:user:jira, read:avatar:jira",
    #
    # Get priorities
    # GET /rest/api/3/priority
    "read:priority:jira",
    #
    # Get fields
    # GET /rest/api/3/field
    "read:field:jira, read:avatar:jira, read:project-category:jira, read:project:jira, read:field-configuration:jira",
    #
    # Get all statuses
    # GET /rest/api/3/status
    "read:status:jira",
    #
    # Get user
    # GET /rest/api/3/user
    "read:application-role:jira, read:group:jira, read:user:jira, read:avatar:jira",
    #
    # Get issue link types
    # GET /rest/api/3/issueLinkType
    "read:issue-link-type:jira",
    #
    # Jira software read scopes
    # "read:board-scope:jira-software",
    # "read:epic:jira-software",
    # "read:issue:jira-software",
    # "read:sprint:jira-software",
    # "read:source-code:jira-software",
    # "read:feature-flag:jira-software",
    # "read:deployment:jira-software",
    # "read:build:jira-software",
    # "read:remote-link:jira-software",
    #
    # Get issues for sprint
    # GET /rest/agile/1.0/sprint/{sprintId}/issue
    "read:sprint:jira-software, read:issue-details:jira, read:jql:jira",
    #
    # Get all boards
    # GET /rest/agile/1.0/board
    # ?
    # Get all sprints
    # GET /rest/agile/1.0/board/{boardId}/sprint
    "read:sprint:jira-software",
]

OAUTH_SCOPES = " ".join(
    sorted(set(s.strip() for sd in _OAUTH_SCOPES for s in sd.split(",")), key=lambda x: (x.split(":")[-1], x))
)


class AtlassianSite(BaseModel):
    id: str
    name: str
    url: str
    scopes: List[str]
    avatar_url: str = Field(..., alias="avatarUrl")


class JiraIntegration(ITSProviderMixin, OAuthLoginMixin, BaseIntegration):
    def oauth_register(self) -> dict:
        logger.debug("Jira Integration Scopes", integration_name=self.name, scopes=OAUTH_SCOPES)
        ret = {
            "access_token_url": "https://auth.atlassian.com/oauth/token",
            "authorize_url": "https://auth.atlassian.com/authorize?audience=api.atlassian.com",
            "userinfo_endpoint": "https://api.atlassian.com/me",
            "client_kwargs": {"scope": OAUTH_SCOPES},
            "client_id": self.settings.oauth.client_id if self.settings.oauth else None,
            "client_secret": self.settings.oauth.client_secret if self.settings.oauth else None,
        }
        return ret

    def refresh_token_if_expired(self, token, update_token: Callable) -> Tuple[bool, dict]:
        return False, token

    def list_accessible_resources(self, token) -> List[AtlassianSite]:
        client = self.get_oauth2_client(token=token)
        resp = client.get("https://api.atlassian.com/oauth/token/accessible-resources")
        client.close()
        return [AtlassianSite.parse_obj(item) for item in resp.json()]

    def list_available_jira_projects(self, token) -> List[Tuple[AtlassianSite, dict]]:
        sites = self.list_accessible_resources(token)
        client = self.get_oauth2_client(token=token)
        ret = []
        for site in sites:
            if "read:project:jira" in site.scopes:
                site_id = site.id
                resp = client.get(f"https://api.atlassian.com/ex/jira/{site_id}/rest/api/2/project")
                resp_json = resp.json()
                for item in resp_json:
                    ret.append((site, item))
            else:
                logger.warning("No Jira scope given for site", site=site)
        return ret

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:

        return UserInfoCreate(
            integration_name=self.name,
            integration_type="jira",
            sub=str(data["account_id"]),
            name=data["name"],
            email=data.get("email"),
            preferred_username=data["nickname"],
            picture=data.get("picture"),
            extra=data,
        )

    # pylint: disable=unused-argument
    def list_available_its_projects(
        self, token: dict, update_token, provider_user_id: Optional[str]
    ) -> List[ITSProjectCreate]:
        jira_projects = self.list_available_jira_projects(token)
        ret = []
        for site, project_dict in jira_projects:
            ret.append(self._transform_to_its_project(site, project_dict))
        return ret

    def _transform_to_its_project(self, site: AtlassianSite, project_dict: dict) -> ITSProjectCreate:
        # print(project_dict)
        return ITSProjectCreate(
            name=project_dict["name"],
            namespace=site.name,
            private=project_dict["isPrivate"],
            api_url=project_dict["self"],
            key=project_dict["key"],
            integration_type="jira",
            integration_name=self.name,
            integration_id=project_dict["id"],
            extra=project_dict,
        )

    # def list_boards(self, token, project_api_url) -> list:
    #     # We're waiting for Atlassian to enable OAuth2 for these endpoints
    #     # https://jira.atlassian.com/browse/JSWCLOUD-18874

    #     #  from pprint import pprint

    #     base_url = _get_rest_api_base_url_from_project_api_url(project_api_url)
    #     # pprint(base_url)
    #     client = self.get_oauth2_client(token=token)
    #     res = client.get(base_url + "/rest/agile/latest/board").json()
    #     # pprint(res)
    #     return []

    # def list_recently_updated_issues(self, token, project_api_url, date_from: datetime) -> list:
    #     base_url = _get_rest_api_base_url_from_project_api_url(project_api_url)
    #     client = self.get_oauth2_client(token=token)
    #     query = "project = GTD AND updated >= -60d ORDER BY updated DESC"
    #     # query = "project = GTD ORDER BY updated DESC"
    #     fields = ["created", "status", "updated", "summary"]
    #     res = get_all_pages_from_paginated(
    #         client, base_url + f"/rest/api/3/search?jql={query}&fields={','.join(fields)}", values_key="issues"
    #     )
    #     from pprint import pprint

    #     pprint(res)

    def _get_single_issue_raw_data(self, token, its_project: ITSProjectInDB, issue_id_or_key: str) -> dict:
        client = self.get_oauth2_client(token=token)
        base_url = get_rest_api_base_url_from_project_api_url(its_project.api_url)

        issue_api_url = base_url + f"/rest/api/3/issue/{issue_id_or_key}?fields=*all&expand=renderedFields"
        resp = client.get(issue_api_url)
        resp.raise_for_status()
        issue_dict = resp.json()

        return issue_dict

    def _get_linked_issues_for_issue(
        self,
        its_project: ITSProjectInDB,
        db_issue_id: str,
        issue_dict: dict,
    ) -> List[ITSIssueLinkedIssue]:

        list_of_linked_issues = issue_dict.get("fields", {}).get("issuelinks")

        if not list_of_linked_issues:
            return []

        ret = []

        for single_linked_issue in list_of_linked_issues:
            ret.append(
                transform_to_its_ITSIssueLinkedIssue(
                    its_project=its_project,
                    db_issue_id=db_issue_id,
                    single_linked_issue=single_linked_issue,
                    issue_dict=issue_dict,
                )
            )
        return ret

    def get_all_data_for_issue(
        self, token, its_project: ITSProjectInDB, issue_id_or_key: str, developer_map_callback: Callable
    ) -> ITSIssueAllData:

        priority_orders = self._get_site_priority_orders(token, its_project)

        issue_dict = self._get_single_issue_raw_data(
            token=token, its_project=its_project, issue_id_or_key=issue_id_or_key
        )

        db_issue_id = get_db_issue_id(its_project, issue_dict)

        all_statuses = self._get_site_statuses(token, its_project)
        changes = self._get_issue_changes_for_issue(
            token, its_project, db_issue_id, issue_id_or_key, developer_map_callback
        )
        comments = self._get_issue_comments_for_issue(
            token, its_project, db_issue_id, issue_id_or_key, developer_map_callback
        )
        times_in_statuses = transform_changes_to_times_in_statuses(
            db_issue_id, its_project.id, issue_dict["fields"]["created"], changes, all_statuses
        )
        linked_issues = self._get_linked_issues_for_issue(
            its_project=its_project, db_issue_id=db_issue_id, issue_dict=issue_dict
        )

        calculated_fields = _calc_additional_fields_for_issue(changes, comments, all_statuses)

        fields = self._get_site_fields(token, its_project)
        calculated_fields["story_points"] = _get_story_points(issue_dict, fields)

        sprints, issue_sprints = _get_sprints(issue_dict, fields, db_issue_id, its_project)

        worklogs = _get_worklogs(issue_dict, db_issue_id, its_project, developer_map_callback)

        issue = transform_dict_to_issue(
            issue_dict,
            its_project,
            developer_map_callback=developer_map_callback,
            priority_orders=priority_orders,
            calculated_fields=calculated_fields,
        )
        return ITSIssueAllData(
            issue=issue,
            comments=comments,
            changes=changes,
            times_in_statuses=times_in_statuses,
            linked_issues=linked_issues,
            sprints=sprints,
            issue_sprints=issue_sprints,
            worklogs=worklogs,
        )

    def _get_issue_changes_for_issue(
        self,
        token,
        its_project: ITSProjectInDB,
        db_issue_id: str,
        issue_id_or_key: str,
        developer_map_callback: Callable,
    ) -> List[ITSIssueChange]:
        try:
            client = self.get_oauth2_client(token=token)
            base_url = get_rest_api_base_url_from_project_api_url(its_project.api_url)
            fields = self._get_site_fields(token, its_project)
            changes = get_all_pages_from_paginated(
                client,
                base_url + f"/rest/api/3/issue/{issue_id_or_key}/changelog",
                values_key="values",
            )

            return transform_dicts_to_issue_changes(changes, fields, its_project, db_issue_id, developer_map_callback)
        finally:
            client.close()

    def _get_issue_comments_for_issue(
        self,
        token,
        its_project: ITSProjectInDB,
        db_issue_id: str,
        issue_id_or_key: str,
        developer_map_callback: Callable,
    ) -> List[ITSIssueComment]:
        try:
            client = self.get_oauth2_client(token=token)
            base_url = get_rest_api_base_url_from_project_api_url(its_project.api_url)
            comments = get_all_pages_from_paginated(
                client,
                base_url + f"/rest/api/3/issue/{issue_id_or_key}/comment?expand=renderedBody&orderBy=-created",
                values_key="comments",
            )
            return transform_dicts_to_issue_comments(comments, its_project, db_issue_id, developer_map_callback)
        finally:
            client.close()

    def _get_site_priority_orders(self, token, its_project: ITSProjectInDB) -> Dict[str, int]:
        base_url = get_rest_api_base_url_from_project_api_url(its_project.api_url)
        priorities = self.http_get_json_and_cache(url=base_url + "/rest/api/2/priority", token=token)
        return {prio["name"]: idx for idx, prio in enumerate(priorities, start=1)}

    def _get_site_fields(self, token, its_project: ITSProjectInDB) -> dict:
        base_url = get_rest_api_base_url_from_project_api_url(its_project.api_url)
        fields = self.http_get_json_and_cache(url=base_url + "/rest/api/2/field", token=token)
        return {field["id"]: field for field in fields}

    def _get_site_statuses(self, token, its_project: ITSProjectInDB) -> dict:
        base_url = get_rest_api_base_url_from_project_api_url(its_project.api_url)
        statuses = self.http_get_json_and_cache(url=base_url + "/rest/api/3/status", token=token)
        return {status["id"]: status for status in statuses}

    def list_all_issues_for_project(
        self,
        token,
        its_project: ITSProjectInDB,
        date_from: Optional[datetime] = None,
    ) -> List[ITSIssueHeader]:
        jql = f'updated >= "{format_datetime_for_jql(date_from)}"' if date_from else None
        logger.debug("list_all_issues_for_project", jql=jql)
        issue_header_dicts = self._list_project_issues(
            token,
            its_project,
            jql=jql,
            order_by="created DESC",
        )
        return [
            transform_dict_to_issue_header(issue_header_dict, its_project) for issue_header_dict in issue_header_dicts
        ]

    def list_recently_updated_issues(
        self,
        token,
        its_project: ITSProjectInDB,
        date_from: Optional[datetime] = None,
    ) -> List[ITSIssueHeader]:
        date_from = date_from or datetime.utcnow() - timedelta(days=7)
        jql = f'updated >= "{format_datetime_for_jql(date_from)}"'
        logger.debug("list_recently_updated_issues", jql=jql)
        issue_header_dicts = self._list_project_issues(token, its_project, jql, order_by="updated DESC")
        return [
            transform_dict_to_issue_header(issue_header_dict, its_project) for issue_header_dict in issue_header_dicts
        ]

    def _list_project_issues(
        self,
        token,
        its_project: ITSProjectInDB,
        jql: Optional[str] = None,
        order_by: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ) -> List[dict]:
        query = f'project = "{its_project.key}"'
        if jql:
            query = f"{query} AND {jql}"
        if order_by:
            query = f"{query} ORDER BY {order_by}"
        fields = fields or ["created", "status", "updated", "summary"]
        client = self.get_oauth2_client(token=token)
        base_url = get_rest_api_base_url_from_project_api_url(its_project.api_url)
        results = get_all_pages_from_paginated(
            client,
            base_url + f"/rest/api/3/search?jql={query}&fields={','.join(fields)}",
            values_key="issues",
        )
        client.close()
        return results


def _calc_additional_fields_for_issue(
    changes: List[ITSIssueChange], comments: List[ITSIssueComment], all_statuses: dict
) -> dict:
    ret: dict = {}
    # commments
    ret["comment_count"] = len(comments)
    ret["last_comment_at"] = (
        sorted(comments, key=lambda c: cast(datetime, c.created_at), reverse=True)[0].created_at if comments else None
    )
    # changes
    ret["change_count"] = len(changes)
    ret["last_change_at"] = (
        sorted(changes, key=lambda c: cast(datetime, c.created_at), reverse=True)[0].created_at if changes else None
    )
    status_changes = sorted(
        [c for c in changes if c.change_type == ITSIssueChangeType.status], key=lambda x: cast(datetime, x.created_at)
    )

    is_started = False
    started_at = None
    is_closed = False
    closed_at = None
    for c in status_changes:
        status_id = c.v_to
        status_category_api = all_statuses.get(status_id, {}).get("statusCategory", {}).get("key", "indeterminate")
        status_category = its_issue_status_category_from_str("jira", status_category_api)
        if status_category == status_category.in_progress and not is_started:
            is_started = True
            started_at = c.created_at
        if status_category == status_category.done and not is_closed:
            is_closed = True
            closed_at = c.created_at
        if status_category != status_category.done and is_closed:
            # clean closed_at, since the issue is reopened
            is_closed = False
            closed_at = None

    ret["is_started"] = is_started
    ret["started_at"] = started_at
    ret["is_closed"] = is_closed
    ret["closed_at"] = closed_at

    return ret


def _get_story_points(issue_dict: dict, all_fields: dict) -> Optional[int]:
    sp = None
    story_point_field_keys = sorted(
        [
            key
            for key, value in all_fields.items()
            if "com.pyxis.greenhopper.jira:jsw-story-points" == value.get("schema", {}).get("custom", "")
            or "story point" in value["name"].lower()
        ]
    )
    for field_key in story_point_field_keys:
        if field_key in issue_dict["fields"]:
            sp = issue_dict["fields"].get(field_key)
            if sp:
                return int(sp)
    return None


def _get_sprints(
    issue_dict: dict, all_fields: dict, db_issue_id: str, its_project: ITSProjectInDB
) -> Tuple[List[ITSSprint], List[ITSIssueSprint]]:
    sprint_field_name = _find_sprint_field_name(all_fields)
    sprint_field_value: list = issue_dict["fields"].get(sprint_field_name, []) if sprint_field_name else []

    sprints, issue_sprints = [], []
    if sprint_field_value:
        for sprint_dict in sprint_field_value:
            sprint, issue_sprint = transform_to_its_Sprint_and_IssueSprint(its_project, db_issue_id, sprint_dict)
            sprints.append(sprint)
            issue_sprints.append(issue_sprint)
    return sprints, issue_sprints


def _find_sprint_field_name(all_fields: dict) -> Optional[str]:
    for k, v in all_fields.items():
        if v.get("custom", False) and v.get("schema", {}).get("custom", "") == "com.pyxis.greenhopper.jira:gh-sprint":
            return k
    return None


def _get_worklogs(
    issue_dict: dict, db_issue_id: str, its_project: ITSProjectInDB, developer_map_callback: Callable
) -> List[ITSIssueWorklog]:
    worklogs: list = []
    for worklog_dict in issue_dict["fields"]["worklog"].get("worklogs", []):
        worklog = transform_to_its_worklog(its_project, db_issue_id, worklog_dict, developer_map_callback)
        worklogs.append(worklog)
    return worklogs
