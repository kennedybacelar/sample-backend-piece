from datetime import datetime
from typing import Callable, Dict, List, Tuple, Optional
from gitential2.datatypes.its_projects import ITSProjectInDB
from gitential2.datatypes.its import (
    ITSIssue,
    ITSIssueChange,
    ITSIssueChangeType,
    ITSIssueComment,
    ITSIssueHeader,
    ITSIssueSprint,
    ITSIssueTimeInStatus,
    ITSIssueWorklog,
    ITSSprint,
    its_issue_status_category_from_str,
    ITSIssueLinkedIssue,
)
from .common import get_db_issue_id, parse_account


def transform_dict_to_issue_header(issue_header_dict: dict, its_project: ITSProjectInDB) -> ITSIssueHeader:
    return ITSIssueHeader(
        id=get_db_issue_id(its_project, issue_header_dict),
        created_at=issue_header_dict["fields"]["created"],
        updated_at=issue_header_dict["fields"]["updated"],
        itsp_id=its_project.id,
        api_url=issue_header_dict["self"],
        api_id=issue_header_dict["id"],
        key=issue_header_dict["key"],
        status_name=issue_header_dict["fields"]["status"]["name"],
        status_id=issue_header_dict["fields"]["status"]["id"],
        status_category=its_issue_status_category_from_str(
            "jira", issue_header_dict["fields"]["status"]["statusCategory"]["key"]
        ),
        summary=issue_header_dict["fields"]["summary"],
    )


def transform_dict_to_issue(
    issue_dict: dict,
    its_project: ITSProjectInDB,
    developer_map_callback: Callable,
    priority_orders: Dict[str, int],
    calculated_fields: dict,
) -> ITSIssue:

    creator_api_id, creator_email, creator_name, creator_dev_id = parse_account(
        issue_dict["fields"].get("creator"), developer_map_callback
    )
    reporter_api_id, reporter_email, reporter_name, reporter_dev_id = parse_account(
        issue_dict["fields"].get("reporter"), developer_map_callback
    )
    assignee_api_id, assignee_email, assignee_name, assignee_dev_id = parse_account(
        issue_dict["fields"].get("assignee"), developer_map_callback
    )

    return ITSIssue(
        id=get_db_issue_id(its_project, issue_dict),
        created_at=issue_dict["fields"]["created"],
        updated_at=issue_dict["fields"]["updated"],
        itsp_id=its_project.id,
        api_url=issue_dict["self"],
        api_id=issue_dict["id"],
        key=issue_dict["key"],
        # Current status
        status_name=issue_dict["fields"]["status"]["name"],
        status_id=issue_dict["fields"]["status"]["id"],
        status_category_api=issue_dict["fields"]["status"]["statusCategory"]["key"],
        status_category=its_issue_status_category_from_str(
            "jira", issue_dict["fields"]["status"]["statusCategory"]["key"]
        ),
        # Issue Type
        issue_type_name=issue_dict["fields"]["issuetype"]["name"],
        issue_type_id=issue_dict["fields"]["issuetype"]["id"],
        # Get parent (if any)
        parent_id=f"{its_project.id}-{issue_dict['fields']['parent']['id']}"
        if issue_dict["fields"].get("parent", {})
        else None,
        # Get resolution
        resolution_name=issue_dict["fields"]["resolution"]["name"] if issue_dict["fields"].get("resolution") else None,
        resolution_id=issue_dict["fields"]["resolution"]["id"] if issue_dict["fields"].get("resolution") else None,
        resolution_date=issue_dict["fields"].get("resolutiondate"),
        # Get issue priority
        priority_name=issue_dict["fields"]["priority"]["name"],
        priority_id=issue_dict["fields"]["priority"]["id"],
        priority_order=priority_orders.get(issue_dict["fields"]["priority"]["name"]),
        # Summary & description
        summary=issue_dict["fields"]["summary"],
        description=issue_dict["renderedFields"]["description"],
        # creator
        creator_api_id=creator_api_id,
        creator_email=creator_email,
        creator_name=creator_name,
        creator_dev_id=creator_dev_id,
        # reporter
        reporter_api_id=reporter_api_id,
        reporter_email=reporter_email,
        reporter_name=reporter_name,
        reporter_dev_id=reporter_dev_id,
        # assignee
        assignee_api_id=assignee_api_id,
        assignee_email=assignee_email,
        assignee_name=assignee_name,
        assignee_dev_id=assignee_dev_id,
        # extra
        labels=issue_dict["fields"].get("labels"),
        extra=issue_dict,
        # calculated
        **calculated_fields,
    )


def _remove_spec_chars(s: str) -> str:
    return "".join(e for e in s if e.isalnum())


def _get_field_id(change: dict) -> str:
    return _remove_spec_chars(change.get("fieldId", change["field"])).lower().replace("customfield", "cf")


def transform_dicts_to_issue_changes(
    changes: List[dict], fields: dict, its_project: ITSProjectInDB, db_issue_id: str, developer_map_callback: Callable
) -> List[ITSIssueChange]:
    ret = []
    for changelog in changes:
        author_api_id, author_email, author_name, author_dev_id = parse_account(
            changelog["author"], developer_map_callback
        )
        common_args_for_changelog = {
            "issue_id": db_issue_id,
            "created_at": changelog["created"],
            "updated_at": changelog["created"],
            "api_id": changelog["id"],
            "itsp_id": its_project.id,
            "author_api_id": author_api_id,
            "author_email": author_email,
            "author_name": author_name,
            "author_dev_id": author_dev_id,
        }

        for change in changelog.get("items", []):
            field_id = _get_field_id(change)
            change_id = f"{its_project.id}-{db_issue_id}-{changelog['id']}-{field_id}"[:32]
            change_obj = _transform_dict_to_issue_change(change, change_id, common_args_for_changelog, fields)
            ret.append(change_obj)
    return ret


def _transform_dict_to_issue_change(
    change_item: dict, change_id: str, common_args: dict, fields: dict
) -> ITSIssueChange:
    field_id = change_item.get("fieldId", change_item["field"])
    field_name, field_schema = get_name_and_schema_for_field(field_id, change_item["field"], fields)[:64]

    return ITSIssueChange(
        id=change_id,
        field_id=field_id,
        field_name=field_name,
        field_type=change_item.get("fieldtype"),
        change_type=calc_change_type(field_schema),
        v_from=change_item["from"],
        v_from_string=change_item["fromString"],
        v_to=change_item["to"],
        v_to_string=change_item["toString"],
        **common_args,
    )


def get_name_and_schema_for_field(field_id: str, field_name, fields: dict) -> Tuple[str, Optional[dict]]:
    if field_id in fields:
        field_def = fields[field_id]
        if field_def["custom"]:
            return field_def["name"], field_def["schema"]
        else:
            return field_def["key"], field_def["schema"]
    else:
        return field_name, None


def calc_change_type(field_schema: Optional[dict]) -> ITSIssueChangeType:
    field_schema = field_schema or {}
    if field_schema.get("type") == "status":
        return ITSIssueChangeType.status
    if field_schema.get("custom") == "com.pyxis.greenhopper.jira:gh-sprint":
        return ITSIssueChangeType.sprint
    if field_schema.get("system") == "assignee" and field_schema.get("type") == "user":
        return ITSIssueChangeType.assignee
    return ITSIssueChangeType.other


def transform_dicts_to_issue_comments(
    comments: List[dict], its_project: ITSProjectInDB, db_issue_id: str, developer_map_callback: Callable
) -> List[ITSIssueComment]:
    ret: List[ITSIssueComment] = []
    common_args = {"issue_id": db_issue_id, "itsp_id": its_project.id}
    for comment_dict in comments:
        comment = _transform_dict_to_issue_comment(comment_dict, db_issue_id, developer_map_callback, common_args)
        ret.append(comment)
    return ret


def _transform_dict_to_issue_comment(
    comment_dict: dict,
    db_issue_id: str,
    developer_map_callback: Callable,
    common_args: dict,
):
    author_api_id, author_email, author_name, author_dev_id = parse_account(
        comment_dict["author"], developer_map_callback
    )
    return ITSIssueComment(
        id=f"{db_issue_id}-{comment_dict['id']}",
        created_at=comment_dict["created"],
        updated_at=comment_dict["updated"],
        author_api_id=author_api_id,
        author_email=author_email,
        author_name=author_name,
        author_dev_id=author_dev_id,
        comment=comment_dict.get("renderedBody"),
        extra=comment_dict,
        **common_args,
    )


def transform_changes_to_times_in_statuses(
    db_issue_id: str, itsp_id: int, issue_created_at: datetime, changes: List[ITSIssueChange], all_statuses: dict
) -> List[ITSIssueTimeInStatus]:
    status_changes = sorted(
        [c for c in changes if c.change_type == ITSIssueChangeType.status],
        key=lambda c: c.created_at or datetime.utcnow(),
    )
    common_args = {
        "issue_id": db_issue_id,
        "itsp_id": itsp_id,
    }
    if not status_changes:
        return []
    else:
        initial_status_change = ITSIssueChange(
            id="",
            created_at=issue_created_at,
            updated_at=issue_created_at,
            issue_id=db_issue_id,
            itsp_id=itsp_id,
            api_id="",
            v_to=status_changes[0].v_from,
            v_to_string=status_changes[0].v_from_string,
        )
        ret = []
        for p, n in zip([initial_status_change] + status_changes, status_changes):
            status_category_api = all_statuses.get(n.v_from, {}).get("statusCategory", {}).get("key", "indeterminate")
            status_category = its_issue_status_category_from_str("jira", status_category_api)
            timeSpent = ITSIssueTimeInStatus(
                created_at=n.created_at,
                updated_at=n.updated_at,
                id=f"{db_issue_id}-{n.api_id}",
                status_name=n.v_from_string,
                status_id=n.v_from,
                status_category_api=status_category_api,
                status_category=status_category,
                started_issue_change_id=p.id,
                started_at=p.created_at,
                ended_issue_change_id=n.id,
                ended_at=n.created_at,
                ended_with_status_name=n.v_to_string,
                ended_with_status_id=n.v_to,
                seconds_in_status=(n.created_at - p.created_at).seconds if (n.created_at and p.created_at) else None,
                **common_args,
            )
            ret.append(timeSpent)
        return ret


def transform_to_its_ITSIssueLinkedIssue(
    its_project: ITSProjectInDB, db_issue_id: str, single_linked_issue: dict, issue_dict: dict
) -> ITSIssueLinkedIssue:

    if single_linked_issue.get("inwardIssue"):
        _linked_issue_id = single_linked_issue.get("inwardIssue", {}).get("id")
        _linked_issue_key = single_linked_issue.get("inwardIssue", {}).get("key")
        _link_type = single_linked_issue.get("type", {}).get("inward")
    elif single_linked_issue.get("outwardIssue"):
        _linked_issue_id = single_linked_issue.get("outwardIssue", {}).get("id")
        _linked_issue_key = single_linked_issue.get("outwardIssue", {}).get("key")
        _link_type = single_linked_issue.get("type", {}).get("outward")

    return ITSIssueLinkedIssue(
        id=f"{db_issue_id}-{_linked_issue_id}",
        itsp_id=its_project.id,
        issue_id=db_issue_id,
        issue_api_id=issue_dict["id"],
        issue_key=issue_dict["key"],
        linked_issue_api_id=_linked_issue_id,
        linked_issue_key=_linked_issue_key,
        link_type=_link_type,
        extra=single_linked_issue,
    )


def transform_to_its_Sprint_and_IssueSprint(
    its_project: ITSProjectInDB,
    db_issue_id: str,
    sprint_dict: dict,
) -> Tuple[ITSSprint, ITSIssueSprint]:
    sprint = ITSSprint(
        id=f"{its_project.id}-{sprint_dict.get('boardId', 0)}-{sprint_dict['id']}",
        itsp_id=its_project.id,
        api_id=sprint_dict["id"],
        name=sprint_dict["name"],
        state=sprint_dict["state"],
        started_at=sprint_dict.get("startDate"),
        ended_at=sprint_dict.get("endDate"),
        completed_at=sprint_dict.get("completeDate"),
        goal=sprint_dict.get("goal"),
        extra=sprint_dict,
    )
    issue_sprint = ITSIssueSprint(
        id=f"{sprint.id}-{db_issue_id}", itsp_id=its_project.id, sprint_id=sprint.id, issue_id=db_issue_id
    )
    return sprint, issue_sprint


def transform_to_its_worklog(
    its_project: ITSProjectInDB,
    db_issue_id: str,
    worklog_dict: dict,
    developer_map_callback: Callable,
) -> ITSIssueWorklog:

    author_api_id, author_email, author_name, author_dev_id = parse_account(
        worklog_dict["author"], developer_map_callback
    )

    return ITSIssueWorklog(
        id=f"{db_issue_id}-{worklog_dict['id']}",
        api_id=str(worklog_dict["id"]),
        issue_id=db_issue_id,
        itsp_id=its_project.id,
        author_api_id=author_api_id,
        author_email=author_email,
        author_name=author_name,
        author_dev_id=author_dev_id,
        # timestaps
        started_at=worklog_dict["started"],
        created_at=worklog_dict["created"],
        updated_at=worklog_dict["updated"],
        # time spent
        time_spent_seconds=worklog_dict["timeSpentSeconds"],
        time_spent_display_str=worklog_dict["timeSpent"],
        extra=worklog_dict,
    )
