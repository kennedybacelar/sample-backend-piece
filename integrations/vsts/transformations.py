from typing import Callable, List, Tuple

from pydantic.datetime_parse import parse_datetime
from gitential2.datatypes.its_projects import ITSProjectInDB
from gitential2.datatypes.its import (
    ITSIssueAllData,
    ITSIssue,
    ITSIssueComment,
    ITSIssueChange,
    ITSIssueTimeInStatus,
    ITSIssueChangeType,
    ITSIssueLinkedIssue,
)

from .common import to_author_alias, _parse_its_issue_change_type


def _transform_to_its_ITSIssueComment(
    comment_dict: dict, its_project: ITSProjectInDB, developer_map_callback: Callable
) -> ITSIssueComment:
    return ITSIssueComment(
        id=comment_dict["id"],
        issue_id=comment_dict["workItemId"],
        itsp_id=its_project.id,
        author_api_id=comment_dict["createdBy"].get("id"),
        author_email=comment_dict["createdBy"].get("uniqueName"),
        author_name=comment_dict["createdBy"].get("displayName"),
        author_dev_id=developer_map_callback(to_author_alias(comment_dict["createdBy"])),
        comment=comment_dict.get("text"),
        created_at=parse_datetime(comment_dict["createdDate"]),
        updated_at=parse_datetime(comment_dict["modifiedDate"]) if comment_dict.get("modifiedDate") else None,
    )


def _initial_status_transform_to_ITSIssueChange(
    initial_change_status: dict,
    its_project: ITSProjectInDB,
) -> ITSIssueChange:

    its_change_id = f"{initial_change_status['issue_id_or_key']}-{initial_change_status['update_api_id']}-System.State"

    return ITSIssueChange(
        id=its_change_id,
        issue_id=initial_change_status["issue_id_or_key"],
        itsp_id=its_project.id,
        api_id=initial_change_status["update_api_id"],
        v_to=initial_change_status["initial_issue_state"],
        v_to_string=initial_change_status["initial_issue_state"],
        change_type=ITSIssueChangeType.status,
        created_at=parse_datetime(initial_change_status["created_date"]),
        updated_at=parse_datetime(initial_change_status["updated_at"]),
        extra={"initial_work_item_type": initial_change_status["initial_work_item_type"]},
    )


def _transform_to_ITSIssueChange(
    its_project: ITSProjectInDB,
    single_update: dict,
    single_field: Tuple[str, dict],
    developer_map_callback: Callable,
) -> ITSIssueChange:

    field_name, field_content = single_field

    v_from_string = (
        field_content["oldValue"].get("displayName")
        if isinstance(field_content.get("oldValue"), dict)
        else field_content.get("oldValue")
    )

    v_to_string = (
        field_content["newValue"].get("displayName")
        if isinstance(field_content.get("newValue"), dict)
        else field_content.get("newValue")
    )

    author_dev_id = developer_map_callback(to_author_alias(single_update.get("revisedBy")))
    its_change_id = f"{str(single_update['workItemId'])[:128]}-{single_update['id']}-{field_name}"

    _updatet_at = single_update["fields"]["System.ChangedDate"]["newValue"]

    return ITSIssueChange(
        id=its_change_id,
        issue_id=single_update["workItemId"],
        itsp_id=its_project.id,
        api_id=single_update["id"],
        author_api_id=single_update["revisedBy"].get("id"),
        author_email=single_update["revisedBy"].get("uniqueName"),
        author_name=single_update["revisedBy"].get("displayName"),
        author_dev_id=author_dev_id,
        field_name=field_name,
        field_id=None,
        field_type=None,
        change_type=_parse_its_issue_change_type(field_name),
        v_from=str(field_content.get("oldValue")),
        v_from_string=v_from_string,
        v_to=str(field_content.get("newValue")),
        v_to_string=v_to_string,
        created_at=parse_datetime(single_update["fields"]["System.ChangedDate"].get("oldValue") or _updatet_at),
        updated_at=parse_datetime(_updatet_at),
    )


# def _transform_to_its_ITSIssueLinkedIssue(
#     its_project: ITSProjectInDB,
#     db_issue_id: str,
#     issue_api_id: str,
#     single_linked_issue: dict,
# ) -> ITSIssueLinkedIssue:

#     _linked_issue_id = single_linked_issue.get("url", "").split("/")[-1]

#     # return ITSIssueLinkedIssue(
#     #     id=f"{its_project.id}-{issue_id_or_key}-{_linked_issue_id}",
#     #     issue_id=int(issue_id_or_key),
#     #     itsp_id=its_project.id,
#     #     linked_issue_id=_linked_issue_id,
#     #     link_type=single_linked_issue.get("attributes", {}).get("name"),
#     # )

#     return ITSIssueLinkedIssue(
#         id=f"{db_issue_id}-{_linked_issue_id}",
#         itsp_id=its_project.id,
#         issue_id=db_issue_id,
#         issue_api_id=issue_dict["id"],
#         issue_key=issue_dict["key"],
#         linked_issue_api_id=_linked_issue_id,
#         linked_issue_key=_linked_issue_key,
#         link_type=_link_type,
#         extra=single_linked_issue,
#     )


def _transform_to_its_ITSIssueAllData(
    issue: ITSIssue,
    comments: List[ITSIssueComment],
    changes: List[ITSIssueChange],
    times_in_statuses: List[ITSIssueTimeInStatus],
    linked_issues: List[ITSIssueLinkedIssue],
) -> ITSIssueAllData:

    return ITSIssueAllData(
        issue=issue,
        comments=comments,
        changes=changes,
        times_in_statuses=times_in_statuses,
        linked_issues=linked_issues,
        sprints=[],
        issue_sprints=[],
        worklogs=[],
    )
