from datetime import datetime
from enum import Enum
from typing import Optional, Tuple, List

from pydantic import BaseModel
from .common import CoreModel, DateTimeModelMixin, ExtraFieldMixin, StringIdModelMixin
from .export import ExportableModel


class ITSIssueChangeType(str, Enum):
    other = "other"

    # Important changes
    sprint = "sprint"
    status = "status"
    assignee = "assignee"


class ITSIssueStatusCategory(str, Enum):
    unknown = "unknown"
    new = "new"
    in_progress = "in_progress"
    done = "done"


def its_issue_status_category_from_str(integration_type: str, value: str) -> ITSIssueStatusCategory:
    jira_values = {
        "indeterminate": ITSIssueStatusCategory.in_progress,
        "done": ITSIssueStatusCategory.done,
        "new": ITSIssueStatusCategory.new,
    }
    if integration_type == "jira":
        return jira_values.get(value.lower(), ITSIssueStatusCategory.unknown)
    else:
        raise ValueError(f"Invalid integration_type for status category: {integration_type}")


class ITSIssueHeader(StringIdModelMixin, DateTimeModelMixin, CoreModel):
    itsp_id: int
    api_url: str
    api_id: str
    key: Optional[str] = None
    status_name: str
    status_id: Optional[str] = None
    status_category: Optional[str] = None  # todo, in progress/indeterminate, done
    summary: str = ""


class ITSIssue(StringIdModelMixin, ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    itsp_id: int
    api_url: str
    api_id: str
    key: Optional[str] = None

    status_name: str
    status_id: Optional[str] = None
    status_category_api: Optional[str] = None  # todo, inprogress, done
    status_category: Optional[ITSIssueStatusCategory] = None  # todo, inprogress, done

    issue_type_name: str
    issue_type_id: Optional[str] = None

    parent_id: Optional[str] = None

    resolution_name: Optional[str] = None
    resolution_id: Optional[str] = None
    resolution_date: Optional[datetime] = None

    priority_name: Optional[str] = None
    priority_id: Optional[str] = None
    priority_order: Optional[int] = None

    summary: str = ""
    description: str = ""

    # creator
    creator_api_id: Optional[str] = None
    creator_email: Optional[str] = None
    creator_name: Optional[str] = None
    creator_dev_id: Optional[int] = None

    # reporter
    reporter_api_id: Optional[str] = None
    reporter_email: Optional[str] = None
    reporter_name: Optional[str] = None
    reporter_dev_id: Optional[int] = None

    # assignee
    assignee_api_id: Optional[str] = None
    assignee_email: Optional[str] = None
    assignee_name: Optional[str] = None
    assignee_dev_id: Optional[int] = None

    labels: Optional[List[str]] = None

    # calculated fields

    is_started: Optional[bool] = None
    started_at: Optional[datetime] = None

    is_closed: Optional[bool] = None
    closed_at: Optional[datetime] = None

    comment_count: int = 0
    last_comment_at: Optional[datetime] = None
    change_count: int = 0
    last_change_at: Optional[datetime] = None
    story_points: Optional[int] = None

    # is_planned: Optional[bool] = None
    # sprint_count: int = 0

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue", "its_issues")

    def export_fields(self) -> List[str]:
        return [
            # general fields
            "id",
            "created_at",
            "updated_at",
            "itsp_id",
            "api_url",
            "api_id",
            "key",
            "parent_id",
            # status & type
            "status_name",
            "status_id",
            "status_category_api",
            "status_category",
            "issue_type_name",
            "issue_type_id",
            # resolution & priority
            "resolution_name",
            "resolution_id",
            "resolution_date",
            "priority_name",
            "priority_id",
            "priority_order",
            # text fields
            "summary",
            "description",
            # creator
            "creator_api_id",
            "creator_email",
            "creator_name",
            "creator_dev_id",
            # reporter
            "reporter_api_id",
            "reporter_email",
            "reporter_name",
            "reporter_dev_id",
            # assignee
            "assignee_api_id",
            "assignee_email",
            "assignee_name",
            "assignee_dev_id",
            # calculated fields + other
            "labels",
            "is_started",
            "started_at",
            "is_closed",
            "closed_at",
            "comment_count",
            "last_comment_at",
            "change_count",
            "last_change_at",
            "story_points",
        ]


class ITSIssueChange(StringIdModelMixin, ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    issue_id: str
    itsp_id: int
    api_id: str

    author_api_id: Optional[str] = None
    author_email: Optional[str] = None
    author_name: Optional[str] = None
    author_dev_id: Optional[int] = None

    field_name: Optional[str] = None
    field_id: Optional[str] = None
    field_type: Optional[str] = None

    change_type: ITSIssueChangeType = ITSIssueChangeType.other

    v_from: Optional[str] = None
    v_from_string: Optional[str] = None
    v_to: Optional[str] = None
    v_to_string: Optional[str] = None

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue_change", "its_issue_changes")

    def export_fields(self) -> List[str]:
        return [
            "id",
            "created_at",
            "updated_at",
            "issue_id",
            "itsp_id",
            "api_id",
            "author_api_id",
            "author_email",
            "author_name",
            "author_dev_id",
            "field_name",
            "field_id",
            "field_type",
            "change_type",
            "v_from",
            "v_from_string",
            "v_to",
            "v_to_string",
        ]


class ITSIssueTimeInStatus(StringIdModelMixin, ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    issue_id: str
    itsp_id: int
    status_name: str
    status_id: Optional[str] = None
    status_category_api: Optional[str] = None  # todo, inprogress, done
    status_category: Optional[ITSIssueStatusCategory] = None  # todo, inprogress, done

    started_issue_change_id: Optional[str] = None
    started_at: datetime
    ended_at: datetime
    ended_issue_change_id: Optional[str] = None
    ended_with_status_name: Optional[str] = None
    ended_with_status_id: Optional[str] = None
    seconds_in_status: int

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue_time_in_status", "its_issue_times_in_status")

    def export_fields(self) -> List[str]:
        return [
            "id",
            "created_at",
            "updated_at",
            "issue_id",
            "itsp_id",
            "status_name",
            "status_id",
            "status_category_api",
            "status_category",
            "started_issue_change_id",
            "started_at",
            "ended_at",
            "ended_issue_change_id",
            "ended_with_status_name",
            "ended_with_status_id",
            "seconds_in_status",
        ]


class ITSIssueComment(StringIdModelMixin, ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    issue_id: str
    itsp_id: int

    author_api_id: Optional[str] = None
    author_email: Optional[str] = None
    author_name: Optional[str] = None
    author_dev_id: Optional[int] = None

    comment: Optional[str] = None

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue_comment", "its_issue_comments")

    def export_fields(self) -> List[str]:
        return [
            "id",
            "created_at",
            "updated_at",
            "issue_id",
            "itsp_id",
            "author_api_id",
            "author_email",
            "author_name",
            "author_dev_id",
            "comment",
        ]


class ITSIssueLinkedIssue(StringIdModelMixin, ExtraFieldMixin, CoreModel, ExportableModel):
    itsp_id: int
    issue_id: str
    issue_key: Optional[str] = None
    issue_api_id: Optional[str] = None
    linked_issue_api_id: str
    linked_issue_key: Optional[str] = None
    link_type: str

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue_linked_issue", "its_issue_linked_issues")

    def export_fields(self) -> List[str]:
        return [
            "itsp_id",
            "issue_api_id",
            "issue_id",
            "extra",
            "linked_issue_key",
            "linked_issue_api_id",
            "issue_key",
            "link_type",
            "id",
        ]


class ITSSprint(StringIdModelMixin, ExtraFieldMixin, CoreModel, ExportableModel):
    itsp_id: int
    api_id: str

    name: str
    state: str

    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    goal: Optional[str] = None

    def export_names(self) -> Tuple[str, str]:
        return ("its_sprint", "its_sprints")

    def export_fields(self) -> List[str]:
        return [
            "itsp_id",
            "extra",
            "api_id",
            "ended_at",
            "started_at",
            "id",
            "completed_at",
            "goal",
            "name",
        ]


class ITSIssueSprint(StringIdModelMixin, CoreModel, ExportableModel):
    issue_id: str
    itsp_id: int
    sprint_id: str

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue_sprint", "its_issue_sprints")

    def export_fields(self) -> List[str]:
        return [
            "itsp_id",
            "sprint_id",
            "issue_id",
            "id",
        ]


class ITSIssueWorklog(StringIdModelMixin, ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    api_id: str
    issue_id: str
    itsp_id: int

    author_api_id: Optional[str] = None
    author_email: Optional[str] = None
    author_name: Optional[str] = None
    author_dev_id: Optional[int] = None

    started_at: datetime
    time_spent_seconds: int
    time_spent_display_str: str

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue_worklog", "its_issue_worklogs")

    def export_fields(self) -> List[str]:
        return [
            "itsp_id",
            "author_api_id",
            "issue_id",
            "extra",
            "author_email",
            "api_id",
            "author_name",
            "author_dev_id",
            "created_at",
            "time_spent_display_str",
            "time_spent_seconds",
            "updated_at",
            "id",
        ]


class ITSIssueAllData(BaseModel):
    issue: ITSIssue
    comments: List[ITSIssueComment]
    changes: List[ITSIssueChange]
    times_in_statuses: List[ITSIssueTimeInStatus]
    linked_issues: List[ITSIssueLinkedIssue]
    sprints: List[ITSSprint]
    issue_sprints: List[ITSIssueSprint]
    worklogs: List[ITSIssueWorklog]
