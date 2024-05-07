from abc import abstractmethod
from datetime import datetime
from typing import Optional, List

from gitential2.datatypes.its import (
    ITSIssue,
    ITSIssueChange,
    ITSIssueHeader,
    ITSIssueSprint,
    ITSIssueTimeInStatus,
    ITSIssueComment,
    ITSIssueLinkedIssue,
    ITSIssueWorklog,
    ITSSprint,
)
from .repositories_base import BaseWorkspaceScopedRepository


class ITSIssueRepository(
    BaseWorkspaceScopedRepository[str, ITSIssue, ITSIssue, ITSIssue],
):
    @abstractmethod
    def get_header(self, workspace_id: int, id_: str) -> Optional[ITSIssueHeader]:
        pass

    @abstractmethod
    def get_list_of_itsp_ids_distinct(self, workspace_id: int) -> List[int]:
        pass

    @abstractmethod
    def select_its_issues(
        self,
        workspace_id: int,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        itsp_ids: Optional[List[int]] = None,
    ) -> List[ITSIssue]:
        pass

    @abstractmethod
    def delete_its_issues(self, workspace_id: int, its_issue_ids: Optional[List[str]] = None) -> int:
        pass


class ITSIssueChangeRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueChange, ITSIssueChange, ITSIssueChange],
):
    @abstractmethod
    def delete_its_issue_changes(self, workspace_id: int, its_ids: List[str]) -> int:
        pass


class ITSIssueTimeInStatusRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueTimeInStatus, ITSIssueTimeInStatus, ITSIssueTimeInStatus],
):
    @abstractmethod
    def delete_its_issue_time_in_statuses(self, workspace_id: int, its_ids: List[str]) -> int:
        pass


class ITSIssueCommentRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueComment, ITSIssueComment, ITSIssueComment],
):
    @abstractmethod
    def delete_its_issue_comments(self, workspace_id: int, its_ids: List[str]) -> int:
        pass


class ITSIssueLinkedIssueRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueLinkedIssue, ITSIssueLinkedIssue, ITSIssueLinkedIssue],
):
    @abstractmethod
    def delete_its_issue_linked_issues(self, workspace_id: int, its_ids: List[str]) -> int:
        pass


class ITSSprintRepository(
    BaseWorkspaceScopedRepository[str, ITSSprint, ITSSprint, ITSSprint],
):
    @abstractmethod
    def delete_its_sprints(self, workspace_id: int, itsp_ids: List[int]) -> int:
        pass


class ITSIssueSprintRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueSprint, ITSIssueSprint, ITSIssueSprint],
):
    @abstractmethod
    def delete_its_issue_sprints(self, workspace_id: int, its_ids: List[str]) -> int:
        pass


class ITSIssueWorklogRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueWorklog, ITSIssueWorklog, ITSIssueWorklog],
):
    @abstractmethod
    def delete_its_issue_worklogs(self, workspace_id: int, its_ids: List[str]) -> int:
        pass
