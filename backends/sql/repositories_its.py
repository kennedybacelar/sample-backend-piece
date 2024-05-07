from datetime import datetime
from typing import Optional, List

from sqlalchemy import distinct, or_
from sqlalchemy.sql import select

from gitential2.backends.base import (
    ITSIssueRepository,
    ITSIssueChangeRepository,
    ITSIssueTimeInStatusRepository,
    ITSIssueCommentRepository,
    ITSIssueLinkedIssueRepository,
)
from gitential2.backends.base.repositories_its import (
    ITSIssueSprintRepository,
    ITSIssueWorklogRepository,
    ITSSprintRepository,
)
from gitential2.datatypes.its import (
    ITSIssue,
    ITSIssueChange,
    ITSIssueSprint,
    ITSIssueTimeInStatus,
    ITSIssueComment,
    ITSIssueHeader,
    ITSIssueLinkedIssue,
    ITSIssueWorklog,
    ITSSprint,
)
from .repositories import SQLWorkspaceScopedRepository, fetchone_
from ...utils import is_list_not_empty

# pylint: disable=unnecessary-lambda-assignment
fetchone_ = lambda result: result.fetchone()
fetchall_ = lambda result: result.fetchall()
inserted_primary_key_ = lambda result: result.inserted_primary_key[0]
rowcount_ = lambda result: result.rowcount


class SQLITSIssueRepository(
    ITSIssueRepository,
    SQLWorkspaceScopedRepository[str, ITSIssue, ITSIssue, ITSIssue],
):
    def get_header(self, workspace_id: int, id_: str) -> Optional[ITSIssueHeader]:
        query = (
            select(
                [
                    self.table.c.id,
                    self.table.c.itsp_id,
                    self.table.c.api_url,
                    self.table.c.api_id,
                    self.table.c.key,
                    self.table.c.status_name,
                    self.table.c.status_id,
                    self.table.c.status_category,
                    self.table.c.summary,
                    self.table.c.created_at,
                    self.table.c.updated_at,
                ]
            )
            .where(self.identity(id_))
            .limit(1)
        )
        row = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchone_)
        return ITSIssueHeader(**row) if row else None

    def get_list_of_itsp_ids_distinct(self, workspace_id: int) -> List[int]:
        query = select([distinct(self.table.c.itsp_id)]).select_from(self.table)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [r["itsp_id"] for r in rows]

    def select_its_issues(
        self,
        workspace_id: int,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        itsp_ids: Optional[List[int]] = None,
    ) -> List[ITSIssue]:
        or_clause = []
        if date_from:
            or_clause.append(self.table.c.updated_at >= date_from)
        if date_to:
            or_clause.append(self.table.c.updated_at <= date_to)
        if is_list_not_empty(itsp_ids):
            or_clause.append(self.table.c.itsp_id.in_(itsp_ids))
        query = self.table.select().where(or_(*or_clause)) if is_list_not_empty(or_clause) else self.table.select()
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [ITSIssue(**row) for row in rows]

    def delete_its_issues(self, workspace_id: int, its_issue_ids: Optional[List[str]] = None) -> int:
        if is_list_not_empty(its_issue_ids):
            query = self.table.delete().where(self.table.c.id.in_(its_issue_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0


class SQLITSIssueChangeRepository(
    ITSIssueChangeRepository,
    SQLWorkspaceScopedRepository[str, ITSIssueChange, ITSIssueChange, ITSIssueChange],
):
    def delete_its_issue_changes(self, workspace_id: int, its_ids: List[str]) -> int:
        if is_list_not_empty(its_ids):
            query = self.table.delete().where(self.table.c.issue_id.in_(its_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0


class SQLITSIssueTimeInStatusRepository(
    ITSIssueTimeInStatusRepository,
    SQLWorkspaceScopedRepository[str, ITSIssueTimeInStatus, ITSIssueTimeInStatus, ITSIssueTimeInStatus],
):
    def delete_its_issue_time_in_statuses(self, workspace_id: int, its_ids: List[str]) -> int:
        if is_list_not_empty(its_ids):
            query = self.table.delete().where(self.table.c.issue_id.in_(its_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0


class SQLITSIssueCommentRepository(
    ITSIssueCommentRepository,
    SQLWorkspaceScopedRepository[str, ITSIssueComment, ITSIssueComment, ITSIssueComment],
):
    def delete_its_issue_comments(self, workspace_id: int, its_ids: List[str]) -> int:
        if is_list_not_empty(its_ids):
            query = self.table.delete().where(self.table.c.issue_id.in_(its_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0


class SQLITSIssueLinkedIssueRepository(
    ITSIssueLinkedIssueRepository,
    SQLWorkspaceScopedRepository[str, ITSIssueLinkedIssue, ITSIssueLinkedIssue, ITSIssueLinkedIssue],
):
    def delete_its_issue_linked_issues(self, workspace_id: int, its_ids: List[str]) -> int:
        if is_list_not_empty(its_ids):
            query = self.table.delete().where(self.table.c.issue_id.in_(its_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0


class SQLITSSprintRepository(
    ITSSprintRepository,
    SQLWorkspaceScopedRepository[str, ITSSprint, ITSSprint, ITSSprint],
):
    def delete_its_sprints(self, workspace_id: int, itsp_ids: List[int]) -> int:
        if is_list_not_empty(itsp_ids):
            query = self.table.delete().where(self.table.c.itsp_id.in_(itsp_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0


class SQLITSIssueSprintRepository(
    ITSIssueSprintRepository,
    SQLWorkspaceScopedRepository[str, ITSIssueSprint, ITSIssueSprint, ITSIssueSprint],
):
    def delete_its_issue_sprints(self, workspace_id: int, its_ids: List[str]) -> int:
        if is_list_not_empty(its_ids):
            query = self.table.delete().where(self.table.c.issue_id.in_(its_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0


class SQLITSIssueWorklogRepository(
    ITSIssueWorklogRepository,
    SQLWorkspaceScopedRepository[str, ITSIssueWorklog, ITSIssueWorklog, ITSIssueWorklog],
):
    def delete_its_issue_worklogs(self, workspace_id: int, its_ids: List[str]) -> int:
        if is_list_not_empty(its_ids):
            query = self.table.delete().where(self.table.c.issue_id.in_(its_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0
