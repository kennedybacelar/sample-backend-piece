from abc import ABC, abstractmethod
from datetime import datetime
from typing import Tuple, Set, Optional

import pandas as pd
from ibis.expr.types import TableExpr

from gitential2.datatypes.stats import IbisTables
from gitential2.extraction.output import OutputHandler
from gitential2.settings import GitentialSettings
from .repositories import (
    AccessApprovalRepository,
    AccessLogRepository,
    AuthorRepository,
    CalculatedPatchRepository,
    PersonalAccessTokenRepository,
    ResellerCodeRepository,
    TeamMemberRepository,
    TeamRepository,
    ExtractedCommitRepository,
    ExtractedPatchRepository,
    ExtractedPatchRewriteRepository,
    CalculatedCommitRepository,
    UserRepository,
    UserInfoRepository,
    SubscriptionRepository,
    CredentialRepository,
    WorkspaceInvitationRepository,
    WorkspaceRepository,
    WorkspaceMemberRepository,
    ProjectRepository,
    RepositoryRepository,
    ITSProjectRepository,
    ProjectRepositoryRepository,
    ProjectITSProjectRepository,
    PullRequestRepository,
    PullRequestCommitRepository,
    PullRequestCommentRepository,
    PullRequestLabelRepository,
    EmailLogRepository,
    ExtractedCommitBranchRepository,
    WorkspaceAPIKeyRepository,
    DeployRepository,
    DashboardRepository,
    ChartRepository,
    ThumbnailRepository,
    DeployCommitRepository,
    AutoExportRepository,
    UserRepositoriesCacheRepository,
    UserITSProjectsCacheRepository,
)
from .repositories_its import (
    ITSIssueRepository,
    ITSIssueChangeRepository,
    ITSIssueCommentRepository,
    ITSIssueSprintRepository,
    ITSIssueTimeInStatusRepository,
    ITSIssueLinkedIssueRepository,
    ITSIssueWorklogRepository,
    ITSSprintRepository,
)
from ...datatypes.workspaces import WorkspaceDuplicate


class GitentialBackend(ABC):
    def __init__(self, settings: GitentialSettings):
        self.settings = settings

    @property
    @abstractmethod
    def access_logs(self) -> AccessLogRepository:
        pass

    @property
    @abstractmethod
    def users(self) -> UserRepository:
        pass

    @property
    @abstractmethod
    def reseller_codes(self) -> ResellerCodeRepository:
        pass

    @property
    @abstractmethod
    def access_approvals(self) -> AccessApprovalRepository:
        pass

    @property
    @abstractmethod
    def pats(self) -> PersonalAccessTokenRepository:
        pass

    @property
    @abstractmethod
    def workspace_api_keys(self) -> WorkspaceAPIKeyRepository:
        pass

    @property
    @abstractmethod
    def subscriptions(self) -> SubscriptionRepository:
        pass

    @property
    @abstractmethod
    def user_infos(self) -> UserInfoRepository:
        pass

    @property
    @abstractmethod
    def credentials(self) -> CredentialRepository:
        pass

    @property
    @abstractmethod
    def workspaces(self) -> WorkspaceRepository:
        pass

    @property
    @abstractmethod
    def workspace_invitations(self) -> WorkspaceInvitationRepository:
        pass

    @property
    @abstractmethod
    def workspace_members(self) -> WorkspaceMemberRepository:
        pass

    @property
    @abstractmethod
    def auto_export(self) -> AutoExportRepository:
        pass

    @property
    @abstractmethod
    def user_repositories_cache(self) -> UserRepositoriesCacheRepository:
        pass

    @property
    @abstractmethod
    def user_its_projects_cache(self) -> UserITSProjectsCacheRepository:
        pass

    @property
    @abstractmethod
    def projects(self) -> ProjectRepository:
        pass

    @property
    @abstractmethod
    def repositories(self) -> RepositoryRepository:
        pass

    @property
    @abstractmethod
    def its_projects(self) -> ITSProjectRepository:
        pass

    @property
    @abstractmethod
    def project_repositories(self) -> ProjectRepositoryRepository:
        pass

    @property
    @abstractmethod
    def project_its_projects(self) -> ProjectITSProjectRepository:
        pass

    @property
    @abstractmethod
    def dashboards(self) -> DashboardRepository:
        pass

    @property
    @abstractmethod
    def charts(self) -> ChartRepository:
        pass

    @property
    @abstractmethod
    def thumbnails(self) -> ThumbnailRepository:
        pass

    @property
    @abstractmethod
    def authors(self) -> AuthorRepository:
        pass

    @property
    @abstractmethod
    def teams(self) -> TeamRepository:
        pass

    @property
    @abstractmethod
    def team_members(self) -> TeamMemberRepository:
        pass

    @property
    @abstractmethod
    def extracted_commits(self) -> ExtractedCommitRepository:
        pass

    @property
    @abstractmethod
    def extracted_patches(self) -> ExtractedPatchRepository:
        pass

    @property
    @abstractmethod
    def extracted_commit_branches(self) -> ExtractedCommitBranchRepository:
        pass

    @property
    @abstractmethod
    def extracted_patch_rewrites(self) -> ExtractedPatchRewriteRepository:
        pass

    @property
    @abstractmethod
    def calculated_commits(self) -> CalculatedCommitRepository:
        pass

    @property
    @abstractmethod
    def calculated_patches(self) -> CalculatedPatchRepository:
        pass

    @property
    @abstractmethod
    def pull_requests(self) -> PullRequestRepository:
        pass

    @property
    @abstractmethod
    def pull_request_commits(self) -> PullRequestCommitRepository:
        pass

    @property
    @abstractmethod
    def pull_request_comments(self) -> PullRequestCommentRepository:
        pass

    @property
    @abstractmethod
    def pull_request_labels(self) -> PullRequestLabelRepository:
        pass

    @property
    @abstractmethod
    def its_issues(self) -> ITSIssueRepository:
        pass

    @property
    @abstractmethod
    def its_issue_changes(self) -> ITSIssueChangeRepository:
        pass

    @property
    @abstractmethod
    def its_issue_times_in_statuses(self) -> ITSIssueTimeInStatusRepository:
        pass

    @property
    @abstractmethod
    def its_issue_comments(self) -> ITSIssueCommentRepository:
        pass

    @property
    @abstractmethod
    def its_issue_linked_issues(self) -> ITSIssueLinkedIssueRepository:
        pass

    @property
    @abstractmethod
    def its_sprints(self) -> ITSSprintRepository:
        pass

    @property
    @abstractmethod
    def its_issue_sprints(self) -> ITSIssueSprintRepository:
        pass

    @property
    @abstractmethod
    def its_issue_worklogs(self) -> ITSIssueWorklogRepository:
        pass

    @property
    @abstractmethod
    def email_log(self) -> EmailLogRepository:
        pass

    @property
    @abstractmethod
    def deploys(self) -> DeployRepository:
        pass

    @property
    @abstractmethod
    def deploy_commits(self) -> DeployCommitRepository:
        pass

    @abstractmethod
    def execute_query(self, query):
        pass

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def initialize_workspace(self, workspace_id: int, workspace_duplicate: Optional[WorkspaceDuplicate] = None):
        pass

    @abstractmethod
    def delete_workspace_schema(self, workspace_id: int):
        pass

    @abstractmethod
    def delete_workspace_sql(self, workspace_id: int):
        pass

    @abstractmethod
    def duplicate_workspace(self, workspace_id_from: int, workspace_id_to: int):
        pass

    @abstractmethod
    def migrate(self):
        pass

    @abstractmethod
    def migrate_workspace(self, workspace_id: int):
        pass

    @abstractmethod
    def reset_workspace(self, workspace_id: int):
        pass

    @abstractmethod
    def delete_schema_revision(self, workspace_id: int):
        pass

    @abstractmethod
    def create_missing_materialized_views(self, workspace_id: int):
        pass

    @abstractmethod
    def drop_existing_materialized_views(self, workspace_id: int):
        pass

    @abstractmethod
    def refresh_materialized_views_in_workspace(self, workspace_id: int):
        pass

    @abstractmethod
    def deactivate_user(self, user_id: int):
        pass

    @abstractmethod
    def purge_user_from_database(self, user_id: int):
        pass

    @abstractmethod
    def delete_own_workspaces_for_user(self, user_id: int):
        pass

    @abstractmethod
    def delete_workspace_collaborations_for_user(self, user_id: int):
        pass

    @abstractmethod
    def output_handler(self, workspace_id: int) -> OutputHandler:
        pass

    @abstractmethod
    def get_commit_ids_for_repository(self, workspace_id: int, repository_id: int) -> Set[str]:
        pass

    @abstractmethod
    def get_extracted_dataframes(
        self, workspace_id: int, repository_id: int, from_: datetime, to_: datetime
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        pass

    @abstractmethod
    def save_calculated_dataframes(
        self,
        workspace_id: int,
        repository_id: int,
        calculated_commits_df: pd.DataFrame,
        calculated_patches_df: pd.DataFrame,
        from_: datetime,
        to_: datetime,
    ):
        pass

    @abstractmethod
    def get_ibis_tables(self, workspace_id: int) -> IbisTables:
        pass

    @abstractmethod
    def get_ibis_table(self, workspace_id: int, source_name: str) -> TableExpr:
        pass
