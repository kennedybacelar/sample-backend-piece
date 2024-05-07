# pylint: disable=unsubscriptable-object
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable, Optional, List, Tuple, Dict, Union, cast, Set

import pandas

from gitential2.datatypes import (
    UserCreate,
    UserUpdate,
    UserInDB,
    WorkspaceCreate,
    WorkspaceUpdate,
    WorkspaceInDB,
    UserInfoCreate,
    UserInfoUpdate,
    UserInfoInDB,
    CredentialCreate,
    CredentialUpdate,
    CredentialInDB,
    AccessLog,
)
from gitential2.datatypes.access_approvals import AccessApprovalCreate, AccessApprovalUpdate, AccessApprovalInDB
from gitential2.datatypes.api_keys import PersonalAccessToken, WorkspaceAPIKey
from gitential2.datatypes.authors import AuthorCreate, AuthorUpdate, AuthorInDB, AuthorNamesAndEmails, IdAndTitle
from gitential2.datatypes.auto_export import AutoExportCreate, AutoExportInDB, AutoExportUpdate
from gitential2.datatypes.calculated import CalculatedCommit, CalculatedCommitId, CalculatedPatch, CalculatedPatchId
from gitential2.datatypes.deploys import Deploy, DeployCommit
from gitential2.datatypes.email_log import (
    EmailLogCreate,
    EmailLogUpdate,
    EmailLogInDB,
)
from gitential2.datatypes.extraction import (
    ExtractedCommit,
    ExtractedCommitId,
    ExtractedPatch,
    ExtractedPatchId,
    ExtractedPatchRewrite,
    ExtractedPatchRewriteId,
    ExtractedCommitBranch,
    ExtractedCommitBranchId,
)
from gitential2.datatypes.its_projects import ITSProjectCreate, ITSProjectUpdate, ITSProjectInDB
from gitential2.datatypes.project_its_projects import (
    ProjectITSProjectCreate,
    ProjectITSProjectUpdate,
    ProjectITSProjectInDB,
)
from gitential2.datatypes.project_repositories import (
    ProjectRepositoryCreate,
    ProjectRepositoryInDB,
    ProjectRepositoryUpdate,
)
from gitential2.datatypes.projects import ProjectCreate, ProjectUpdate, ProjectInDB
from gitential2.datatypes.pull_requests import (
    PullRequest,
    PullRequestId,
    PullRequestComment,
    PullRequestCommentId,
    PullRequestCommit,
    PullRequestCommitId,
    PullRequestLabel,
    PullRequestLabelId,
)
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryInDB, RepositoryUpdate
from gitential2.datatypes.reseller_codes import ResellerCode
from gitential2.datatypes.sprints import Sprint
from gitential2.datatypes.subscriptions import SubscriptionCreate, SubscriptionUpdate, SubscriptionInDB
from gitential2.datatypes.teammembers import TeamMemberCreate, TeamMemberInDB, TeamMemberUpdate
from gitential2.datatypes.teams import TeamCreate, TeamUpdate, TeamInDB
from gitential2.datatypes.workspace_invitations import (
    WorkspaceInvitationCreate,
    WorkspaceInvitationUpdate,
    WorkspaceInvitationInDB,
)
from gitential2.datatypes.workspacemember import WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB
from .repositories_base import BaseRepository, BaseWorkspaceScopedRepository
from ...datatypes.charts import ChartCreate, ChartUpdate, ChartInDB
from ...datatypes.dashboards import DashboardInDB, DashboardCreate, DashboardUpdate
from ...datatypes.thumbnails import ThumbnailCreate, ThumbnailUpdate, ThumbnailInDB
from ...datatypes.user_its_projects_cache import (
    UserITSProjectCacheId,
    UserITSProjectCacheCreate,
    UserITSProjectCacheUpdate,
    UserITSProjectCacheInDB,
    UserITSProjectGroup,
)
from ...datatypes.user_repositories_cache import (
    UserRepositoryCacheCreate,
    UserRepositoryCacheUpdate,
    UserRepositoryCacheInDB,
    UserRepositoryCacheId,
    UserRepositoryGroup,
)


class AccessLogRepository(ABC):
    @abstractmethod
    def create(self, log: AccessLog) -> AccessLog:
        pass

    @abstractmethod
    def last_interaction(self, user_id: int) -> Optional[AccessLog]:
        pass

    @abstractmethod
    def delete_for_user(self, user_id: int) -> int:
        pass


class UserRepository(BaseRepository[int, UserCreate, UserUpdate, UserInDB]):
    @abstractmethod
    def get_by_email(self, email: str) -> Optional[UserInDB]:
        pass


class ResellerCodeRepository(BaseRepository[str, ResellerCode, ResellerCode, ResellerCode]):
    def set_user_id(self, reseller_id: str, reseller_code: str, user_id: int) -> ResellerCode:
        rc = self.get_or_error(reseller_code)
        if (rc.reseller_id == reseller_id) and not rc.user_id:
            rc.user_id = user_id
            return self.update(reseller_code, rc)
        else:
            raise ValueError(
                f"Invalid reseller code reseller_id={reseller_id}, reseller_code={reseller_code}, user_id={user_id}"
            )

    @abstractmethod
    def delete_for_user(self, user_id: int) -> int:
        pass


class AccessApprovalRepository(BaseRepository[int, AccessApprovalCreate, AccessApprovalUpdate, AccessApprovalInDB]):
    @abstractmethod
    def delete_for_user(self, user_id: int) -> int:
        pass


class PersonalAccessTokenRepository(BaseRepository[str, PersonalAccessToken, PersonalAccessToken, PersonalAccessToken]):
    @abstractmethod
    def delete_for_user(self, user_id: int) -> int:
        pass


class WorkspaceAPIKeyRepository(BaseRepository[str, WorkspaceAPIKey, WorkspaceAPIKey, WorkspaceAPIKey]):
    @abstractmethod
    def get_all_api_keys_by_workspace_id(self, workspace_id: int) -> List[WorkspaceAPIKey]:
        pass

    @abstractmethod
    def get_single_api_key_by_workspace_id(self, workspace_id: int) -> Optional[WorkspaceAPIKey]:
        pass

    @abstractmethod
    def delete_rows_for_workspace(self, workspace_id: int):
        pass


class DeployRepository(BaseWorkspaceScopedRepository[str, Deploy, Deploy, Deploy]):
    @abstractmethod
    def get_deploy_by_id(self, workspace_id: int, deploy_id: str) -> Optional[Deploy]:
        pass

    @abstractmethod
    def delete_deploy_by_id(self, workspace_id: int, deploy_id: str) -> Optional[str]:
        pass


class DeployCommitRepository(BaseWorkspaceScopedRepository[str, DeployCommit, DeployCommit, DeployCommit]):
    @abstractmethod
    def get_deploy_commits_by_deploy_id(self, workspace_id: int, deploy_id: str) -> List[DeployCommit]:
        pass

    @abstractmethod
    def delete_deploy_commits_by_deploy_id(self, workspace_id: int, deploy_id: str) -> List[str]:
        pass


class SubscriptionRepository(BaseRepository[int, SubscriptionCreate, SubscriptionUpdate, SubscriptionInDB]):
    @abstractmethod
    def get_subscriptions_for_user(self, user_id: int) -> List[SubscriptionInDB]:
        pass

    @abstractmethod
    def delete_for_user(self, user_id: int) -> int:
        pass


class UserInfoRepository(BaseRepository[int, UserInfoCreate, UserInfoUpdate, UserInfoInDB]):
    @abstractmethod
    def get_by_sub_and_integration(self, sub: str, integration_name: str) -> Optional[UserInfoInDB]:
        pass

    @abstractmethod
    def get_for_user(self, user_id: int) -> List[UserInfoInDB]:
        pass

    @abstractmethod
    def get_by_email(self, email: str) -> Optional[UserInfoInDB]:
        pass

    @abstractmethod
    def delete_for_user(self, user_id: int) -> int:
        pass


class CredentialRepository(BaseRepository[int, CredentialCreate, CredentialUpdate, CredentialInDB]):
    @abstractmethod
    def get_by_user_and_integration(self, owner_id: int, integration_name: str) -> Optional[CredentialInDB]:
        pass

    @abstractmethod
    def get_for_user(self, owner_id) -> List[CredentialInDB]:
        pass

    @abstractmethod
    def delete_for_user(self, user_id: int) -> int:
        pass


class WorkspaceRepository(BaseRepository[int, WorkspaceCreate, WorkspaceUpdate, WorkspaceInDB]):
    @abstractmethod
    def get_workspaces_by_ids(self, workspace_ids: List[int]) -> List[WorkspaceInDB]:
        pass


class WorkspaceInvitationRepository(
    BaseRepository[int, WorkspaceInvitationCreate, WorkspaceInvitationUpdate, WorkspaceInvitationInDB]
):
    @abstractmethod
    def get_invitations_for_workspace(self, workspace_id: int) -> List[WorkspaceInvitationInDB]:
        pass

    @abstractmethod
    def get_invitation_by_code(self, invitation_code: str) -> Optional[WorkspaceInvitationInDB]:
        pass

    @abstractmethod
    def delete_rows_for_workspace(self, workspace_id: int):
        pass


class WorkspaceMemberRepository(BaseRepository[int, WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB]):
    @abstractmethod
    def get_for_user(self, user_id: int) -> List[WorkspaceMemberInDB]:
        pass

    @abstractmethod
    def get_for_workspace(self, workspace_id: int) -> List[WorkspaceMemberInDB]:
        pass

    @abstractmethod
    def get_for_workspace_and_user(self, workspace_id: int, user_id: int) -> Optional[WorkspaceMemberInDB]:
        pass

    @abstractmethod
    def delete_rows_for_workspace(self, workspace_id: int):
        pass

    @abstractmethod
    def delete_rows_for_user(self, user_id: int) -> int:
        pass


class AutoExportRepository(BaseRepository[int, AutoExportCreate, AutoExportUpdate, AutoExportInDB]):
    @abstractmethod
    def schedule_exists(self, workspace_id: int, cron_schedule_time: int) -> bool:
        pass

    @abstractmethod
    def delete_rows_for_workspace(self, workspace_id: int) -> bool:
        pass


class UserRepositoriesCacheRepository(
    BaseRepository[UserRepositoryCacheId, UserRepositoryCacheCreate, UserRepositoryCacheUpdate, UserRepositoryCacheInDB]
):
    @abstractmethod
    def get_all_repositories_for_user(self, user_id: int) -> List[UserRepositoryCacheInDB]:
        pass

    @abstractmethod
    def insert_repository_cache_for_user(self, repo: UserRepositoryCacheCreate) -> UserRepositoryCacheInDB:
        pass

    @abstractmethod
    def insert_repositories_cache_for_user(
        self, repos: List[UserRepositoryCacheCreate]
    ) -> List[UserRepositoryCacheInDB]:
        pass

    @abstractmethod
    def delete_cache_for_user(self, user_id: int) -> int:
        pass

    @abstractmethod
    def get_repo_groups(self, user_id: int) -> List[UserRepositoryGroup]:
        pass

    @abstractmethod
    def delete_for_user(self, user_id: int) -> int:
        pass


class UserITSProjectsCacheRepository(
    BaseRepository[UserITSProjectCacheId, UserITSProjectCacheCreate, UserITSProjectCacheUpdate, UserITSProjectCacheInDB]
):
    @abstractmethod
    def get_all_its_project_for_user(self, user_id: int) -> List[UserITSProjectCacheInDB]:
        pass

    @abstractmethod
    def get_its_projects_cache_paginated(
        self,
        user_id: int,
        limit: int,
        offset: int,
        order_by_option: str,
        order_by_direction_is_asc: bool,
        integration_type: Optional[str] = None,
        namespace: Optional[str] = None,
        credential_id: Optional[int] = None,
        search_pattern: Optional[str] = None,
    ) -> Tuple[int, List[ITSProjectCreate]]:
        pass

    @abstractmethod
    def insert_its_project_cache_for_user(self, itsp: UserITSProjectCacheCreate) -> UserITSProjectCacheInDB:
        pass

    @abstractmethod
    def insert_its_projects_cache_for_user(
        self, its_projects: List[UserITSProjectCacheCreate]
    ) -> List[UserITSProjectCacheInDB]:
        pass

    @abstractmethod
    def delete_cache_for_user(self, user_id: int) -> int:
        pass

    @abstractmethod
    def get_its_project_groups(self, user_id: int) -> List[UserITSProjectGroup]:
        pass

    @abstractmethod
    def delete_for_user(self, user_id: int) -> int:
        pass


class ProjectRepository(BaseWorkspaceScopedRepository[int, ProjectCreate, ProjectUpdate, ProjectInDB]):
    @abstractmethod
    def search(self, workspace_id: int, q: str) -> List[ProjectInDB]:
        pass

    @abstractmethod
    def update_sprint_by_project_id(self, workspace_id: int, project_id: int, sprint: Sprint) -> bool:
        pass

    @abstractmethod
    def get_projects_by_ids(self, workspace_id: int, project_ids: List[int]) -> List[ProjectInDB]:
        pass

    @abstractmethod
    def get_projects_ids_and_names(
        self, workspace_id: int, project_ids: Optional[List[int]] = None
    ) -> List[IdAndTitle]:
        pass


class RepositoryRepository(BaseWorkspaceScopedRepository[int, RepositoryCreate, RepositoryUpdate, RepositoryInDB]):
    @abstractmethod
    def get_by_clone_url(self, workspace_id: int, clone_url: str) -> Optional[RepositoryInDB]:
        pass

    @abstractmethod
    def search(self, workspace_id: int, q: str) -> List[RepositoryInDB]:
        pass

    @abstractmethod
    def get_repo_id_info_by_repo_name(self, workspace_id: int, repo_name: str) -> List[Tuple[int, str, str]]:
        pass

    def create_or_update_by_clone_url(
        self, workspace_id: int, obj: Union[RepositoryCreate, RepositoryUpdate, RepositoryInDB]
    ) -> RepositoryInDB:
        existing = self.get_by_clone_url(workspace_id, obj.clone_url)
        if existing:
            return self.update(workspace_id=workspace_id, id_=existing.id, obj=RepositoryUpdate(**obj.dict()))
        else:
            return self.create(workspace_id=workspace_id, obj=cast(RepositoryCreate, obj))

    @abstractmethod
    def delete_repos_by_id(self, workspace_id: int, repo_ids: List[int]):
        pass

    @abstractmethod
    def get_repo_groups(self, workspace_id: int) -> List[UserRepositoryGroup]:
        pass

    @abstractmethod
    def get_repo_groups_with_repo_cache(self, workspace_id: int, user_id: int) -> List[UserRepositoryGroup]:
        pass


class ITSProjectRepository(BaseWorkspaceScopedRepository[int, ITSProjectCreate, ITSProjectUpdate, ITSProjectInDB]):
    @abstractmethod
    def get_by_api_url(self, workspace_id: int, api_url: str) -> Optional[ITSProjectInDB]:
        pass

    @abstractmethod
    def search(self, workspace_id: int, q: str) -> List[ITSProjectInDB]:
        pass

    def create_or_update_by_api_url(
        self, workspace_id: int, obj: Union[ITSProjectCreate, ITSProjectUpdate, ITSProjectInDB]
    ) -> ITSProjectInDB:
        existing = self.get_by_api_url(workspace_id, obj.api_url)
        if existing:
            return self.update(workspace_id=workspace_id, id_=existing.id, obj=ITSProjectUpdate(**obj.dict()))
        else:
            return self.create(workspace_id=workspace_id, obj=cast(ITSProjectCreate, obj))

    @abstractmethod
    def delete_its_projects_by_id(self, workspace_id: int, its_project_ids: List[int]):
        pass

    @abstractmethod
    def get_its_project_groups(self, workspace_id: int) -> List[UserITSProjectGroup]:
        pass

    @abstractmethod
    def get_its_projects_groups_with_cache(self, workspace_id: int, user_id: int) -> List[UserITSProjectGroup]:
        pass


class ProjectRepositoryRepository(
    BaseWorkspaceScopedRepository[int, ProjectRepositoryCreate, ProjectRepositoryUpdate, ProjectRepositoryInDB]
):
    @abstractmethod
    def get_repo_ids_for_project(self, workspace_id: int, project_id: int) -> List[int]:
        pass

    @abstractmethod
    def add_repo_ids_to_project(self, workspace_id: int, project_id: int, repo_ids: List[int]):
        pass

    @abstractmethod
    def remove_repo_ids_from_project(self, workspace_id: int, project_id: int, repo_ids: List[int]):
        pass

    @abstractmethod
    def get_repo_ids_by_project_ids(self, workspace_id: int, project_ids: List[int]) -> List[int]:
        pass

    @abstractmethod
    def get_project_ids_for_repo_ids(self, workspace_id: int, repo_ids: List[int]) -> Dict[int, List[int]]:
        pass

    def update_project_repositories(
        self, workspace_id: int, project_id: int, repo_ids: List[int]
    ) -> Tuple[List[int], List[int], List[int]]:
        current_ids = self.get_repo_ids_for_project(workspace_id=workspace_id, project_id=project_id)
        ids_needs_addition = [r_id for r_id in repo_ids if r_id not in current_ids]
        ids_needs_removal = [r_id for r_id in current_ids if r_id not in repo_ids]
        ids_kept = [r_id for r_id in current_ids if r_id in repo_ids]
        if ids_needs_addition:
            self.add_repo_ids_to_project(workspace_id, project_id, ids_needs_addition)
        if ids_needs_removal:
            self.remove_repo_ids_from_project(workspace_id, project_id, ids_needs_removal)
        return ids_needs_addition, ids_needs_removal, ids_kept

    @abstractmethod
    def get_all_repos_assigned_to_projects(self, workspace_id: int):
        pass

    def add_project_repositories(self, workspace_id: int, project_id: int, repo_ids_to_add: List[int]) -> List[int]:
        current_ids = self.get_repo_ids_for_project(workspace_id=workspace_id, project_id=project_id)
        ids_needs_addition = [r_id for r_id in repo_ids_to_add if r_id not in current_ids]
        if ids_needs_addition:
            self.add_repo_ids_to_project(workspace_id, project_id, ids_needs_addition)
        return ids_needs_addition

    def remove_project_repositories(
        self, workspace_id: int, project_id: int, repo_ids_to_remove: List[int]
    ) -> List[int]:
        current_ids = self.get_repo_ids_for_project(workspace_id=workspace_id, project_id=project_id)
        ids_needs_removal = [r_id for r_id in repo_ids_to_remove if r_id in current_ids]
        if ids_needs_removal:
            self.remove_repo_ids_from_project(workspace_id, project_id, ids_needs_removal)
        return ids_needs_removal


class ProjectITSProjectRepository(
    BaseWorkspaceScopedRepository[int, ProjectITSProjectCreate, ProjectITSProjectUpdate, ProjectITSProjectInDB]
):
    @abstractmethod
    def get_itsp_ids_for_project(self, workspace_id: int, project_id: int) -> List[int]:
        pass

    @abstractmethod
    def add_itsp_ids_to_project(self, workspace_id: int, project_id: int, itsp_ids: List[int]):
        pass

    @abstractmethod
    def remove_itsp_ids_from_project(self, workspace_id: int, project_id: int, itsp_ids: List[int]):
        pass

    def update_its_projects(
        self, workspace_id: int, project_id: int, itsp_ids: List[int]
    ) -> Tuple[List[int], List[int], List[int]]:
        current_ids = self.get_itsp_ids_for_project(workspace_id=workspace_id, project_id=project_id)
        ids_needs_addition = [r_id for r_id in itsp_ids if r_id not in current_ids]
        ids_needs_removal = [r_id for r_id in current_ids if r_id not in itsp_ids]
        ids_kept = [r_id for r_id in current_ids if r_id in itsp_ids]
        if ids_needs_addition:
            self.add_itsp_ids_to_project(workspace_id, project_id, ids_needs_addition)
        if ids_needs_removal:
            self.remove_itsp_ids_from_project(workspace_id, project_id, ids_needs_removal)
        return ids_needs_addition, ids_needs_removal, ids_kept

    def add_its_projects(self, workspace_id: int, project_id: int, itsp_ids_to_add: List[int]) -> List[int]:
        current_ids = self.get_itsp_ids_for_project(workspace_id=workspace_id, project_id=project_id)
        ids_needs_addition = [itsp_id for itsp_id in itsp_ids_to_add if itsp_id not in current_ids]
        if ids_needs_addition:
            self.add_itsp_ids_to_project(workspace_id, project_id, ids_needs_addition)
        return ids_needs_addition

    def remove_its_projects(self, workspace_id: int, project_id: int, itsp_ids_to_remove: List[int]) -> List[int]:
        current_ids = self.get_itsp_ids_for_project(workspace_id=workspace_id, project_id=project_id)
        ids_needs_removal = [itsp_id for itsp_id in itsp_ids_to_remove if itsp_id in current_ids]
        if ids_needs_removal:
            self.remove_itsp_ids_from_project(workspace_id, project_id, ids_needs_removal)
        return ids_needs_removal


class DashboardRepository(BaseWorkspaceScopedRepository[int, DashboardCreate, DashboardUpdate, DashboardInDB]):
    @abstractmethod
    def search(self, workspace_id: int, q: str) -> List[DashboardInDB]:
        pass


class ChartRepository(BaseWorkspaceScopedRepository[int, ChartCreate, ChartUpdate, ChartInDB]):
    @abstractmethod
    def search(self, workspace_id: int, q: str) -> List[ChartInDB]:
        pass


class ThumbnailRepository(BaseWorkspaceScopedRepository[str, ThumbnailCreate, ThumbnailUpdate, ThumbnailInDB]):
    pass


class RepoDFMixin(ABC):
    @abstractmethod
    def get_repo_df(self, workspace_id: int, repo_id: int) -> pandas.DataFrame:
        pass


class ExtractedCommitRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[ExtractedCommitId, ExtractedCommit, ExtractedCommit, ExtractedCommit],
):
    @abstractmethod
    def count(
        self,
        workspace_id: int,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[datetime] = None,
        to_: Optional[datetime] = None,
        keywords: Optional[List[str]] = None,
    ) -> int:
        pass

    @abstractmethod
    def get_list_of_repo_ids_distinct(self, workspace_id: int) -> List[int]:
        pass

    @abstractmethod
    def select_extracted_commits(
        self,
        workspace_id: int,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        repo_ids: Optional[List[int]] = None,
    ) -> List[ExtractedCommit]:
        pass

    @abstractmethod
    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        pass

    @abstractmethod
    def delete_commits(self, workspace_id: int, commit_ids: Optional[List[str]] = None) -> int:
        pass


class ExtractedPatchRepository(
    RepoDFMixin, BaseWorkspaceScopedRepository[ExtractedPatchId, ExtractedPatch, ExtractedPatch, ExtractedPatch]
):
    @abstractmethod
    def delete_extracted_patches(self, workspace_id: int, commit_ids: List[str]) -> int:
        pass

    @abstractmethod
    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        pass


class ExtractedCommitBranchRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[
        ExtractedCommitBranchId, ExtractedCommitBranch, ExtractedCommitBranch, ExtractedCommitBranch
    ],
):
    @abstractmethod
    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        pass

    @abstractmethod
    def delete_extracted_commit_branches(self, workspace_id: int, commit_ids: List[str]) -> int:
        pass


class ExtractedPatchRewriteRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[
        ExtractedPatchRewriteId, ExtractedPatchRewrite, ExtractedPatchRewrite, ExtractedPatchRewrite
    ],
):
    @abstractmethod
    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        pass

    @abstractmethod
    def delete_extracted_patch_rewrites(self, workspace_id: int, commit_ids: List[str]) -> int:
        pass


class CalculatedCommitRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[CalculatedCommitId, CalculatedCommit, CalculatedCommit, CalculatedCommit],
):
    @abstractmethod
    def select(
        self,
        workspace_id: int,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[datetime] = None,
        to_: Optional[datetime] = None,
        author_ids: Optional[List[int]] = None,
        is_merge: Optional[bool] = None,
        keywords: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Iterable[CalculatedCommit]:
        pass

    @abstractmethod
    def count(
        self,
        workspace_id: int,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[datetime] = None,
        to_: Optional[datetime] = None,
        author_ids: Optional[List[int]] = None,
        is_merge: Optional[bool] = None,
        keywords: Optional[List[str]] = None,
    ) -> int:
        pass

    @abstractmethod
    def count_distinct_author_ids(self, workspace_id: int) -> int:
        pass

    @abstractmethod
    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        pass

    @abstractmethod
    def delete_commits(self, workspace_id: int, commit_ids: List[str]) -> int:
        pass


class CalculatedPatchRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[CalculatedPatchId, CalculatedPatch, CalculatedPatch, CalculatedPatch],
):
    @abstractmethod
    def get_all_for_commit(self, workspace_id: int, commit_id: CalculatedCommitId) -> List[CalculatedPatch]:
        pass

    @abstractmethod
    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        pass

    @abstractmethod
    def delete_calculated_patches(self, workspace_id: int, commit_ids: List[str]) -> int:
        pass


class PullRequestRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[PullRequestId, PullRequest, PullRequest, PullRequest],
):
    @abstractmethod
    def get_prs_updated_at(self, workspace_id: int, repository_id: int) -> Dict[int, datetime]:
        pass

    @abstractmethod
    def select(
        self,
        workspace_id: int,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[datetime] = None,
        to_: Optional[datetime] = None,
        developer_ids: Optional[List[int]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Iterable[PullRequest]:
        pass

    @abstractmethod
    def select_pull_requests(
        self,
        workspace_id: int,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        repo_ids: Optional[List[int]] = None,
    ) -> List[PullRequest]:
        pass

    @abstractmethod
    def delete_pull_requests(self, workspace_id: int, pr_ids: Optional[List[PullRequestId]] = None) -> int:
        pass

    @abstractmethod
    def count(
        self,
        workspace_id: int,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[datetime] = None,
        to_: Optional[datetime] = None,
        developer_ids: Optional[List[int]] = None,
    ) -> int:
        pass


class PullRequestCommitRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[PullRequestCommitId, PullRequestCommit, PullRequestCommit, PullRequestCommit],
):
    @abstractmethod
    def delete_pull_request_commits(self, workspace_id: int, pr_ids: Optional[List[PullRequestId]] = None) -> int:
        pass


class PullRequestCommentRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[PullRequestCommentId, PullRequestComment, PullRequestComment, PullRequestComment],
):
    @abstractmethod
    def delete_pull_request_comment(self, workspace_id: int, pr_ids: Optional[List[PullRequestId]] = None) -> int:
        pass


class PullRequestLabelRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[PullRequestLabelId, PullRequestLabel, PullRequestLabel, PullRequestLabel],
):
    @abstractmethod
    def delete_pull_request_labels(self, workspace_id: int, pr_ids: Optional[List[PullRequestId]] = None) -> int:
        pass


class AuthorRepository(BaseWorkspaceScopedRepository[int, AuthorCreate, AuthorUpdate, AuthorInDB]):
    @abstractmethod
    def search(self, workspace_id: int, q: str) -> List[AuthorInDB]:
        pass

    @abstractmethod
    def get_authors_by_author_ids(self, workspace_id: int, author_ids: List[int]) -> List[AuthorInDB]:
        pass

    @abstractmethod
    def get_author_names_and_emails(self, workspace_id: int) -> AuthorNamesAndEmails:
        pass

    @abstractmethod
    def count(self, workspace_id: int) -> int:
        pass

    @abstractmethod
    def change_active_status_authors_by_ids(self, workspace_id: int, author_ids: Set[int], active_status: bool):
        pass

    @abstractmethod
    def get_authors_by_email_and_login(self, workspace_id: int, emails_and_logins: List[str]) -> List[AuthorInDB]:
        pass

    @abstractmethod
    def get_authors_with_null_name_or_email(self, workspace_id: int):
        pass

    @abstractmethod
    def get_by_name_pattern(self, workspace_id: int, author_name: str) -> List[AuthorInDB]:
        pass


class TeamRepository(BaseWorkspaceScopedRepository[int, TeamCreate, TeamUpdate, TeamInDB]):
    @abstractmethod
    def get_teams_by_team_ids(self, workspace_id: int, team_ids: List[int]):
        pass

    @abstractmethod
    def get_teams_ids_and_names(self, workspace_id: int, team_ids: Optional[List[int]] = None) -> List[IdAndTitle]:
        pass


class TeamMemberRepository(BaseWorkspaceScopedRepository[int, TeamMemberCreate, TeamMemberUpdate, TeamMemberInDB]):
    @abstractmethod
    def add_members_to_team(self, workspace_id: int, team_id: int, author_ids: List[int]) -> List[TeamMemberInDB]:
        pass

    @abstractmethod
    def remove_members_from_team(self, workspace_id: int, team_id: int, author_ids: List[int]) -> int:
        pass

    @abstractmethod
    def get_team_member_author_ids(self, workspace_id: int, team_id: int) -> List[int]:
        pass

    @abstractmethod
    def get_author_ids_by_team_ids(self, workspace_id: int, team_ids: List[int]) -> List[int]:
        pass

    @abstractmethod
    def get_author_team_ids(self, workspace_id: int, author_id: int) -> List[int]:
        pass

    @abstractmethod
    def get_team_members_by_author_ids(self, workspace_id: int, author_ids: List[int]) -> List[TeamMemberInDB]:
        pass

    def remove_all_members_from_team(self, workspace_id: int, team_id) -> int:
        author_ids = self.get_team_member_author_ids(workspace_id, team_id)
        return self.remove_members_from_team(workspace_id, team_id, author_ids)


class EmailLogRepository(BaseRepository[int, EmailLogCreate, EmailLogUpdate, EmailLogInDB]):
    def schedule_email(self, user_id: int, template_name: str, scheduled_at: Optional[datetime] = None) -> EmailLogInDB:
        email_log_create = EmailLogCreate(
            user_id=user_id,
            template_name=template_name,
            status="scheduled",
            scheduled_at=scheduled_at or datetime.utcnow(),
        )
        return self.create(email_log_create)

    @abstractmethod
    def email_log_status_update(self, user_id: int, template_name: str, status: str) -> Optional[EmailLogInDB]:
        pass

    @abstractmethod
    def get_emails_to_send(self) -> List[EmailLogInDB]:
        pass

    @abstractmethod
    def cancel_email(self, user_id: int, template: str) -> Optional[EmailLogInDB]:
        pass

    @abstractmethod
    def delete_for_user(self, user_id: int) -> int:
        pass
