import datetime as dt
from collections import defaultdict
from threading import Lock
from typing import Iterable, Optional, Callable, List, cast, Dict, Tuple, Union, Set

import pandas as pd
from ibis.expr.types import TableExpr

from gitential2.backends.base.repositories import (
    AccessLogRepository,
    PersonalAccessTokenRepository,
    WorkspaceAPIKeyRepository,
)
from gitential2.datatypes import (
    CoreModel,
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
from gitential2.datatypes.api_keys import PersonalAccessToken, WorkspaceAPIKey
from gitential2.datatypes.email_log import (
    EmailLogCreate,
    EmailLogUpdate,
    EmailLogInDB,
)
from gitential2.datatypes.project_repositories import (
    ProjectRepositoryCreate,
    ProjectRepositoryUpdate,
    ProjectRepositoryInDB,
)
from gitential2.datatypes.projects import ProjectCreate, ProjectUpdate, ProjectInDB
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryUpdate, RepositoryInDB
from gitential2.datatypes.sprints import Sprint
from gitential2.datatypes.stats import IbisTables
from gitential2.datatypes.workspacemember import WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB
from gitential2.extraction.output import DataCollector, OutputHandler
from gitential2.settings import GitentialSettings
from .base import (
    BaseRepository,
    BaseWorkspaceScopedRepository,
    IdType,
    CreateType,
    UpdateType,
    InDBType,
    UserRepository,
    UserInfoRepository,
    GitentialBackend,
    WorkspaceRepository,
    WorkspaceMemberRepository,
    CredentialRepository,
    ProjectRepository,
    RepositoryRepository,
    ProjectRepositoryRepository,
    EmailLogRepository,
)
from .base.mixins import WithRepositoriesMixin
from ..datatypes.workspaces import WorkspaceDuplicate


class InMemAccessLogRepository(AccessLogRepository):
    def __init__(self):
        self._logs: List[AccessLog] = []

    def create(self, log: AccessLog) -> AccessLog:
        self._logs.append(log)
        return log

    def last_interaction(self, user_id: int) -> Optional[AccessLog]:
        ret: Optional[AccessLog] = None
        for log in self._logs:
            if log.user_id == user_id:
                if ret is None or (log.log_time and ret.log_time and log.log_time > ret.log_time):
                    ret = log
        return ret

    def delete_for_user(self, user_id: int):
        self._logs = [log for log in self._logs if getattr(log, "user_id", None) != user_id]


class InMemRepository(
    BaseRepository[IdType, CreateType, UpdateType, InDBType]
):  # pylint: disable=unsubscriptable-object
    def count_rows(self) -> int:
        return len(self._state)

    def __init__(self, in_db_cls: Callable[..., InDBType]):
        self._state: dict = {}
        self._counter = 1
        self._in_db_cls = in_db_cls
        self._counter_lock = Lock()

    def get(self, id_: IdType) -> Optional[InDBType]:
        return self._state.get(id_)

    def get_by(self, **kwargs) -> Optional[InDBType]:
        unique_fields = cast(CoreModel, self._in_db_cls).unique_fields()
        if not unique_fields:
            return None
        else:
            for obj in self._state.values():
                if all(getattr(obj, k) == v for k, v in kwargs.items()):
                    return obj
        return None

    def insert(self, id_: IdType, obj: InDBType) -> InDBType:
        values = obj.dict()
        self._state[id_] = self._in_db_cls(**values)
        return self._state[id_]

    def create(self, obj: CreateType) -> InDBType:
        values = obj.dict()
        id_ = self._new_id()
        values["id"] = id_
        self._state[id_] = self._in_db_cls(**values)
        return self._state[id_]

    def create_or_update(self, obj: Union[CreateType, UpdateType, InDBType]) -> InDBType:
        id_ = getattr(obj, "id_", None)
        if not id_:
            return self.create(cast(CreateType, obj))
        else:
            return self.update(id_, cast(UpdateType, obj))

    def update(self, id_: IdType, obj: UpdateType) -> InDBType:
        update_dict = obj.dict(exclude_unset=True)
        original_obj = self._state[id_]
        if hasattr(original_obj, "updated_at") and "updated_at" not in update_dict:
            update_dict["updated_at"] = dt.datetime.utcnow()

        updated_obj = original_obj.copy(update=update_dict)
        self._state[id_] = updated_obj
        return self._state[id_]

    def delete(self, id_: IdType) -> int:
        try:
            del self._state[id_]
            return 1
        except KeyError:
            return 0

    def all(self) -> Iterable[InDBType]:
        return self._state.values()

    def truncate(self):
        self._state = {}

    def reset_primary_key_id(self):
        pass

    def _new_id(self):
        with self._counter_lock:
            ret = self._counter
            self._counter += 1
        return ret


def constant_factory(value):
    return lambda: value


class InMemWorkspaceScopedRepository(
    BaseWorkspaceScopedRepository[IdType, CreateType, UpdateType, InDBType]
):  # pylint: disable=unsubscriptable-object
    def count_rows(self, workspace_id: int) -> int:
        return len(self._state)

    def all_ids(self, workspace_id: int) -> List[int]:  # type: ignore[empty-body]
        pass

    def __init__(self, in_db_cls: Callable[..., InDBType]):
        self._state: dict = defaultdict(dict)
        self._counters: dict = defaultdict(constant_factory(1))
        self._in_db_cls = in_db_cls
        self._counter_lock = Lock()

    def get(self, workspace_id: int, id_: IdType) -> Optional[InDBType]:
        return self._state[workspace_id].get(id_)

    def create(self, workspace_id: int, obj: CreateType) -> InDBType:
        values = obj.dict()
        id_ = self._new_id(workspace_id)
        values["id"] = id_
        self._state[workspace_id][id_] = self._in_db_cls(**values)
        return self._state[id_]

    def create_or_update(self, workspace_id: int, obj: Union[CreateType, UpdateType, InDBType]) -> InDBType:
        id_ = getattr(obj, "id_", None)
        if not id_:
            return self.create(workspace_id, cast(CreateType, obj))
        else:
            return self.update(workspace_id, id_, cast(UpdateType, obj))

    def insert(self, workspace_id: int, id_: IdType, obj: InDBType) -> InDBType:
        values = obj.dict()
        self._state[workspace_id][id_] = self._in_db_cls(**values)
        return self._state[id_]

    def update(self, workspace_id: int, id_: IdType, obj: UpdateType) -> InDBType:
        update_dict = obj.dict(exclude_unset=True)
        original_obj = self._state[workspace_id][id_]
        if hasattr(original_obj, "updated_at") and "updated_at" not in update_dict:
            update_dict["updated_at"] = dt.datetime.utcnow()

        updated_obj = original_obj.copy(update=update_dict)
        self._state[workspace_id][id_] = updated_obj
        return self._state[id_]

    def truncate(self, workspace_id: int):
        self._state[workspace_id] = {}

    def reset_primary_key_id(self, workspace_id: int):
        pass

    def delete(self, workspace_id: int, id_: IdType) -> int:
        try:
            del self._state[workspace_id][id_]
            return 1
        except KeyError:
            return 0

    def all(self, workspace_id: int) -> Iterable[InDBType]:
        return self._state[workspace_id].values()

    def iterate_all(self, workspace_id: int) -> Iterable[InDBType]:
        return self._state[workspace_id].values()

    def iterate_desc(self, workspace_id: int) -> Iterable[InDBType]:
        return self._state[workspace_id].values()

    def _new_id(self, workspace_id):
        with self._counter_lock:
            ret = self._counters[workspace_id]
            self._counters[workspace_id] += 1
        return ret


class InMemUserRepository(UserRepository, InMemRepository[int, UserCreate, UserUpdate, UserInDB]):
    def get_by_email(self, email: str) -> Optional[UserInDB]:
        return None


class InMemPersonalAccessTokenRepository(
    PersonalAccessTokenRepository, InMemRepository[str, PersonalAccessToken, PersonalAccessToken, PersonalAccessToken]
):
    pass


class InMemUserInfoRepository(UserInfoRepository, InMemRepository[int, UserInfoCreate, UserInfoUpdate, UserInfoInDB]):
    def get_by_sub_and_integration(self, sub: str, integration_name: str) -> Optional[UserInfoInDB]:
        return None

    def get_for_user(self, user_id: int) -> List[UserInfoInDB]:
        return []

    def get_by_email(self, email: str) -> Optional[UserInfoInDB]:
        return None

    def delete_for_user(self, user_id: int):
        return None


class InMemWorkspaceAPIKeyRepository(
    WorkspaceAPIKeyRepository, InMemRepository[str, WorkspaceAPIKey, WorkspaceAPIKey, WorkspaceAPIKey]
):
    pass


class InMemWorkspaceRepository(
    WorkspaceRepository, InMemRepository[int, WorkspaceCreate, WorkspaceUpdate, WorkspaceInDB]
):
    def get_workspaces_by_ids(self, workspace_ids: List[int]) -> List[WorkspaceInDB]:
        return [item for item in self._state.values() if item.id in workspace_ids]


class InMemWorkspaceMemberRepository(
    WorkspaceMemberRepository,
    InMemRepository[int, WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB],
):
    def get_for_user(self, user_id: int) -> List[WorkspaceMemberInDB]:
        return [item for item in self._state.values() if item.user_id == user_id]

    def get_for_workspace(self, workspace_id: int) -> List[WorkspaceMemberInDB]:
        return [item for item in self._state.values() if item.workspace_id == workspace_id]

    def get_for_workspace_and_user(self, workspace_id: int, user_id: int) -> Optional[WorkspaceMemberInDB]:
        for item in self._state.values():
            if item.workspace_id == workspace_id and item.user_id == user_id:
                return item
        return None

    def delete_rows_for_workspace(self, workspace_id: int):
        for item in self._state.values():
            if item.workspace_id == workspace_id:
                self.delete(id_=item.id)

    def delete_rows_for_user(self, user_id: int):
        for item in self._state.values():
            if item.user_id == user_id:
                self.delete(id_=item.id)


class InMemCredentialRepository(
    CredentialRepository, InMemRepository[int, CredentialCreate, CredentialUpdate, CredentialInDB]
):
    def get_by_user_and_integration(self, owner_id: int, integration_name: str) -> Optional[CredentialInDB]:
        found_credentials = [
            cast(CredentialInDB, item)
            for item in self._state.values()
            if item.owner_id == owner_id and item.integration_name == integration_name
        ]
        return found_credentials[0] if found_credentials else None

    def get_for_user(self, owner_id: int) -> List[CredentialInDB]:
        return [cast(CredentialInDB, item) for item in self._state.values() if item.owner_id == owner_id]

    def delete_for_user(self, user_id: int):
        return None


class InMemProjectRepository(
    ProjectRepository, InMemWorkspaceScopedRepository[int, ProjectCreate, ProjectUpdate, ProjectInDB]
):
    def all_ids(self, workspace_id: int) -> List[int]:  # type: ignore[empty-body]
        pass

    def search(self, workspace_id: int, q: str) -> List[ProjectInDB]:
        return [
            ProjectInDB(**item)
            for item in self._state[workspace_id].values()
            if q.capitalize() in item.name.capitalize()
        ]

    def update_sprint_by_project_id(self, workspace_id: int, project_id: int, sprint: Sprint) -> bool:  # type: ignore[empty-body]
        pass

    def get_projects_by_ids(self, workspace_id: int, project_ids: List[int]) -> List[ProjectInDB]:  # type: ignore[empty-body]
        pass

    def get_projects_ids_and_names(self, workspace_id: int, project_ids: Optional[List[int]] = None):
        pass


class InMemRepositoryRepository(
    RepositoryRepository, InMemWorkspaceScopedRepository[int, RepositoryCreate, RepositoryUpdate, RepositoryInDB]
):
    def all_ids(self, workspace_id: int) -> List[int]:  # type: ignore[empty-body]
        pass

    def get_by_clone_url(self, workspace_id: int, clone_url: str) -> Optional[RepositoryInDB]:
        for o in self._state[workspace_id].values():
            if o.clone_url == clone_url:
                return o
        return None

    def search(self, workspace_id: int, q: str) -> List[RepositoryInDB]:
        return [
            RepositoryInDB(**item)
            for item in self._state[workspace_id].values()
            if q.capitalize() in item.clone_url.capitalize()
        ]

    def get_repo_id_info_by_repo_name(self, workspace_id: int, repo_name: str):
        pass

    def delete_repos_by_id(self, workspace_id: int, repo_ids: List[int]):
        pass

    def get_repo_groups(self, workspace_id: int):
        pass

    def get_repo_groups_with_repo_cache(self, workspace_id: int, user_id: int):
        pass


class InMemEmailLogRepository(EmailLogRepository, InMemRepository[int, EmailLogCreate, EmailLogUpdate, EmailLogInDB]):
    def email_log_status_update(self, user_id: int, template_name: str, status: str) -> Optional[EmailLogInDB]:
        return None

    def get_emails_to_send(self) -> List[EmailLogInDB]:
        return []

    def cancel_email(self, user_id: int, template: str) -> Optional[EmailLogInDB]:
        return None

    def delete_for_user(self, user_id: int):
        return None


class InMemProjectRepositoryRepository(
    ProjectRepositoryRepository,
    InMemWorkspaceScopedRepository[int, ProjectRepositoryCreate, ProjectRepositoryUpdate, ProjectRepositoryInDB],
):
    def all_ids(self, workspace_id: int) -> List[int]:  # type: ignore[empty-body]
        pass

    def get_repo_ids_for_project(self, workspace_id: int, project_id: int) -> List[int]:
        return [item.repo_id for item in self._state[workspace_id] if item.project_id == project_id]

    def add_repo_ids_to_project(self, workspace_id: int, project_id: int, repo_ids: List[int]):
        for repo_id in repo_ids:
            self.create(workspace_id=workspace_id, obj=ProjectRepositoryCreate(project_id=project_id, repo_id=repo_id))

    def remove_repo_ids_from_project(self, workspace_id: int, project_id: int, repo_ids: List[int]):
        needs_delete = [
            item.id for item in self._state[workspace_id] if item.project_id == project_id and item.repo_id in repo_ids
        ]
        for d_id in needs_delete:
            self.delete(workspace_id=workspace_id, id_=d_id)

    def get_repo_ids_by_project_ids(self, workspace_id: int, project_ids: List[int]) -> List[int]:
        return [item.repo_id for item in self._state[workspace_id] if item.project_id in project_ids]

    def get_project_ids_for_repo_ids(self, workspace_id: int, repo_ids: List[int]) -> Dict[int, List[int]]:
        rows = [item for item in self._state[workspace_id] if item.repo_id in repo_ids]
        result: dict = defaultdict(lambda: [])
        for row in rows:
            result[row["repo_id"]].append(row["project_id"])
        return result

    def get_all_repos_assigned_to_projects(self, workspace_id: int):
        pass


class InMemGitentialBackend(WithRepositoriesMixin, GitentialBackend):
    def __init__(self, settings: GitentialSettings):
        super().__init__(settings)
        self._access_logs: AccessLogRepository = InMemAccessLogRepository()
        self._users: UserRepository = InMemUserRepository(in_db_cls=UserInDB)
        self._user_infos: UserInfoRepository = InMemUserInfoRepository(in_db_cls=UserInfoInDB)
        self._workspaces: WorkspaceRepository = InMemWorkspaceRepository(in_db_cls=WorkspaceInDB)
        self._workspace_members: WorkspaceMemberRepository = InMemWorkspaceMemberRepository(
            in_db_cls=WorkspaceMemberInDB
        )
        self._credentials: CredentialRepository = InMemCredentialRepository(in_db_cls=CredentialInDB)
        self._projects: ProjectRepository = InMemProjectRepository(in_db_cls=ProjectInDB)
        self._repositories: RepositoryRepository = InMemRepositoryRepository(in_db_cls=RepositoryInDB)
        self._project_repositories: ProjectRepositoryRepository = InMemProjectRepositoryRepository(
            in_db_cls=ProjectRepositoryInDB
        )
        self._email_log: EmailLogRepository = InMemEmailLogRepository(in_db_cls=EmailLogInDB)
        self._output_handlers: Dict[int, OutputHandler] = defaultdict(DataCollector)

    # def get_accessible_workspaces(self, user_id: int) -> List[WorkspaceWithPermission]:
    #     ret = []
    #     workspace_permissions = self.workspace_permissions.get_for_user(user_id)

    #     for wp in workspace_permissions:
    #         w = self.workspaces.get(wp.workspace_id)
    #         if w:
    #             ret.append(
    #                 WorkspaceWithPermission(
    #                     id=w.id,
    #                     name=w.name,
    #                     role=wp.role,
    #                     primary=wp.primary,
    #                     user_id=wp.user_id,
    #                 )
    #             )
    #     return ret

    def execute_query(self, query):
        pass

    def initialize(self):
        pass

    def initialize_workspace(self, workspace_id: int, workspace_duplicate: Optional[WorkspaceDuplicate] = None):
        pass

    def delete_workspace_schema(self, workspace_id: int):
        pass

    def delete_workspace_sql(self, workspace_id: int):
        pass

    def duplicate_workspace(self, workspace_id_from: int, workspace_id_to: int):
        pass

    def migrate(self):
        pass

    def migrate_workspace(self, workspace_id: int):
        pass

    def reset_workspace(self, workspace_id: int):
        pass

    def delete_schema_revision(self, workspace_id: int):
        pass

    def create_missing_materialized_views(self, workspace_id: int):
        pass

    def drop_existing_materialized_views(self, workspace_id: int):
        pass

    def refresh_materialized_views_in_workspace(self, workspace_id: int):
        pass

    def deactivate_user(self, user_id: int):
        pass

    def purge_user_from_database(self, user_id: int):
        pass

    def delete_own_workspaces_for_user(self, user_id: int):
        pass

    def delete_workspace_collaborations_for_user(self, user_id: int):
        pass

    def output_handler(self, workspace_id: int) -> OutputHandler:
        return self._output_handlers[workspace_id]

    def get_extracted_dataframes(
        self, workspace_id: int, repository_id: int, from_: dt.datetime, to_: dt.datetime
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    def save_calculated_dataframes(
        self,
        workspace_id: int,
        repository_id: int,
        calculated_commits_df: pd.DataFrame,
        calculated_patches_df: pd.DataFrame,
        from_: dt.datetime,
        to_: dt.datetime,
    ):
        return

    def get_ibis_tables(self, workspace_id: int) -> IbisTables:
        return IbisTables()

    def get_ibis_table(self, workspace_id: int, source_name: str) -> TableExpr:
        return TableExpr(None)

    def get_commit_ids_for_repository(self, workspace_id: int, repository_id: int) -> Set[str]:
        return set()
