# pylint: disable=too-many-lines

import datetime as dt
import typing
from collections import defaultdict
from typing import Iterable, Optional, Callable, List, Dict, Union, cast, Set, Tuple

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import func, distinct
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import and_, select, desc, or_, update

from gitential2.backends.base.repositories import (
    AccessApprovalRepository,
    BaseRepository,
    BaseWorkspaceScopedRepository,
    DeployCommitRepository,
    ITSProjectRepository,
    PersonalAccessTokenRepository,
    ProjectITSProjectRepository,
    ResellerCodeRepository,
    UserRepository,
    SubscriptionRepository,
    UserInfoRepository,
    CredentialRepository,
    WorkspaceRepository,
    WorkspaceMemberRepository,
    ProjectRepository,
    RepositoryRepository,
    ProjectRepositoryRepository,
    TeamRepository,
    EmailLogRepository,
    AccessLogRepository,
    AuthorRepository,
    CalculatedCommitRepository,
    CalculatedPatchRepository,
    ExtractedCommitRepository,
    ExtractedPatchRepository,
    ExtractedPatchRewriteRepository,
    PullRequestRepository,
    PullRequestCommitRepository,
    PullRequestCommentRepository,
    PullRequestLabelRepository,
    TeamMemberRepository,
    ExtractedCommitBranchRepository,
    WorkspaceInvitationRepository,
    WorkspaceAPIKeyRepository,
    DeployRepository,
    DashboardRepository,
    ChartRepository,
    ThumbnailRepository,
    AutoExportRepository,
    UserRepositoriesCacheRepository,
    UserITSProjectsCacheRepository,
)
from gitential2.datatypes import (
    UserCreate,
    UserUpdate,
    UserInDB,
    UserInfoCreate,
    UserInfoUpdate,
    UserInfoInDB,
    CredentialCreate,
    CredentialUpdate,
    CredentialInDB,
    WorkspaceCreate,
    WorkspaceUpdate,
    WorkspaceInDB,
    AutoExportCreate,
    AutoExportInDB,
)
from gitential2.datatypes.access_approvals import AccessApprovalCreate, AccessApprovalInDB, AccessApprovalUpdate
from gitential2.datatypes.access_log import AccessLog
from gitential2.datatypes.api_keys import PersonalAccessToken, WorkspaceAPIKey
from gitential2.datatypes.authors import (
    AuthorCreate,
    AuthorInDB,
    AuthorUpdate,
    AuthorNamesAndEmails,
    IdAndTitle,
)
from gitential2.datatypes.auto_export import AutoExportUpdate
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
    ExtractedPatchRewriteId,
    ExtractedPatchRewrite,
    ExtractedCommitBranchId,
    ExtractedCommitBranch,
)
from gitential2.datatypes.its_projects import ITSProjectCreate, ITSProjectInDB, ITSProjectUpdate
from gitential2.datatypes.project_its_projects import (
    ProjectITSProjectCreate,
    ProjectITSProjectInDB,
    ProjectITSProjectUpdate,
)
from gitential2.datatypes.project_repositories import (
    ProjectRepositoryCreate,
    ProjectRepositoryUpdate,
    ProjectRepositoryInDB,
)
from gitential2.datatypes.projects import ProjectCreate, ProjectUpdate, ProjectInDB
from gitential2.datatypes.pull_requests import (
    PullRequest,
    PullRequestComment,
    PullRequestCommentId,
    PullRequestCommit,
    PullRequestId,
    PullRequestCommitId,
    PullRequestLabel,
    PullRequestLabelId,
)
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryUpdate, RepositoryInDB
from gitential2.datatypes.reseller_codes import ResellerCode
from gitential2.datatypes.sprints import Sprint
from gitential2.datatypes.subscriptions import SubscriptionCreate, SubscriptionUpdate, SubscriptionInDB
from gitential2.datatypes.teammembers import TeamMemberCreate, TeamMemberInDB, TeamMemberUpdate
from gitential2.datatypes.teams import TeamCreate, TeamInDB, TeamUpdate
from gitential2.datatypes.workspace_invitations import (
    WorkspaceInvitationCreate,
    WorkspaceInvitationUpdate,
    WorkspaceInvitationInDB,
)
from gitential2.datatypes.workspacemember import WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB
from gitential2.exceptions import NotFoundException
from ..base import (
    IdType,
    CreateType,
    UpdateType,
    InDBType,
)
from ...datatypes.charts import ChartInDB, ChartUpdate, ChartCreate
from ...datatypes.dashboards import DashboardCreate, DashboardUpdate, DashboardInDB
from ...datatypes.thumbnails import ThumbnailInDB, ThumbnailUpdate, ThumbnailCreate
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
from ...utils import get_schema_name, is_string_not_empty, is_list_not_empty

# pylint: disable=unnecessary-lambda-assignment
fetchone_ = lambda result: result.fetchone()
fetchall_ = lambda result: result.fetchall()
inserted_primary_key_ = lambda result: result.inserted_primary_key[0]
rowcount_ = lambda result: result.rowcount


def convert_times_to_utc(values_dict: dict) -> dict:
    def _convert_to_utc_if_dt(v):
        if isinstance(v, dt.datetime):
            if v.tzinfo != dt.timezone.utc:
                return v.astimezone(dt.timezone.utc)
            else:
                return v
        else:
            return v

    return {k: _convert_to_utc_if_dt(v) for k, v in values_dict.items()}


class SQLAccessLogRepository(AccessLogRepository):
    def __init__(self, table: sa.Table, engine: sa.engine.Engine):
        self.table = table
        self.engine = engine

    def create(self, log: AccessLog) -> AccessLog:
        query = self.table.insert().values(**convert_times_to_utc(log.dict()))
        self._execute_query(query)
        return log

    def last_interaction(self, user_id: int) -> Optional[AccessLog]:
        query = (
            self.table.select()
            .where(self.table.c.user_id == user_id)
            .order_by(
                desc(self.table.c.log_time),
            )
        )
        row = self._execute_query(query, callback_fn=fetchone_)
        return AccessLog(**row) if row else None

    def _execute_query(self, query, callback_fn=lambda result: result):
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return callback_fn(result)

    def delete_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLRepository(BaseRepository[IdType, CreateType, UpdateType, InDBType]):  # pylint: disable=unsubscriptable-object
    def __init__(self, table: sa.Table, engine: sa.engine.Engine, in_db_cls: Callable[..., InDBType]):
        self.table = table
        self.engine = engine
        self.in_db_cls = in_db_cls

    def identity(self, id_: IdType):
        return self.table.c.id == id_

    def get(self, id_: IdType) -> Optional[InDBType]:
        query = self.table.select().where(self.identity(id_)).limit(1)
        row = self._execute_query(query, callback_fn=fetchone_)
        return self.in_db_cls(**row) if row else None

    def get_or_error(self, id_: IdType) -> InDBType:
        query = self.table.select().where(self.identity(id_)).limit(1)
        row = self._execute_query(query, callback_fn=fetchone_)
        return self.in_db_cls(**row)

    def create(self, obj: CreateType) -> InDBType:
        query = self.table.insert().values(**convert_times_to_utc(obj.dict()))
        id_ = self._execute_query(query, callback_fn=inserted_primary_key_)
        return self.get_or_error(id_)

    def create_or_update(self, obj: Union[CreateType, UpdateType, InDBType]) -> InDBType:
        id_ = getattr(obj, "id_", None)
        if not id_:
            return self.create(cast(CreateType, obj))
        else:
            values_dict = convert_times_to_utc(obj.dict(exclude_unset=True))
            if "updated_at" in self.table.columns.keys() and "updated_at" not in values_dict:
                values_dict["updated_at"] = dt.datetime.utcnow()
            query = (
                insert(self.table)
                .values(**values_dict)
                .on_conflict_do_update(constraint=f"{self.table.name}_pkey", set_=values_dict)
            )
            self._execute_query(query)
            return self.get_or_error(id_)

    def insert(self, id_: IdType, obj: InDBType) -> InDBType:
        values_dict = convert_times_to_utc(obj.dict(exclude_unset=True))
        values_dict["id"] = id_
        if "updated_at" in self.table.columns.keys() and "updated_at" not in values_dict:
            values_dict["updated_at"] = dt.datetime.utcnow()

        query = self.table.insert().values(**values_dict)
        self._execute_query(query)
        return self.get_or_error(id_)

    def update(self, id_: IdType, obj: UpdateType) -> InDBType:
        update_dict = convert_times_to_utc(obj.dict(exclude_unset=True))
        if "updated_at" in self.table.columns.keys() and "updated_at" not in update_dict:
            update_dict["updated_at"] = dt.datetime.utcnow()

        query = self.table.update().where(self.identity(id_)).values(**update_dict)
        self._execute_query(query)
        return self.get_or_error(id_)

    def delete(self, id_: IdType) -> int:
        query = self.table.delete().where(self.identity(id_))
        return self._execute_query(query, callback_fn=rowcount_)

    def all(self) -> Iterable[InDBType]:
        query = self.table.select()
        rows = self._execute_query(query, callback_fn=fetchall_)
        return (self.in_db_cls(**row) for row in rows)

    def count_rows(self) -> int:
        query = select([func.count()]).select_from(self.table)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return rows[0][0]

    def _execute_query(self, query, callback_fn=lambda result: result):
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return callback_fn(result)

    def truncate(self):
        query = f"TRUNCATE TABLE {self.table.name} CASCADE;"
        self._execute_query(query)

    def reset_primary_key_id(self):
        query = (
            f"SELECT pg_catalog.setval(pg_get_serial_sequence('{self.table.name}', 'id'), "
            f"(SELECT coalesce(max(id), 1) FROM {self.table.name}));"
        )
        self._execute_query(query)


class SQLWorkspaceScopedRepository(
    BaseWorkspaceScopedRepository[IdType, CreateType, UpdateType, InDBType]
):  # pylint: disable=unsubscriptable-object
    def __init__(
        self, table: sa.Table, metadata: sa.MetaData, engine: sa.engine.Engine, in_db_cls: Callable[..., InDBType]
    ):
        self.table = table
        self.engine = engine
        self.metadata = metadata
        self.in_db_cls = in_db_cls

    def identity(self, id_: IdType):
        return self.table.c.id == id_

    def get(self, workspace_id: int, id_: IdType) -> Optional[InDBType]:
        query = self.table.select().where(self.identity(id_)).limit(1)
        row = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchone_)
        return self.in_db_cls(**row) if row else None

    def get_or_error(self, workspace_id: int, id_: IdType) -> InDBType:
        query = self.table.select().where(self.identity(id_)).limit(1)
        row = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchone_)
        if row:
            return self.in_db_cls(**row)
        else:
            raise NotFoundException("Object not found")

    def create(self, workspace_id: int, obj: CreateType) -> InDBType:
        query = self.table.insert().values(**convert_times_to_utc(obj.dict()))
        id_ = self._execute_query(query, workspace_id=workspace_id, callback_fn=inserted_primary_key_)
        return self.get_or_error(workspace_id, id_)

    def create_or_update(self, workspace_id: int, obj: Union[CreateType, UpdateType, InDBType]) -> InDBType:
        id_ = getattr(obj, "id_", None)
        if not id_:
            return self.create(workspace_id, cast(CreateType, obj))
        else:
            values_dict = convert_times_to_utc(obj.dict(exclude_unset=True))
            if "updated_at" in self.table.columns.keys() and "updated_at" not in values_dict:
                values_dict["updated_at"] = dt.datetime.utcnow()
            query = (
                insert(self.table)
                .values(**values_dict)
                .on_conflict_do_update(constraint=f"{self.table.name}_pkey", set_=values_dict)
            )
            self._execute_query(query, workspace_id=workspace_id)
            return self.get_or_error(workspace_id, id_)

    def insert(self, workspace_id: int, id_: IdType, obj: InDBType) -> InDBType:
        values_dict = convert_times_to_utc(obj.dict(exclude_unset=True))
        values_dict["id"] = id_
        if "updated_at" in self.table.columns.keys() and "updated_at" not in values_dict:
            values_dict["updated_at"] = dt.datetime.utcnow()

        query = self.table.insert().values(**values_dict)
        self._execute_query(query, workspace_id=workspace_id)
        return self.get_or_error(workspace_id, id_)

    def update(self, workspace_id: int, id_: IdType, obj: UpdateType) -> InDBType:
        update_dict = convert_times_to_utc(obj.dict(exclude_unset=True))
        if "updated_at" in self.table.columns.keys() and "updated_at" not in update_dict:
            update_dict["updated_at"] = dt.datetime.utcnow()

        query = self.table.update().where(self.identity(id_)).values(**update_dict)
        self._execute_query(query, workspace_id=workspace_id)
        return self.get_or_error(workspace_id, id_)

    def delete(self, workspace_id: int, id_: IdType) -> int:
        query = self.table.delete().where(self.identity(id_))
        return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)

    def all(self, workspace_id: int) -> Iterable[InDBType]:
        query = self.table.select()
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return (self.in_db_cls(**row) for row in rows)

    def all_ids(self, workspace_id: int) -> List[int]:
        query = select([self.table.c.id])
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [r["id"] for r in rows]

    def count_rows(self, workspace_id: int) -> int:
        query = select([func.count()]).select_from(self.table)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return rows[0][0]

    def iterate_all(self, workspace_id: int) -> Iterable[InDBType]:
        query = self.table.select()
        with self._connection_with_schema(workspace_id) as connection:
            proxy = connection.execution_options(stream_results=True).execute(query)
            while True:
                batch = proxy.fetchmany(10000)
                if not batch:
                    break
                for row in batch:
                    yield self.in_db_cls(**row)
            proxy.close()

    def iterate_desc(self, workspace_id: int) -> Iterable[InDBType]:
        query = self.table.select().order_by(desc(self.table.c.created_at))
        with self._connection_with_schema(workspace_id) as connection:
            proxy = connection.execution_options(stream_results=True).execute(query)
            while True:
                batch = proxy.fetchmany(10000)
                if not batch:
                    break
                for row in batch:
                    yield self.in_db_cls(**row)
            proxy.close()

    def truncate(self, workspace_id: int):
        schema_name = self._schema_name(workspace_id)
        query = f"TRUNCATE TABLE `{schema_name}`.`{self.table.name}`;"
        self._execute_query(query, workspace_id=workspace_id)

    def reset_primary_key_id(self, workspace_id: int):
        schema_name = self._schema_name(workspace_id)
        # query = f"ALTER SEQUENCE {schema_name}.{self.table.name}_id_seq RESTART WITH (SELECT max(id)+1 FROM {schema_name}.{self.table.name});"
        query = (
            f"SELECT pg_catalog.setval(pg_get_serial_sequence('{schema_name}.{self.table.name}', 'id'), "
            f"(SELECT coalesce(max(id), 1) FROM {schema_name}.{self.table.name}));"
        )
        self._execute_query(query, workspace_id)

    def _execute_query(
        self, query, workspace_id, values: Optional[List[dict]] = None, callback_fn=lambda result: result
    ):
        with self._connection_with_schema(workspace_id) as connection:
            if values:
                result = connection.execute(query, values)
            else:
                result = connection.execute(query)
            return callback_fn(result)

    def _schema_name(self, workspace_id):
        return get_schema_name(workspace_id)

    def _connection_with_schema(self, workspace_id):
        return self.engine.connect().execution_options(schema_translate_map={None: self._schema_name(workspace_id)})


class SQLUserRepository(UserRepository, SQLRepository[int, UserCreate, UserUpdate, UserInDB]):
    def get_by_email(self, email: str) -> Optional[UserInDB]:
        query = self.table.select().where(self.table.c.email == email)
        result = self._execute_query(query)
        row = result.fetchone()
        return UserInDB(**row) if row else None


class SQLResellerCodeRepository(ResellerCodeRepository, SQLRepository[str, ResellerCode, ResellerCode, ResellerCode]):
    def delete_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLPersonalAccessTokenRepository(
    PersonalAccessTokenRepository, SQLRepository[str, PersonalAccessToken, PersonalAccessToken, PersonalAccessToken]
):
    def delete_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLWorkspaceAPIKeyRepository(
    WorkspaceAPIKeyRepository, SQLRepository[str, WorkspaceAPIKey, WorkspaceAPIKey, WorkspaceAPIKey]
):
    def get_all_api_keys_by_workspace_id(self, workspace_id: int) -> List[WorkspaceAPIKey]:
        query = self.table.select().where(self.table.c.workspace_id == workspace_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [WorkspaceAPIKey(**row) for row in rows]

    def get_single_api_key_by_workspace_id(self, workspace_id: int):
        query = self.table.select().where(self.table.c.workspace_id == workspace_id)
        row = self._execute_query(query, callback_fn=fetchone_)
        if row:
            return WorkspaceAPIKey(**row)
        return None

    def delete_rows_for_workspace(self, workspace_id: int) -> int:
        query = self.table.delete().where(self.table.c.workspace_id == workspace_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLDeployRepository(DeployRepository, SQLWorkspaceScopedRepository[str, Deploy, Deploy, Deploy]):
    def get_deploy_by_id(self, workspace_id: int, deploy_id: str):
        query = self.table.select().where(self.table.c.id == deploy_id)
        row = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchone_)
        if row:
            return Deploy(**row)
        return None

    def delete_deploy_by_id(self, workspace_id: int, deploy_id: str):
        query = self.table.delete().where(self.table.c.id == deploy_id)
        self._execute_query(query, workspace_id=workspace_id)


class SQLDeployCommitRepository(
    DeployCommitRepository, SQLWorkspaceScopedRepository[str, DeployCommit, DeployCommit, DeployCommit]
):
    def get_deploy_commits_by_deploy_id(self, workspace_id: int, deploy_id: str):
        query = self.table.select().where(self.table.c.id == deploy_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        if rows:
            return [DeployCommit(**row) for row in rows]
        return []

    def delete_deploy_commits_by_deploy_id(self, workspace_id: int, deploy_id: str):
        query = self.table.delete().where(self.table.c.deploy_id == deploy_id)
        self._execute_query(query, workspace_id=workspace_id)


class SQLAccessApprovalRepository(
    AccessApprovalRepository, SQLRepository[int, AccessApprovalCreate, AccessApprovalUpdate, AccessApprovalInDB]
):
    def delete_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLSubscriptionRepository(
    SubscriptionRepository, SQLRepository[int, SubscriptionCreate, SubscriptionUpdate, SubscriptionInDB]
):
    def get_subscriptions_for_user(self, user_id: int) -> List[SubscriptionInDB]:
        query = self.table.select().where(self.table.c.user_id == user_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [SubscriptionInDB(**row) for row in rows]

    def delete_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLUserInfoRepository(UserInfoRepository, SQLRepository[int, UserInfoCreate, UserInfoUpdate, UserInfoInDB]):
    def get_by_sub_and_integration(self, sub: str, integration_name: str) -> Optional[UserInfoInDB]:
        query = self.table.select().where(
            and_(self.table.c.sub == sub, self.table.c.integration_name == integration_name)
        )
        row = self._execute_query(query, callback_fn=fetchone_)
        return UserInfoInDB(**row) if row else None

    def get_for_user(self, user_id: int) -> List[UserInfoInDB]:
        query = self.table.select().where(self.table.c.user_id == user_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [UserInfoInDB(**row) for row in rows]

    def get_by_email(self, email: str) -> Optional[UserInfoInDB]:
        query = self.table.select().where(self.table.c.email == email)
        row = self._execute_query(query, callback_fn=fetchone_)
        return UserInfoInDB(**row) if row else None

    def delete_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLCredentialRepository(
    CredentialRepository, SQLRepository[int, CredentialCreate, CredentialUpdate, CredentialInDB]
):
    def get_by_user_and_integration(self, owner_id: int, integration_name: str) -> Optional[CredentialInDB]:
        query = self.table.select().where(
            and_(self.table.c.owner_id == owner_id, self.table.c.integration_name == integration_name)
        )
        row = self._execute_query(query, callback_fn=fetchone_)
        return CredentialInDB(**row) if row else None

    def get_for_user(self, owner_id) -> List[CredentialInDB]:
        query = self.table.select().where(self.table.c.owner_id == owner_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [CredentialInDB(**row) for row in rows]

    def delete_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.owner_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLWorkspaceRepository(WorkspaceRepository, SQLRepository[int, WorkspaceCreate, WorkspaceUpdate, WorkspaceInDB]):
    def get_workspaces_by_ids(self, workspace_ids: List[int]) -> List[WorkspaceInDB]:
        query = self.table.select().where(self.table.c.id.in_(workspace_ids))
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [WorkspaceInDB(**row) for row in rows]


class SQLWorkspaceInvitationRepository(
    WorkspaceInvitationRepository,
    SQLRepository[int, WorkspaceInvitationCreate, WorkspaceInvitationUpdate, WorkspaceInvitationInDB],
):
    def get_invitations_for_workspace(self, workspace_id: int) -> List[WorkspaceInvitationInDB]:
        query = self.table.select().where(self.table.c.workspace_id == workspace_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [WorkspaceInvitationInDB(**row) for row in rows]

    def get_invitation_by_code(self, invitation_code: str) -> Optional[WorkspaceInvitationInDB]:
        query = self.table.select().where(self.table.c.invitation_code == invitation_code)
        row = self._execute_query(query, callback_fn=fetchone_)
        return WorkspaceInvitationInDB(**row) if row else None

    def delete_rows_for_workspace(self, workspace_id: int) -> int:
        query = self.table.delete().where(self.table.c.workspace_id == workspace_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLWorkspaceMemberRepository(
    WorkspaceMemberRepository,
    SQLRepository[int, WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB],
):
    def get_for_user(self, user_id: int) -> List[WorkspaceMemberInDB]:
        query = self.table.select().where(self.table.c.user_id == user_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [WorkspaceMemberInDB(**row) for row in rows]

    def get_for_workspace(self, workspace_id: int) -> List[WorkspaceMemberInDB]:
        query = self.table.select().where(self.table.c.workspace_id == workspace_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [WorkspaceMemberInDB(**row) for row in rows]

    def get_for_workspace_and_user(self, workspace_id: int, user_id: int) -> Optional[WorkspaceMemberInDB]:
        query = self.table.select().where(
            and_(self.table.c.workspace_id == workspace_id, self.table.c.user_id == user_id)
        )
        row = self._execute_query(query, callback_fn=fetchone_)
        return WorkspaceMemberInDB(**row) if row else None

    def delete_rows_for_workspace(self, workspace_id: int) -> int:
        query = self.table.delete().where(self.table.c.workspace_id == workspace_id)
        return self._execute_query(query, callback_fn=rowcount_)

    def delete_rows_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLAutoExportRepository(
    AutoExportRepository, SQLRepository[int, AutoExportCreate, AutoExportUpdate, AutoExportInDB]
):
    def schedule_exists(self, workspace_id: int, cron_schedule_time: int) -> bool:
        """
        @desc: Checks if a schedule already exists, to prevent creating multiple schedules for the same workspace
        """
        query = self.table.select().where(
            and_(self.table.c.workspace_id == workspace_id, self.table.c.cron_schedule_time == cron_schedule_time)
        )
        row = self._execute_query(query, callback_fn=fetchone_)
        return bool(row)

    def delete_rows_for_workspace(self, workspace_id: int) -> bool:
        query = self.table.delete().where(self.table.c.workspace_id == workspace_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLUserRepositoryCacheRepository(
    UserRepositoriesCacheRepository,
    SQLRepository[UserRepositoryCacheId, UserRepositoryCacheCreate, UserRepositoryCacheUpdate, UserRepositoryCacheInDB],
):
    def identity(self, id_: UserRepositoryCacheId):
        return and_(
            self.table.c.user_id == id_.user_id,
            self.table.c.repo_provider_id == id_.repo_provider_id,
            self.table.c.integration_type == id_.integration_type,
        )

    def get_all_repositories_for_user(self, user_id: int) -> List[UserRepositoryCacheInDB]:
        query = self.table.select().where(self.table.c.user_id == user_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [UserRepositoryCacheInDB(**row) for row in rows]

    def insert_repository_cache_for_user(self, repo: UserRepositoryCacheCreate) -> UserRepositoryCacheInDB:
        return self.create(repo)

    def insert_repositories_cache_for_user(
        self, repos: List[UserRepositoryCacheCreate]
    ) -> List[UserRepositoryCacheInDB]:
        results: List[UserRepositoryCacheInDB] = []
        for repo in repos:
            repo_saved_or_updated = self.create_or_update(repo)
            results.append(repo_saved_or_updated)
        return results

    def delete_cache_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)

    def get_repo_groups(self, user_id: int) -> List[UserRepositoryGroup]:
        query = (
            self.table.select()
            .distinct(self.table.c.integration_type, self.table.c.namespace, self.table.c.credential_id)
            .where(self.table.c.user_id == user_id)
        )
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [
            UserRepositoryGroup(
                integration_type=row["integration_type"],
                namespace=row["namespace"],
                credential_id=row["credential_id"],
            )
            for row in rows
        ]

    def delete_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLUserITSProjectsCacheRepository(
    UserITSProjectsCacheRepository,
    SQLRepository[UserITSProjectCacheId, UserITSProjectCacheCreate, UserITSProjectCacheUpdate, UserITSProjectCacheInDB],
):
    def identity(self, id_: UserITSProjectCacheId):
        return and_(
            self.table.c.user_id == id_.user_id,
            self.table.c.integration_id == id_.integration_id,
            self.table.c.integration_type == id_.integration_type,
        )

    def get_all_its_project_for_user(self, user_id: int) -> List[UserITSProjectCacheInDB]:
        query = self.table.select().where(self.table.c.user_id == user_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [UserITSProjectCacheInDB(**row) for row in rows]

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
        def get_filters():
            def get_filter(column_name: str, filter_value: Union[str, int, None]) -> Union[str, None]:
                return f"{column_name} = '{filter_value}'" if filter_value else None

            user_id_filter: str = f"user_id = {user_id}"
            name_filter: Optional[str] = (
                f"name ILIKE '{search_pattern.replace('%', '%%')}'" if is_string_not_empty(search_pattern) else None
            )
            integration_type_filter: Optional[str] = get_filter("integration_type", integration_type)
            namespace_filter: Optional[str] = get_filter("namespace", namespace)
            credential_id_filter: Optional[str] = get_filter("credential_id", credential_id)
            filters: str = " AND ".join(
                [
                    f
                    for f in [
                        user_id_filter,
                        name_filter,
                        integration_type_filter,
                        namespace_filter,
                        credential_id_filter,
                    ]
                    if is_string_not_empty(f)
                ]
            )
            return f"WHERE {filters}" if is_string_not_empty(filters) else ""

        query = (
            "WITH its_project_selection AS "
            "    (SELECT * "
            "    FROM public.user_its_projects_cache "
            f"       {get_filters()}) "
            "SELECT * FROM ("
            "    TABLE its_project_selection "
            f"   ORDER BY {order_by_option} {'ASC' if order_by_direction_is_asc else 'DESC'} "
            f"   LIMIT {limit} "
            f"   OFFSET {offset}) sub "
            "RIGHT JOIN (SELECT COUNT(*) FROM its_project_selection) c(total_count) ON TRUE;"
        )

        rows = self._execute_query(query, callback_fn=fetchall_)
        total_count = rows[0]["total_count"] if is_list_not_empty(rows) else 0

        its_projects_cache = [
            ITSProjectCreate(
                name=row["name"],
                namespace=row["namespace"],
                private=row["private"],
                api_url=row["api_url"],
                key=row["key"],
                integration_type=row["integration_type"],
                integration_name=row["integration_name"],
                integration_id=row["integration_id"],
                credential_id=row["credential_id"],
                extra=row["extra"],
            )
            for row in rows
            if "api_url" in row and row["api_url"]
        ]

        return total_count, its_projects_cache

    def insert_its_project_cache_for_user(self, itsp: UserITSProjectCacheCreate) -> UserITSProjectCacheInDB:
        return self.create(itsp)

    def insert_its_projects_cache_for_user(
        self, its_projects: List[UserITSProjectCacheCreate]
    ) -> List[UserITSProjectCacheInDB]:
        results: List[UserITSProjectCacheInDB] = []
        for itsp in its_projects:
            itsp_saved_or_updated = self.create_or_update(itsp)
            results.append(itsp_saved_or_updated)
        return results

    def delete_cache_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)

    def get_its_project_groups(self, user_id: int) -> List[UserITSProjectGroup]:
        query = (
            self.table.select()
            .distinct(self.table.c.integration_type, self.table.c.namespace, self.table.c.credential_id)
            .where(self.table.c.user_id == user_id)
        )
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [
            UserITSProjectGroup(
                integration_type=row["integration_type"],
                namespace=row["namespace"],
                credential_id=row["credential_id"],
            )
            for row in rows
        ]

    def delete_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)


class SQLProjectRepository(
    ProjectRepository, SQLWorkspaceScopedRepository[int, ProjectCreate, ProjectUpdate, ProjectInDB]
):
    def search(self, workspace_id: int, q: str) -> List[ProjectInDB]:
        query = self.table.select().where(self.table.c.name.ilike(f"%{q}%"))
        rows = self._execute_query(query, workspace_id)
        return [ProjectInDB(**row) for row in rows]

    def update_sprint_by_project_id(self, workspace_id: int, project_id: int, sprint: Sprint):
        query = self.table.update(values={self.table.c.sprint: sprint}).where(self.table.c.id == project_id)
        self._execute_query(query, workspace_id)
        return True

    def get_projects_by_ids(self, workspace_id: int, project_ids: List[int]) -> List[ProjectInDB]:
        query = self.table.select().where(self.table.c.id.in_(project_ids))
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [ProjectInDB(**row) for row in rows]

    def get_projects_ids_and_names(
        self, workspace_id: int, project_ids: Optional[List[int]] = None
    ) -> List[IdAndTitle]:
        if project_ids:
            query = select([self.table.c.id, self.table.c.name.label("title")]).where(self.table.c.id.in_(project_ids))
        else:
            query = select([self.table.c.id, self.table.c.name.label("title")])
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [IdAndTitle(**row) for row in rows]


class SQLRepositoryRepository(
    RepositoryRepository, SQLWorkspaceScopedRepository[int, RepositoryCreate, RepositoryUpdate, RepositoryInDB]
):
    def search(self, workspace_id: int, q: str) -> List[RepositoryInDB]:
        query = self.table.select().where(self.table.c.clone_url.ilike(f"%{q}%"))
        rows = self._execute_query(query, workspace_id)
        return [RepositoryInDB(**row) for row in rows]

    def get_by_clone_url(self, workspace_id: int, clone_url: str) -> Optional[RepositoryInDB]:
        query = self.table.select().where(self.table.c.clone_url == clone_url)
        row = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchone_)
        return RepositoryInDB(**row) if row else None

    def get_repo_id_info_by_repo_name(self, workspace_id: int, repo_name: str):
        query = select([self.table.c.id, self.table.c.clone_url, self.table.c.namespace]).where(
            self.table.c.name == repo_name
        )
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return rows

    def delete_repos_by_id(self, workspace_id: int, repo_ids: List[int]):
        query = self.table.delete().where(self.table.c.id.in_(repo_ids))
        return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)

    def get_repo_groups(self, workspace_id: int) -> List[UserRepositoryGroup]:
        query = self.table.select().distinct(
            self.table.c.integration_type, self.table.c.namespace, self.table.c.credential_id
        )
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [
            UserRepositoryGroup(
                integration_type=row["integration_type"],
                namespace=row["namespace"],
                credential_id=row["credential_id"],
            )
            for row in rows
        ]

    def get_repo_groups_with_repo_cache(self, workspace_id: int, user_id: int) -> List[UserRepositoryGroup]:
        schema_name: str = self._schema_name(workspace_id=workspace_id)
        query = (
            "WITH repo_selection AS "
            "    (SELECT integration_type, namespace, credential_id, clone_url "
            "    FROM public.user_repositories_cache "
            f"    WHERE user_id = {user_id} "
            "    UNION "
            "    SELECT integration_type, namespace, credential_id, clone_url "
            f"   FROM {schema_name}.repositories) "
            "SELECT integration_type, namespace, credential_id, COUNT(*) AS total_count "
            "FROM repo_selection "
            "GROUP BY integration_type, namespace, credential_id "
            "ORDER BY integration_type, namespace;"
        )
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [UserRepositoryGroup(**row) for row in rows]


class SQLITSProjectRepository(
    ITSProjectRepository, SQLWorkspaceScopedRepository[int, ITSProjectCreate, ITSProjectUpdate, ITSProjectInDB]
):
    def search(self, workspace_id: int, q: str) -> List[ITSProjectInDB]:
        query = self.table.select().where(self.table.c.api_url.ilike(f"%{q}%"))
        rows = self._execute_query(query, workspace_id)
        return [ITSProjectInDB(**row) for row in rows]

    def get_by_api_url(self, workspace_id: int, api_url: str) -> Optional[ITSProjectInDB]:
        query = self.table.select().where(self.table.c.api_url == api_url)
        row = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchone_)
        return ITSProjectInDB(**row) if row else None

    def delete_its_projects_by_id(self, workspace_id: int, its_project_ids: List[int]):
        query = self.table.delete().where(self.table.c.id.in_(its_project_ids))
        return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)

    def get_its_project_groups(self, workspace_id: int) -> List[UserITSProjectGroup]:
        query = self.table.select().distinct(
            self.table.c.integration_type, self.table.c.namespace, self.table.c.credential_id
        )
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [
            UserITSProjectGroup(
                integration_type=row["integration_type"],
                namespace=row["namespace"],
                credential_id=row["credential_id"],
            )
            for row in rows
        ]

    def get_its_projects_groups_with_cache(self, workspace_id: int, user_id: int) -> List[UserITSProjectGroup]:
        schema_name: str = self._schema_name(workspace_id=workspace_id)
        query = (
            "WITH its_project_selection AS "
            "    (SELECT integration_type, namespace, credential_id, api_url "
            "    FROM public.user_its_projects_cache "
            f"    WHERE user_id = {user_id} "
            "    UNION "
            "    SELECT integration_type, namespace, credential_id, api_url "
            f"   FROM {schema_name}.its_projects) "
            "SELECT integration_type, namespace, credential_id, COUNT(*) AS total_count "
            "FROM its_project_selection "
            "GROUP BY integration_type, namespace, credential_id "
            "ORDER BY integration_type, namespace;"
        )
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [UserITSProjectGroup(**row) for row in rows]


class SQLProjectRepositoryRepository(
    ProjectRepositoryRepository,
    SQLWorkspaceScopedRepository[int, ProjectRepositoryCreate, ProjectRepositoryUpdate, ProjectRepositoryInDB],
):
    def get_repo_ids_for_project(self, workspace_id: int, project_id: int) -> List[int]:
        query = select([self.table.c.repo_id]).where(self.table.c.project_id == project_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [r["repo_id"] for r in rows]

    def add_repo_ids_to_project(self, workspace_id: int, project_id: int, repo_ids: List[int]):
        query = self.table.insert()
        self._execute_query(
            query,
            workspace_id=workspace_id,
            values=[{"project_id": project_id, "repo_id": repo_id} for repo_id in repo_ids],
        )

    def remove_repo_ids_from_project(self, workspace_id: int, project_id: int, repo_ids: List[int]):
        query = self.table.delete().where(
            and_(self.table.c.project_id == project_id, self.table.c.repo_id.in_(repo_ids))
        )
        self._execute_query(query, workspace_id=workspace_id)

    def get_repo_ids_by_project_ids(self, workspace_id: int, project_ids: List[int]) -> List[int]:
        query = select([self.table.c.repo_id]).where(self.table.c.project_id.in_(project_ids))
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["repo_id"] for row in rows]

    def get_project_ids_for_repo_ids(self, workspace_id: int, repo_ids: List[int]) -> Dict[int, List[int]]:
        query = self.table.select().where(self.table.c.repo_id.in_(repo_ids))
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        result: dict = defaultdict(lambda: [])
        for row in rows:
            result[row["repo_id"]].append(row["project_id"])
        return result

    def get_all_repos_assigned_to_projects(self, workspace_id: int):
        query = self.table.select().distinct(self.table.c.repo_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["repo_id"] for row in rows]


class SQLProjectITSProjectRepository(
    ProjectITSProjectRepository,
    SQLWorkspaceScopedRepository[int, ProjectITSProjectCreate, ProjectITSProjectUpdate, ProjectITSProjectInDB],
):
    def get_itsp_ids_for_project(self, workspace_id: int, project_id: int) -> List[int]:
        query = select([self.table.c.itsp_id]).where(self.table.c.project_id == project_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [r["itsp_id"] for r in rows]

    def add_itsp_ids_to_project(self, workspace_id: int, project_id: int, itsp_ids: List[int]):
        query = self.table.insert()
        self._execute_query(
            query,
            workspace_id=workspace_id,
            values=[{"project_id": project_id, "itsp_id": itsp_id} for itsp_id in itsp_ids],
        )

    def remove_itsp_ids_from_project(self, workspace_id: int, project_id: int, itsp_ids: List[int]):
        query = self.table.delete().where(self.table.c.itsp_id.in_(itsp_ids))
        self._execute_query(query, workspace_id=workspace_id)


class SQLDashboardRepository(
    DashboardRepository, SQLWorkspaceScopedRepository[int, DashboardCreate, DashboardUpdate, DashboardInDB]
):
    def search(self, workspace_id: int, q: str) -> List[DashboardInDB]:
        query = self.table.select().where(self.table.c.title.ilike(f"%{q}%"))
        rows = self._execute_query(query, workspace_id)
        return [DashboardInDB(**row) for row in rows]


class SQLChartRepository(ChartRepository, SQLWorkspaceScopedRepository[int, ChartCreate, ChartUpdate, ChartInDB]):
    def search(self, workspace_id: int, q: str) -> List[ChartInDB]:
        query = self.table.select().where(self.table.c.title.ilike(f"%{q}%"))
        rows = self._execute_query(query, workspace_id)
        return [ChartInDB(**row) for row in rows]


class SQLThumbnailRepository(
    ThumbnailRepository, SQLWorkspaceScopedRepository[str, ThumbnailCreate, ThumbnailUpdate, ThumbnailInDB]
):
    pass


class SQLAuthorRepository(AuthorRepository, SQLWorkspaceScopedRepository[int, AuthorCreate, AuthorUpdate, AuthorInDB]):
    def search(self, workspace_id: int, q: str) -> List[AuthorInDB]:
        query = self.table.select().where(or_(self.table.c.name.ilike(f"%{q}%"), self.table.c.email.ilike(f"%{q}%")))
        rows = self._execute_query(query, workspace_id)
        return [AuthorInDB(**row) for row in rows]

    def get_authors_by_author_ids(self, workspace_id: int, author_ids: List[int]) -> List[AuthorInDB]:
        query = self.table.select().where(self.table.c.id.in_(author_ids))
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [AuthorInDB(**row) for row in rows]

    def get_author_names_and_emails(self, workspace_id: int) -> AuthorNamesAndEmails:
        query = select([self.table.c.name, self.table.c.email])
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)

        names_set: Set[str] = set()
        emails_set: Set[str] = set()
        for row in rows:
            if is_string_not_empty(row[0]):
                names_set.add(row[0])
            if is_string_not_empty(row[1]):
                emails_set.add(row[1])

        names: List[str] = list(names_set)
        names.sort()
        emails: List[str] = list(emails_set)
        emails.sort()

        return AuthorNamesAndEmails(names=names, emails=emails)

    def get_authors_with_null_name_or_email(self, workspace_id: int):
        query = self.table.select().where(or_(self.table.c.name.is_(None), self.table.c.email.is_(None)))
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [AuthorInDB(**row) for row in rows]

    def count(self, workspace_id: int) -> int:
        query = select([func.count()]).select_from(self.table)
        with self._connection_with_schema(workspace_id) as connection:
            result = connection.execute(query)
            return result.fetchone()[0]

    def change_active_status_authors_by_ids(self, workspace_id: int, author_ids: Set[int], active_status: bool):
        query = update(self.table).where(self.table.c.id.in_(author_ids)).values(active=active_status)
        self._execute_query(query, workspace_id=workspace_id)

    def get_authors_by_email_and_login(self, workspace_id: int, emails_and_logins: List[str]) -> List[AuthorInDB]:
        result = []
        if is_list_not_empty(emails_and_logins):
            schema_name = self._schema_name(workspace_id)
            list_str = ",".join(f"'{e}'" for e in emails_and_logins)
            query = (
                "SELECT DISTINCT ON (results.id) id, "
                "results.active, results.name, results.email, results.aliases, results.extra "
                "FROM ("
                f"SELECT * FROM {schema_name}.authors a, JSON_ARRAY_ELEMENTS(a.aliases) al "
                f"WHERE ((al ->> 'email') IN ({list_str})) "
                f"OR ((al ->> 'login') IN ({list_str}))"
                ") AS results"
            )
            rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
            result = [AuthorInDB(**row) for row in rows]
        return result

    def get_by_name_pattern(self, workspace_id: int, author_name: str) -> List[AuthorInDB]:
        author_name_pattern: str = f"%{author_name}%"
        query = (
            self.table.select().where(self.table.c.name.ilike(author_name_pattern)).order_by(self.table.c.name.asc())
        )
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [AuthorInDB(**row) for row in rows]


class SQLTeamRepository(TeamRepository, SQLWorkspaceScopedRepository[int, TeamCreate, TeamUpdate, TeamInDB]):
    def get_teams_by_team_ids(self, workspace_id: int, team_ids: List[int]) -> List[TeamInDB]:
        query = self.table.select().where(self.table.c.id.in_(team_ids))
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [TeamInDB(**row) for row in rows]

    def get_teams_ids_and_names(self, workspace_id: int, team_ids: Optional[List[int]] = None) -> List[IdAndTitle]:
        if team_ids:
            query = select([self.table.c.id, self.table.c.name.label("title")]).where(self.table.c.id.in_(team_ids))
        else:
            query = select([self.table.c.id, self.table.c.name.label("title")])
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [IdAndTitle(**row) for row in rows]


class SQLTeamMemberRepository(
    TeamMemberRepository, SQLWorkspaceScopedRepository[int, TeamMemberCreate, TeamMemberUpdate, TeamMemberInDB]
):
    def add_members_to_team(self, workspace_id: int, team_id: int, author_ids: List[int]) -> List[TeamMemberInDB]:
        query = self.table.insert([{"team_id": team_id, "author_id": author_id} for author_id in author_ids]).returning(
            self.table.c.id, self.table.c.team_id, self.table.c.author_id
        )
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [TeamMemberInDB(**row) for row in rows]

    def remove_members_from_team(self, workspace_id: int, team_id: int, author_ids: List[int]) -> int:
        query = self.table.delete().where(and_(self.table.c.team_id == team_id, self.table.c.author_id.in_(author_ids)))
        return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)

    def get_author_team_ids(self, workspace_id: int, author_id: int) -> List[int]:
        query = select([self.table.c.team_id]).where(self.table.c.author_id == author_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["team_id"] for row in rows]

    def get_team_member_author_ids(self, workspace_id: int, team_id: int) -> List[int]:
        query = select([self.table.c.author_id]).where(self.table.c.team_id == team_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["author_id"] for row in rows]

    def get_author_ids_by_team_ids(self, workspace_id: int, team_ids: List[int]) -> List[int]:
        query = select([self.table.c.author_id]).where(self.table.c.team_id.in_(team_ids))
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["author_id"] for row in rows]

    def get_team_members_by_author_ids(self, workspace_id: int, author_ids: List[int]) -> List[TeamMemberInDB]:
        query = self.table.select().where(self.table.c.author_id.in_(author_ids))
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [TeamMemberInDB(**row) for row in rows]


class SQLRepoDFMixin:

    if typing.TYPE_CHECKING:
        _connection_with_schema: Callable
        table: sa.Table

    def get_repo_df(self, workspace_id: int, repo_id: int) -> pd.DataFrame:
        with self._connection_with_schema(workspace_id) as connection:
            df = pd.read_sql_query(
                sql=self.table.select().where(self.table.c.repo_id == repo_id),
                con=connection,
            )
            return df


class SQLExtractedCommitRepository(
    SQLRepoDFMixin,
    ExtractedCommitRepository,
    SQLWorkspaceScopedRepository[ExtractedCommitId, ExtractedCommit, ExtractedCommit, ExtractedCommit],
):
    def identity(self, id_: ExtractedCommitId):
        return and_(self.table.c.commit_id == id_.commit_id, self.table.c.repo_id == id_.repo_id)

    def count(
        self,
        workspace_id: int,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[dt.datetime] = None,
        to_: Optional[dt.datetime] = None,
        keywords: Optional[List[str]] = None,
    ) -> int:
        query = select([func.count()]).select_from(self.table)
        query = self._build_filters(query, repository_ids, from_, to_, keywords)
        with self._connection_with_schema(workspace_id) as connection:
            result = connection.execute(query)
            return result.fetchone()[0]

    def get_list_of_repo_ids_distinct(self, workspace_id: int) -> List[int]:
        query = select([distinct(self.table.c.repo_id)]).select_from(self.table)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [r["repo_id"] for r in rows]

    def select_extracted_commits(
        self,
        workspace_id: int,
        date_from: Optional[dt.datetime] = None,
        date_to: Optional[dt.datetime] = None,
        repo_ids: Optional[List[int]] = None,
    ) -> List[ExtractedCommit]:
        or_clause = []
        if date_from:
            or_clause.append(self.table.c.atime >= date_from)
            or_clause.append(self.table.c.ctime >= date_from)
        if date_to:
            or_clause.append(self.table.c.atime <= date_to)
            or_clause.append(self.table.c.ctime <= date_to)
        if is_list_not_empty(repo_ids):
            or_clause.append(self.table.c.repo_id.in_(repo_ids))
        query = self.table.select().where(or_(*or_clause)) if is_list_not_empty(or_clause) else self.table.select()
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [ExtractedCommit(**row) for row in rows]

    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        query = self.table.select().distinct(self.table.c.commit_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["commit_id"] for row in rows]

    def delete_commits(self, workspace_id: int, commit_ids: Optional[List[str]] = None) -> int:
        if is_list_not_empty(commit_ids):
            query = self.table.delete().where(self.table.c.commit_id.in_(commit_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0

    def _build_filters(
        self,
        query,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[dt.datetime] = None,
        to_: Optional[dt.datetime] = None,
        keywords: Optional[List[str]] = None,
    ):
        if repository_ids:
            query = query.where(self.table.c.repo_id.in_(repository_ids))
        if from_:
            query = query.where(self.table.c.atime >= from_)
        if to_:
            query = query.where(self.table.c.atime < to_)
        if keywords:
            for keyword in keywords:
                if keyword:
                    query = query.where(self.table.c.message.ilike(f"%{keyword}%"))
        return query


class SQLExtractedCommitBranchRepository(
    SQLRepoDFMixin,
    ExtractedCommitBranchRepository,
    SQLWorkspaceScopedRepository[
        ExtractedCommitBranchId, ExtractedCommitBranch, ExtractedCommitBranch, ExtractedCommitBranch
    ],
):
    def identity(self, id_: ExtractedCommitBranchId):
        return and_(
            self.table.c.commit_id == id_.commit_id,
            self.table.c.repo_id == id_.repo_id,
            self.table.c.branch == id_.branch,
        )

    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        query = self.table.select().distinct(self.table.c.commit_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["commit_id"] for row in rows]

    def delete_extracted_commit_branches(self, workspace_id: int, commit_ids: List[str]) -> int:
        if is_list_not_empty(commit_ids):
            query = self.table.delete().where(self.table.c.commit_id.in_(commit_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0


class SQLExtractedPatchRepository(
    SQLRepoDFMixin,
    ExtractedPatchRepository,
    SQLWorkspaceScopedRepository[ExtractedPatchId, ExtractedPatch, ExtractedPatch, ExtractedPatch],
):
    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        query = self.table.select().distinct(self.table.c.commit_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["commit_id"] for row in rows]

    def delete_extracted_patches(self, workspace_id: int, commit_ids: List[str]) -> int:
        if is_list_not_empty(commit_ids):
            query = self.table.delete().where(self.table.c.commit_id.in_(commit_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0

    def identity(self, id_: ExtractedPatchId):
        return and_(
            self.table.c.commit_id == id_.commit_id,
            self.table.c.repo_id == id_.repo_id,
            self.table.c.parent_commit_id == id_.parent_commit_id,
            self.table.c.newpath == id_.newpath,
        )


class SQLExtractedPatchRewriteRepository(
    SQLRepoDFMixin,
    ExtractedPatchRewriteRepository,
    SQLWorkspaceScopedRepository[
        ExtractedPatchRewriteId, ExtractedPatchRewrite, ExtractedPatchRewrite, ExtractedPatchRewrite
    ],
):
    def identity(self, id_: ExtractedPatchRewriteId):
        return and_(
            self.table.c.commit_id == id_.commit_id,
            self.table.c.repo_id == id_.repo_id,
            self.table.c.newpath == id_.newpath,
            self.table.c.rewritten_commit_id == id_.rewritten_commit_id,
        )

    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        query = self.table.select().distinct(self.table.c.commit_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["commit_id"] for row in rows]

    def delete_extracted_patch_rewrites(self, workspace_id: int, commit_ids: List[str]) -> int:
        if is_list_not_empty(commit_ids):
            query = self.table.delete().where(self.table.c.commit_id.in_(commit_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0


class SQLCalculatedCommitRepository(
    SQLRepoDFMixin,
    CalculatedCommitRepository,
    SQLWorkspaceScopedRepository[CalculatedCommitId, CalculatedCommit, CalculatedCommit, CalculatedCommit],
):
    def identity(self, id_: CalculatedCommitId):
        return and_(self.table.c.commit_id == id_.commit_id, self.table.c.repo_id == id_.repo_id)

    def select(
        self,
        workspace_id: int,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[dt.datetime] = None,
        to_: Optional[dt.datetime] = None,
        author_ids: Optional[List[int]] = None,
        is_merge: Optional[bool] = None,
        keywords: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Iterable[CalculatedCommit]:
        query = self.table.select().order_by(self.table.c.date.desc()).limit(limit).offset(offset)
        query = self._build_filters(query, repository_ids, from_, to_, author_ids, is_merge, keywords)
        with self._connection_with_schema(workspace_id) as connection:
            proxy = connection.execution_options(stream_results=True).execute(query)
            while True:
                batch = proxy.fetchmany(10000)
                if not batch:
                    break
                for row in batch:
                    yield self.in_db_cls(**row)
            proxy.close()

    def count(
        self,
        workspace_id: int,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[dt.datetime] = None,
        to_: Optional[dt.datetime] = None,
        author_ids: Optional[List[int]] = None,
        is_merge: Optional[bool] = None,
        keywords: Optional[List[str]] = None,
    ) -> int:
        query = select([func.count()]).select_from(self.table)
        query = self._build_filters(query, repository_ids, from_, to_, author_ids, is_merge, keywords)
        with self._connection_with_schema(workspace_id) as connection:
            result = connection.execute(query)
            return result.fetchone()[0]

    def count_distinct_author_ids(self, workspace_id: int) -> int:
        query = select([func.count(distinct(self.table.c.aid))]).select_from(self.table)
        with self._connection_with_schema(workspace_id) as connection:
            result = connection.execute(query)
            return result.fetchone()[0]

    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        query = self.table.select().distinct(self.table.c.commit_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["commit_id"] for row in rows]

    def delete_commits(self, workspace_id: int, commit_ids: List[str]) -> int:
        if is_list_not_empty(commit_ids):
            query = self.table.delete().where(self.table.c.commit_id.in_(commit_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0

    def _build_filters(
        self,
        query,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[dt.datetime] = None,
        to_: Optional[dt.datetime] = None,
        author_ids: Optional[List[int]] = None,
        is_merge: Optional[bool] = None,
        keywords: Optional[List[str]] = None,
    ):
        if repository_ids:
            query = query.where(self.table.c.repo_id.in_(repository_ids))
        if author_ids:
            query = query.where(self.table.c.aid.in_(author_ids))
        if from_:
            query = query.where(self.table.c.date >= from_)
        if to_:
            query = query.where(self.table.c.date < to_)
        if is_merge is not None:
            query = query.where(self.table.c.is_merge == is_merge)
        if keywords:
            for keyword in keywords:
                if keyword:
                    query = query.where(self.table.c.message.ilike(f"%{keyword}%"))
        return query


class SQLCalculatedPatchRepository(
    SQLRepoDFMixin,
    CalculatedPatchRepository,
    SQLWorkspaceScopedRepository[CalculatedPatchId, CalculatedPatch, CalculatedPatch, CalculatedPatch],
):
    def identity(self, id_: CalculatedPatchId):
        return and_(
            self.table.c.commit_id == id_.commit_id,
            self.table.c.repo_id == id_.repo_id,
            self.table.c.parent_commit_id == id_.parent_commit_id,
            self.table.c.newpath == id_.newpath,
        )

    def get_all_for_commit(self, workspace_id: int, commit_id: CalculatedCommitId) -> List[CalculatedPatch]:
        query = self.table.select().where(
            and_(self.table.c.repo_id == commit_id.repo_id, self.table.c.commit_id == commit_id.commit_id)
        )
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [CalculatedPatch(**row) for row in rows]

    def get_commit_ids_all(self, workspace_id: int) -> List[str]:
        query = self.table.select().distinct(self.table.c.commit_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["commit_id"] for row in rows]

    def delete_calculated_patches(self, workspace_id: int, commit_ids: List[str]) -> int:
        if is_list_not_empty(commit_ids):
            query = self.table.delete().where(self.table.c.commit_id.in_(commit_ids))
            return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
        return 0


class SQLPullRequestRepository(
    SQLRepoDFMixin,
    PullRequestRepository,
    SQLWorkspaceScopedRepository[PullRequestId, PullRequest, PullRequest, PullRequest],
):
    def identity(self, id_: PullRequestId):
        return and_(
            self.table.c.repo_id == id_.repo_id,
            self.table.c.number == id_.number,
        )

    def get_prs_updated_at(self, workspace_id: int, repository_id: int) -> Dict[int, dt.datetime]:
        def _add_utc_timezone(d: dt.datetime):
            if d:
                return d.replace(tzinfo=dt.timezone.utc)
            else:
                return dt.datetime.min.replace(tzinfo=dt.timezone.utc)

        query = select([self.table.c.number, self.table.c.updated_at]).where(self.table.c.repo_id == repository_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return {row["number"]: _add_utc_timezone(row["updated_at"]) for row in rows}

    def select(
        self,
        workspace_id: int,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[dt.datetime] = None,
        to_: Optional[dt.datetime] = None,
        developer_ids: Optional[List[int]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Iterable[PullRequest]:
        query = self.table.select().order_by(self.table.c.created_at.desc()).limit(limit).offset(offset)
        query = self._build_filters(query, repository_ids, from_, to_, developer_ids)
        with self._connection_with_schema(workspace_id) as connection:
            proxy = connection.execution_options(stream_results=True).execute(query)
            while True:
                batch = proxy.fetchmany(10000)
                if not batch:
                    break
                for row in batch:
                    yield self.in_db_cls(**row)
            proxy.close()

    def select_pull_requests(
        self,
        workspace_id: int,
        date_from: Optional[dt.datetime] = None,
        date_to: Optional[dt.datetime] = None,
        repo_ids: Optional[List[int]] = None,
    ) -> List[PullRequest]:
        or_clause = []
        if date_from:
            or_clause.append(self.table.c.created_at >= date_from)
        if date_to:
            or_clause.append(self.table.c.created_at <= date_to)
        if is_list_not_empty(repo_ids):
            or_clause.append(self.table.c.repo_id.in_(repo_ids))
        query = self.table.select().where(or_(*or_clause)) if is_list_not_empty(or_clause) else self.table.select()
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [PullRequest(**row) for row in rows]

    def delete_pull_requests(self, workspace_id: int, pr_ids: Optional[List[PullRequestId]] = None) -> int:
        if is_list_not_empty(pr_ids):
            final_result = 0
            for pr_id in pr_ids:
                query = self.table.delete().where(
                    and_(self.table.c.number == pr_id.number, self.table.c.repo_id == pr_id.repo_id)
                )
                final_result += self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
            return final_result
        return 0

    def count(
        self,
        workspace_id: int,
        repository_ids: Optional[List[int]] = None,
        from_: Optional[dt.datetime] = None,
        to_: Optional[dt.datetime] = None,
        developer_ids: Optional[List[int]] = None,
    ) -> int:
        query = select([func.count()]).select_from(self.table)
        query = self._build_filters(query, repository_ids, from_, to_, developer_ids)

        with self._connection_with_schema(workspace_id) as connection:
            result = connection.execute(query)
            return result.fetchone()[0]

    def _build_filters(
        self,
        query,
        repository_ids: Optional[List[int]],
        from_: Optional[dt.datetime],
        to_: Optional[dt.datetime],
        developer_ids: Optional[List[int]],
    ):
        if repository_ids:
            query = query.where(self.table.c.repo_id.in_(repository_ids))
        if developer_ids:
            query = query.where(self.table.c.user_aid.in_(developer_ids))
        if from_:
            query = query.where(self.table.c.created_at >= from_)
        if to_:
            query = query.where(self.table.c.created_at < to_)
        return query


class SQLPullRequestCommitRepository(
    SQLRepoDFMixin,
    PullRequestCommitRepository,
    SQLWorkspaceScopedRepository[PullRequestCommitId, PullRequestCommit, PullRequestCommit, PullRequestCommit],
):
    def identity(self, id_: PullRequestCommitId):
        return and_(
            self.table.c.repo_id == id_.repo_id,
            self.table.c.pr_number == id_.pr_number,
            self.table.c.commit_id == id_.commit_id,
        )

    def delete_pull_request_commits(self, workspace_id: int, pr_ids: Optional[List[PullRequestId]] = None) -> int:
        if is_list_not_empty(pr_ids):
            final_result = 0
            for pr_id in pr_ids:
                query = self.table.delete().where(
                    and_(self.table.c.pr_number == pr_id.number, self.table.c.repo_id == pr_id.repo_id)
                )
                final_result += self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
            return final_result
        return 0


class SQLPullRequestCommentRepository(
    SQLRepoDFMixin,
    PullRequestCommentRepository,
    SQLWorkspaceScopedRepository[PullRequestCommentId, PullRequestComment, PullRequestComment, PullRequestComment],
):
    def identity(self, id_: PullRequestCommentId):
        return and_(
            self.table.c.repo_id == id_.repo_id,
            self.table.c.pr_number == id_.pr_number,
            self.table.c.comment_type == id_.comment_type,
            self.table.c.comment_id == id_.comment_id,
        )

    def delete_pull_request_comment(self, workspace_id: int, pr_ids: Optional[List[PullRequestId]] = None) -> int:
        if is_list_not_empty(pr_ids):
            final_result = 0
            for pr_id in pr_ids:
                query = self.table.delete().where(
                    and_(self.table.c.pr_number == pr_id.number, self.table.c.repo_id == pr_id.repo_id)
                )
                final_result += self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
            return final_result
        return 0


class SQLPullRequestLabelRepository(
    SQLRepoDFMixin,
    PullRequestLabelRepository,
    SQLWorkspaceScopedRepository[
        PullRequestLabelId,
        PullRequestLabel,
        PullRequestLabel,
        PullRequestLabel,
    ],
):
    def identity(self, id_: PullRequestLabelId):
        return and_(
            self.table.c.repo_id == id_.repo_id,
            self.table.c.pr_number == id_.pr_number,
            self.table.c.name == id_.name,
        )

    def delete_pull_request_labels(self, workspace_id: int, pr_ids: Optional[List[PullRequestId]] = None) -> int:
        if is_list_not_empty(pr_ids):
            final_result = 0
            for pr_id in pr_ids:
                query = self.table.delete().where(
                    and_(self.table.c.pr_number == pr_id.number, self.table.c.repo_id == pr_id.repo_id)
                )
                final_result += self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)
            return final_result
        return 0


class SQLEmailLogRepository(EmailLogRepository, SQLRepository[int, EmailLogCreate, EmailLogUpdate, EmailLogInDB]):
    def get_emails_to_send(self) -> List[EmailLogInDB]:
        query = self.table.select().where(
            and_(self.table.c.status == "scheduled", self.table.c.scheduled_at <= dt.datetime.utcnow())
        )
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [EmailLogInDB(**row) for row in rows]

    def email_log_status_update(self, user_id: int, template_name: str, status: str) -> Optional[EmailLogInDB]:
        # query = self.table.update().where(self.table.c.id == id).values(status=status)
        query = (
            self.table.update()
            .where(
                and_(
                    self.table.c.user_id == user_id,
                    self.table.c.template_name.like("%" + template_name + "%"),
                    self.table.c.status != "canceled",
                )
            )
            .values(status="sent")
        )
        self._execute_query(query, callback_fn=fetchall_)
        return self.get_or_error(user_id)

    def cancel_email(self, user_id: int, template: str) -> Optional[EmailLogInDB]:
        query = (
            self.table.update()
            .where(
                and_(
                    self.table.c.user_id == user_id,
                    self.table.c.template_name == template,
                    self.table.c.status == "scheduled",
                )
            )
            .values(status="canceled")
        )
        self._execute_query(query)
        # return [EmailLogInDB(**row) for row in rows]
        return self.get_or_error(user_id)

    def delete_for_user(self, user_id: int) -> int:
        query = self.table.delete().where(self.table.c.user_id == user_id)
        return self._execute_query(query, callback_fn=rowcount_)
