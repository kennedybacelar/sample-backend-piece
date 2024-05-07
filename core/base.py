# from datetime import datetime
# from typing import List, Optional, cast, Tuple

# from gitential2.datatypes.subscriptions import SubscriptionType
# from gitential2.license import License, check_license
# from gitential2.settings import GitentialSettings
# from gitential2.integrations import init_integrations
# from gitential2.backends import init_backend, GitentialBackend
# from gitential2.kvstore import init_key_value_store, KeyValueStore
# from gitential2.secrets import Fernet
# from gitential2.datatypes import (
#     UserCreate,
#     UserInDB,
#     SubscriptionInDB,
#     SubscriptionCreate,
#     UserInfoUpdate,
#     UserInfoCreate,
#     UserUpdate,
#     UserHeader,
#     CredentialCreate,
#     CredentialUpdate,
#     WorkspaceCreate,
#     WorkspaceInDB,
#     WorkspaceRole,
#     WorkspacePublic,
#     WorkspaceMemberCreate,
# )
# from gitential2.datatypes.permissions import Entity, Action

# from gitential2.datatypes.workspaces import WorkspaceUpdate
# from gitential2.datatypes.workspacemember import WorkspaceMemberInDB, WorkspaceMemberPublic, MemberInvite

# from .abc import WorkspaceCtrl, Gitential
# from .workspace_ctrl import WorkspaceCtrlImpl


# class GitentialImpl(Gitential):
#     def __init__(
#         self,
#         settings: GitentialSettings,
#         integrations: dict,
#         backend: GitentialBackend,
#         kvstore: KeyValueStore,
#         fernet: Fernet,
#         license_: License,
#     ):
#         self._settings = settings
#         self._integrations = integrations
#         self._backend = backend
#         self._fernet = fernet
#         self._kvstore = kvstore
#         self._license = license_

#     @property
#     def settings(self) -> GitentialSettings:
#         return self._settings

#     @property
#     def backend(self) -> GitentialBackend:
#         return self._backend

#     @property
#     def integrations(self) -> dict:
#         return self._integrations

#     @property
#     def fernet(self) -> Fernet:
#         return self._fernet

#     @property
#     def kvstore(self) -> KeyValueStore:
#         return self._kvstore

#     @property
#     def license(self) -> License:
#         return self._license

#     @classmethod
#     def from_config(cls, settings: GitentialSettings):
#         integrations = init_integrations(settings)
#         backend: GitentialBackend = init_backend(settings)
#         fernet = Fernet(settings)
#         kvstore = init_key_value_store(settings)
#         license_ = check_license()
#         return cls(
#             settings=settings,
#             integrations=integrations,
#             backend=backend,
#             kvstore=kvstore,
#             fernet=fernet,
#             license_=license_,
#         )

#     def get_user(self, user_id: int) -> Optional[UserInDB]:
#         return self.backend.users.get(user_id)

#     def get_current_subscription(self, user_id: int) -> SubscriptionInDB:
#         return SubscriptionInDB(
#             id=0,
#             user_id=user_id,
#             subscription_type=SubscriptionType.trial,
#             subscription_start=datetime.utcnow(),
#         )


#     def _create_default_subscription(self, user) -> SubscriptionInDB:
#         return self.backend.subscriptions.create(SubscriptionCreate.default_for_new_user(user.id))

#     def handle_authorize(self, integration_name: str, token, user_info: dict, current_user: Optional[UserInDB] = None):
#         integration = self.integrations[integration_name]

#         # normalize the userinfo
#         normalized_userinfo: UserInfoCreate = integration.normalize_userinfo(user_info, token=token)

#         # update or create a user and the proper user_info in backend
#         user, user_info, is_new_user = self._create_or_update_user_and_user_info(normalized_userinfo, current_user)

#         # update or create credentials based on integration and user
#         self._create_or_update_credential_from(user, integration_name, integration.integration_type, token)

#         # Create workspace if missing
#         self._create_primary_workspace_if_missing(user)
#         return {"ok": True, "user": user, "user_info": user_info, "is_new_user": is_new_user}

#     def _create_or_update_user_and_user_info(
#         self, normalized_userinfo: UserInfoCreate, current_user: Optional[UserInDB] = None
#     ):
#         existing_userinfo = self.backend.user_infos.get_by_sub_and_integration(
#             sub=normalized_userinfo.sub, integration_name=normalized_userinfo.integration_name
#         )
#         if existing_userinfo:
#             if current_user and existing_userinfo.user_id != current_user.id:
#                 raise ValueError("Authentication error...")

#             user = self.backend.users.get(existing_userinfo.user_id)
#             user_info = self.backend.user_infos.update(existing_userinfo.id, cast(UserInfoUpdate, normalized_userinfo))
#             return user, user_info, False
#         else:
#             existing_user = current_user or (
#                 self.backend.users.get_by_email(normalized_userinfo.email) if normalized_userinfo.email else None
#             )
#             if existing_user:
#                 user_update = existing_user.copy()
#                 user_update.login_ready = True
#                 user = self.backend.users.update(existing_user.id, cast(UserUpdate, user_update))
#                 is_new_user = False
#             else:
#                 new_user = UserCreate.from_user_info(normalized_userinfo)
#                 new_user.login_ready = True
#                 user = self.backend.users.create(new_user)
#                 is_new_user = True
#             user_info_data = normalized_userinfo.dict(exclude_none=True)
#             user_info_data.setdefault("user_id", user.id)
#             user_info = self.backend.user_infos.create(normalized_userinfo.copy(update={"user_id": user.id}))
#             return user, user_info, is_new_user

#     def _create_or_update_credential_from(
#         self, user: UserInDB, integration_name: str, integration_type: str, token: dict
#     ):
#         new_credential = CredentialCreate.from_token(
#             token=token,
#             fernet=self.fernet,
#             owner_id=user.id,
#             integration_name=integration_name,
#             integration_type=integration_type,
#         )

#         existing_credential = self.backend.credentials.get_by_user_and_integration(
#             owner_id=user.id, integration_name=integration_name
#         )
#         if existing_credential:
#             self.backend.credentials.update(id_=existing_credential.id, obj=CredentialUpdate(**new_credential.dict()))
#         else:
#             self.backend.credentials.create(new_credential)

#     def _create_primary_workspace_if_missing(self, user: UserInDB):
#         existing_workspace_memberships = self.backend.workspace_members.get_for_user(user_id=user.id)
#         has_primary = any(ewm.role == WorkspaceRole.owner for ewm in existing_workspace_memberships)
#         if not has_primary:
#             workspace = WorkspaceCreate(name=f"{user.login}'s workspace")
#             self.create_workspace(workspace, current_user=user, primary=True)

#     def create_workspace(self, workspace: WorkspaceCreate, current_user: UserInDB, primary=False) -> WorkspaceInDB:
#         workspace.created_by = current_user.id

#         workspace_in_db = self.backend.workspaces.create(workspace)
#         self.backend.workspace_members.create(
#             WorkspaceMemberCreate(
#                 workspace_id=workspace_in_db.id, user_id=current_user.id, role=WorkspaceRole.owner, primary=primary
#             )
#         )
#         self.get_workspace_ctrl(workspace_id=workspace_in_db.id).initialize()
#         return workspace_in_db

#     def update_workspace(self, workspace_id: int, workspace: WorkspaceUpdate, current_user: UserInDB) -> WorkspaceInDB:
#         membership = self.backend.workspace_members.get_for_workspace_and_user(
#             workspace_id=workspace_id, user_id=current_user.id
#         )
#         if membership:
#             return self.backend.workspaces.update(workspace_id, workspace)
#         else:
#             raise Exception("Authentication error")

#     def delete_workspace(self, workspace_id: int, current_user: UserInDB) -> int:
#         membership = self.backend.workspace_members.get_for_workspace_and_user(
#             workspace_id=workspace_id, user_id=current_user.id
#         )
#         if membership:
#             return self.backend.workspaces.delete(workspace_id)
#         else:
#             raise Exception("Authentication error")

#     def get_accessible_workspaces(
#         self, current_user: UserInDB, include_members: bool = False, include_projects: bool = False
#     ) -> List[WorkspacePublic]:
#         workspace_memberships = self.backend.workspace_members.get_for_user(user_id=current_user.id)
#         return [
#             self.get_workspace(
#                 workspace_id=membership.workspace_id,
#                 current_user=current_user,
#                 include_members=include_members,
#                 include_projects=include_projects,
#                 _membership=membership,
#             )
#             for membership in workspace_memberships
#         ]

#     def get_workspace(
#         self,
#         workspace_id: int,
#         current_user: UserInDB,
#         include_members: bool = False,
#         include_projects: bool = False,
#         _membership: Optional[WorkspaceMemberInDB] = None,
#     ) -> WorkspacePublic:

#         membership = _membership or self.backend.workspace_members.get_for_workspace_and_user(
#             workspace_id=workspace_id, user_id=current_user.id
#         )

#         if membership:
#             workspace = self.backend.workspaces.get_or_error(workspace_id)
#             workspace_data = workspace.dict()
#             workspace_data["membership"] = membership.dict()

#             if include_members:
#                 workspace_data["members"] = self.get_members(workspace_id=workspace.id)

#             if include_projects:
#                 workspace_data["projects"] = self.get_workspace_ctrl(workspace_id=workspace_id).list_projects()

#             return WorkspacePublic(**workspace_data)
#         else:
#             raise Exception("Access Denied")

#     def get_members(self, workspace_id: int, include_user_header=True) -> List[WorkspaceMemberPublic]:
#         def _process(member):
#             member_data = member.dict()
#             if include_user_header:
#                 user = self.backend.users.get_or_error(member.user_id)
#                 member_data["user"] = UserHeader(id=user.id, login=user.login)
#             return WorkspaceMemberPublic(**member_data)

#         return [
#             _process(member) for member in self.backend.workspace_members.get_for_workspace(workspace_id=workspace_id)
#         ]

#     def invite_members(self, workspace_id: int, invitations: List[MemberInvite]) -> int:
#         for invitation in invitations:
#             existing_user = self.backend.users.get_by_email(invitation.email)
#             if existing_user:
#                 user_id = existing_user.id
#             else:
#                 new_user = self.backend.users.create(invitation.user_create())
#                 user_id = new_user.id
#             self.backend.workspace_members.create(
#                 WorkspaceMemberCreate(
#                     workspace_id=workspace_id,
#                     user_id=user_id,
#                     role=WorkspaceRole.collaborator,
#                     primary=False,
#                 )
#             )
#         return len(invitations)

#     def remove_member(self, workspace_id: int, workspace_member_id: int) -> int:
#         workspace_member = self.backend.workspace_members.get(workspace_member_id)
#         if workspace_member and workspace_member.workspace_id == workspace_id:
#             self.backend.workspace_members.delete(workspace_member_id)
#         return 1

#     def get_workspace_ctrl(self, workspace_id: int) -> WorkspaceCtrl:
#         return WorkspaceCtrlImpl(
#             id_=workspace_id,
#             backend=self.backend,
#             core=self,
#         )
