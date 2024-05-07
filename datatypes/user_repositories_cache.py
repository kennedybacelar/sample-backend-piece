from typing import List, Tuple, Optional

from gitential2.datatypes.export import ExportableModel
from gitential2.datatypes.repositories import GitProtocol
from .common import DateTimeModelMixin, ExtraFieldMixin, CoreModel


class UserRepositoryPublic(CoreModel):
    clone_url: str
    # repo_provider_id is optional because of ssh repositories
    # but there can not be any ssh repo in the user_repositories_cache database table
    repo_provider_id: Optional[str] = None
    protocol: GitProtocol
    name: str = ""
    namespace: str = ""
    private: bool = False
    integration_type: Optional[str] = None
    integration_name: Optional[str] = None
    credential_id: Optional[int] = None


class UserRepositoryGroup(CoreModel):
    integration_type: Optional[str] = None
    namespace: Optional[str] = None
    credential_id: Optional[int] = None
    total_count: int = 0


class UserRepositoryCacheId(CoreModel):
    user_id: int
    repo_provider_id: str
    integration_type: str


class UserRepositoryCacheBase(ExtraFieldMixin, CoreModel):
    user_id: int
    repo_provider_id: str
    clone_url: str
    protocol: GitProtocol
    name: str = ""
    namespace: str = ""
    private: bool = False
    integration_type: Optional[str] = None
    integration_name: Optional[str] = None
    credential_id: Optional[int] = None

    @property
    def id_(self):
        return UserRepositoryCacheId(
            user_id=self.user_id, repo_provider_id=self.repo_provider_id, integration_type=self.integration_type
        )


class UserRepositoryCacheCreate(UserRepositoryCacheBase):
    pass


class UserRepositoryCacheUpdate(UserRepositoryCacheBase):
    pass


class UserRepositoryCacheInDB(DateTimeModelMixin, UserRepositoryCacheBase, ExportableModel):
    def export_fields(self) -> List[str]:
        return [
            "user_id",
            "repo_provider_id",
            "clone_url",
            "protocol",
            "name",
            "namespace",
            "private",
            "integration_type",
            "integration_name",
            "credential_id",
            "created_at",
            "updated_at",
            "extra",
        ]

    def export_names(self) -> Tuple[str, str]:
        return "user_repositories_cache", "user_repositories_caches"
