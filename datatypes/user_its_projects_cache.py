from typing import Optional, List, Tuple

from gitential2.datatypes.export import ExportableModel
from .common import DateTimeModelMixin, ExtraFieldMixin, CoreModel


class UserITSProjectGroup(CoreModel):
    integration_type: str = ""
    namespace: str = ""
    credential_id: Optional[int] = None
    total_count: int = 0


class UserITSProjectCacheId(CoreModel):
    user_id: int
    integration_id: str
    integration_type: str


class UserITSProjectCacheBase(ExtraFieldMixin, CoreModel):
    user_id: int
    name: str
    namespace: str = ""
    private: bool = False
    api_url: str = ""
    key: Optional[str] = None
    integration_type: str = ""
    integration_name: str = ""
    integration_id: str = ""
    credential_id: Optional[int] = None

    @property
    def id_(self):
        return UserITSProjectCacheId(
            user_id=self.user_id, integration_id=self.integration_id, integration_type=self.integration_type
        )


class UserITSProjectCacheCreate(UserITSProjectCacheBase):
    pass


class UserITSProjectCacheUpdate(UserITSProjectCacheBase):
    pass


class UserITSProjectCacheInDB(DateTimeModelMixin, UserITSProjectCacheBase, ExportableModel):
    def export_fields(self) -> List[str]:
        return [
            "user_id",
            "name",
            "namespace",
            "private",
            "api_url",
            "key",
            "integration_type",
            "integration_name",
            "integration_id",
            "credential_id",
            "extra",
            "created_at",
            "updated_at",
        ]

    def export_names(self) -> Tuple[str, str]:
        return "user_its_project_cache", "user_its_project_caches"
