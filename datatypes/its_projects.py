from typing import Optional, List, Tuple
from gitential2.datatypes.export import ExportableModel

from .common import IDModelMixin, DateTimeModelMixin, CoreModel, ExtraFieldMixin


class ITSProjectBase(ExtraFieldMixin, CoreModel):
    name: str
    namespace: str = ""
    private: bool = False
    api_url: str = ""
    key: Optional[str] = None
    integration_type: str
    integration_name: str
    integration_id: str
    credential_id: Optional[int] = None


class ITSProjectCreate(ITSProjectBase):
    pass


class ITSProjectUpdate(ITSProjectBase):
    pass


class ITSProjectInDB(IDModelMixin, DateTimeModelMixin, ITSProjectBase, ExportableModel):
    def export_fields(self) -> List[str]:
        return [
            "id",
            "created_at",
            "updated_at",
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
        ]

    def export_names(self) -> Tuple[str, str]:
        return "its_project", "its_projects"
