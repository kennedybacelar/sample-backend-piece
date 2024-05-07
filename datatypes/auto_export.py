from typing import List, Tuple
from gitential2.datatypes.export import ExportableModel
from .common import CoreModel, IDModelMixin, DateTimeModelMixin, ExtraFieldMixin


class AutoExportBase(ExtraFieldMixin, CoreModel):
    """
    Base data class for auto export model
    """

    workspace_id: int
    emails: List[str]


class AutoExportCreate(AutoExportBase):
    pass


class AutoExportUpdate(AutoExportBase):
    pass


class AutoExportInDB(IDModelMixin, DateTimeModelMixin, AutoExportBase, ExportableModel):
    def export_fields(self) -> List[str]:
        return ["id", "workspace_id", "emails", "created_at", "updated_at", "extra"]

    def export_names(self) -> Tuple[str, str]:
        return "auto_export", "auto_exports"
