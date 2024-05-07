from typing import Optional, List, Dict, Any, Tuple

from pydantic import Field

from gitential2.datatypes.stats import FilterName
from .charts import ChartPublic
from .common import IDModelMixin, CoreModel, DateTimeModelMixin, ExtraFieldMixin
from .export import ExportableModel


class DashboardBase(ExtraFieldMixin, CoreModel):
    title: Optional[str]
    charts: List[ChartPublic]
    filters: Optional[Dict[FilterName, Any]]


class DashboardPublic(IDModelMixin, DateTimeModelMixin, DashboardBase):
    pass


class DashboardCreate(DashboardBase):
    title: str = Field(..., min_length=2, max_length=128)


class DashboardUpdate(DashboardCreate):
    pass


class DashboardInDB(IDModelMixin, DateTimeModelMixin, DashboardBase, ExportableModel):
    def export_names(self) -> Tuple[str, str]:
        return "dashboard", "dashboards"

    def export_fields(self) -> List[str]:
        return [
            "id",
            "title",
            "created_at",
            "updated_at",
            "filters",
            "charts",
            "extra",
        ]
