from enum import Enum
from typing import List, Optional, Tuple, Dict, Any

from pydantic import Field

from gitential2.datatypes import CoreModel
from gitential2.datatypes.common import ExtraFieldMixin, DateTimeModelMixin, IDModelMixin
from gitential2.datatypes.stats import MetricName, DimensionName, FilterName
from .export import ExportableModel


class ChartVisualizationTypes(str, Enum):
    chart_line_chart_bar = "chart-line_chart-bar"
    chart_bubble = "chart-bubble"
    chart_pie = "chart-pie"
    chart_stacked_bar = "chart-stacked_bar"
    table = "table"


class ChartLayout(CoreModel):
    x: int
    y: int
    w: int
    h: int


class ChartBase(ExtraFieldMixin, CoreModel):
    is_custom: Optional[bool] = True
    title: Optional[str]
    layout: ChartLayout
    chart_type: ChartVisualizationTypes
    metrics: List[MetricName]
    dimensions: List[DimensionName]
    filters: Optional[Dict[FilterName, Any]]


class ChartPublic(IDModelMixin, DateTimeModelMixin, ChartBase):
    pass


class ChartCreate(ChartBase):
    title: str = Field(..., min_length=2, max_length=128)


class ChartUpdate(ChartBase):
    pass


class ChartInDB(IDModelMixin, DateTimeModelMixin, ChartBase, ExportableModel):
    def export_names(self) -> Tuple[str, str]:
        return "chart", "charts"

    def export_fields(self) -> List[str]:
        return [
            "id",
            "created_at",
            "updated_at",
            "extra",
            "is_custom",
            "title",
            "chart_type",
            "layout",
            "metrics",
            "dimensions",
            "filters",
        ]
