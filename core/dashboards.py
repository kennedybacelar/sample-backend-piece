from typing import List, Dict

from structlog import get_logger

from gitential2.core import GitentialContext
from gitential2.datatypes.charts import (
    ChartInDB,
    ChartCreate,
    ChartUpdate,
    ChartPublic,
    ChartLayout,
    ChartVisualizationTypes,
)
from gitential2.datatypes.dashboards import (
    DashboardInDB,
    DashboardUpdate,
    DashboardCreate,
)
from gitential2.datatypes.stats import MetricName, DimensionName
from gitential2.exceptions import SettingsException

logger = get_logger(__name__)

INTERNAL_CHARTS: Dict[int, ChartInDB] = {
    -1: ChartInDB(
        id=-1,
        is_custom=False,
        title="Avg PR Cycle Time",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_line_chart_bar,
        metrics=[MetricName.avg_pr_cycle_time],
        dimensions=[DimensionName.week],
    ),
    -2: ChartInDB(
        id=-2,
        is_custom=False,
        title="Code complexity",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_line_chart_bar,
        metrics=[MetricName.comp_sum, MetricName.loc_sum],
        dimensions=[DimensionName.week],
    ),
    -3: ChartInDB(
        id=-3,
        is_custom=False,
        title="Code Volume",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_line_chart_bar,
        metrics=[MetricName.sum_loc_effort],
        dimensions=[DimensionName.week],
    ),
    -4: ChartInDB(
        id=-4,
        is_custom=False,
        title="Coding Hours",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_line_chart_bar,
        metrics=[MetricName.sum_hours],
        dimensions=[DimensionName.week],
    ),
    -5: ChartInDB(
        id=-5,
        is_custom=False,
        title="Developer Productivity",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_bubble,
        metrics=[MetricName.avg_loc_effort, MetricName.avg_hours, MetricName.count_commits],
        dimensions=[DimensionName.name],
    ),
    -6: ChartInDB(
        id=-6,
        is_custom=False,
        title="Efficiency",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_line_chart_bar,
        metrics=[MetricName.efficiency],
        dimensions=[DimensionName.week],
    ),
    -7: ChartInDB(
        id=-7,
        is_custom=False,
        title="No of Commits",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_line_chart_bar,
        metrics=[MetricName.count_commits],
        dimensions=[DimensionName.week],
    ),
    -8: ChartInDB(
        id=-8,
        is_custom=False,
        title="No of Developers",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_line_chart_bar,
        metrics=[MetricName.nunique_contributors],
        dimensions=[DimensionName.week],
    ),
    -9: ChartInDB(
        id=-9,
        is_custom=False,
        title="No of PRs Created",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_line_chart_bar,
        metrics=[MetricName.sum_pr_count],
        dimensions=[DimensionName.week],
    ),
    -10: ChartInDB(
        id=-10,
        is_custom=False,
        title="PR Cycle Time by Activity",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_pie,
        metrics=[MetricName.avg_review_time, MetricName.avg_pickup_time, MetricName.avg_development_time],
        dimensions=[DimensionName.week],
    ),
    -11: ChartInDB(
        id=-11,
        is_custom=False,
        title="PR Merge Ratio",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_line_chart_bar,
        metrics=[MetricName.pr_merge_ratio],
        dimensions=[DimensionName.week],
    ),
    -12: ChartInDB(
        id=-12,
        is_custom=False,
        title="Productive vs. unproductive work",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_stacked_bar,
        metrics=[MetricName.sum_ploc, MetricName.sum_uploc],
        dimensions=[DimensionName.week],
    ),
    -13: ChartInDB(
        id=-13,
        is_custom=False,
        title="Progress Overview",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_pie,
        metrics=[MetricName.sum_loc_effort, MetricName.sum_hours, MetricName.nunique_contributors],
        dimensions=[DimensionName.week],
    ),
    -14: ChartInDB(
        id=-14,
        is_custom=False,
        title="Pull Request Cycle Time",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_stacked_bar,
        metrics=[MetricName.avg_review_time, MetricName.avg_pickup_time, MetricName.avg_development_time],
        dimensions=[DimensionName.week],
    ),
    -15: ChartInDB(
        id=-15,
        is_custom=False,
        title="Pull Request Reviews",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_line_chart_bar,
        metrics=[MetricName.sum_review_comment_count, MetricName.avg_pr_review_comment_count],
        dimensions=[DimensionName.week],
    ),
    -16: ChartInDB(
        id=-16,
        is_custom=False,
        title="Pull Requests",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_stacked_bar,
        metrics=[MetricName.sum_pr_closed, MetricName.sum_pr_merged, MetricName.sum_pr_open],
        dimensions=[DimensionName.week],
    ),
    -17: ChartInDB(
        id=-17,
        is_custom=False,
        title="Velocity",
        layout=ChartLayout(x=-1, y=-1, w=-1, h=-1),
        chart_type=ChartVisualizationTypes.chart_line_chart_bar,
        metrics=[MetricName.avg_velocity],
        dimensions=[DimensionName.week],
    ),
}


def list_dashboards(g: GitentialContext, workspace_id: int) -> List[DashboardInDB]:
    return list(g.backend.dashboards.all(workspace_id=workspace_id))


def get_dashboard(g: GitentialContext, workspace_id: int, dashboard_id: int) -> DashboardInDB:
    return g.backend.dashboards.get_or_error(workspace_id=workspace_id, id_=dashboard_id)


def get_chart_public_from_chart_in_db(chart_in_db: ChartInDB) -> ChartPublic:
    return ChartPublic(
        id=chart_in_db.id,
        created_at=chart_in_db.created_at,
        updated_at=chart_in_db.updated_at,
        extra=chart_in_db.extra,
        is_custom=True,
        title=chart_in_db.title,
        layout=chart_in_db.layout,
        chart_type=chart_in_db.chart_type,
        metrics=chart_in_db.metrics,
        dimensions=chart_in_db.dimensions,
        filters=chart_in_db.filters,
    )


def create_dashboard(g: GitentialContext, workspace_id: int, dashboard_create: DashboardCreate) -> DashboardInDB:
    if not dashboard_create.charts:
        raise SettingsException("Can not create dashboard with no charts!")
    logger.info("creating dashboard", workspace_id=workspace_id, title=dashboard_create.title)
    charts = [get_chart(g, workspace_id, chart.id, chart.layout) for chart in dashboard_create.charts]
    d = DashboardCreate(
        title=dashboard_create.title,
        filters=dashboard_create.filters,
        charts=[get_chart_public_from_chart_in_db(c_in_db) for c_in_db in charts],
    )
    return g.backend.dashboards.create(workspace_id, d)


def update_dashboard(
    g: GitentialContext, workspace_id: int, dashboard_id: int, dashboard_update: DashboardUpdate
) -> DashboardInDB:
    if not dashboard_update.charts:
        raise SettingsException("Can not update dashboard with no charts!")
    charts = [get_chart(g, workspace_id, chart.id, chart.layout) for chart in dashboard_update.charts]
    d = DashboardUpdate(
        title=dashboard_update.title,
        filters=dashboard_update.filters,
        charts=[get_chart_public_from_chart_in_db(c_in_db) for c_in_db in charts],
    )
    return g.backend.dashboards.update(workspace_id, dashboard_id, d)


def delete_dashboard(g: GitentialContext, workspace_id: int, dashboard_id: int) -> bool:
    g.backend.dashboards.delete(workspace_id=workspace_id, id_=dashboard_id)
    return True


# Chart


def list_charts(g: GitentialContext, workspace_id: int) -> List[ChartInDB]:
    return list(g.backend.charts.all(workspace_id=workspace_id)) + list(INTERNAL_CHARTS.values())


def get_chart(
    g: GitentialContext,
    workspace_id: int,
    chart_id: int,
    chart_layout: ChartLayout = ChartLayout(x=-1, y=-1, h=-1, w=-1),
) -> ChartInDB:
    if chart_id in INTERNAL_CHARTS:
        chart = INTERNAL_CHARTS.get(chart_id)
        if not chart:
            raise SettingsException(f"Can not get custom chart with id: {chart_id}!")
    else:
        chart = g.backend.charts.get_or_error(workspace_id=workspace_id, id_=chart_id)
    chart.layout = chart_layout
    return chart


def create_chart(g: GitentialContext, workspace_id: int, chart_create: ChartCreate) -> ChartInDB:
    logger.info("creating chart", workspace_id=workspace_id, title=chart_create.title)
    chart_create.is_custom = True
    return g.backend.charts.create(workspace_id, chart_create)


def update_chart(g: GitentialContext, workspace_id: int, chart_id: int, chart_update: ChartUpdate) -> ChartInDB:
    if chart_id < 0:
        raise SettingsException("Can not update not custom chart!")
    logger.info("updating chart", workspace_id=workspace_id, title=chart_update.title)
    chart_update.is_custom = True
    chart_updated = g.backend.charts.update(workspace_id, chart_id, chart_update)
    for d in list(g.backend.dashboards.all(workspace_id=workspace_id)):
        dashboard_chart_ids = [c.id for c in d.charts]
        is_dashboard_need_to_be_updated = any(cid == chart_id for cid in dashboard_chart_ids)
        if is_dashboard_need_to_be_updated:
            dashboard_charts = [
                ChartPublic(
                    id=chart_id,
                    created_at=chart.created_at,
                    updated_at=chart.updated_at,
                    is_custom=chart.is_custom,
                    extra=chart.extra,
                    title=chart_update.title,
                    layout=chart.layout,
                    chart_type=chart_update.chart_type,
                    metrics=chart_update.metrics,
                    dimensions=chart_update.dimensions,
                    filters=chart_update.filters,
                )
                if chart.id == chart_id
                else chart
                for chart in d.charts
            ]
            dashboard_update = DashboardUpdate(title=d.title, filters=d.filters, charts=dashboard_charts)
            update_dashboard(g, workspace_id=workspace_id, dashboard_id=d.id, dashboard_update=dashboard_update)
    return chart_updated


def delete_chart(g: GitentialContext, workspace_id: int, chart_id: int) -> bool:
    if chart_id < 0:
        raise SettingsException("Can not delete not custom chart!")
    dashboards = list(g.backend.dashboards.all(workspace_id=workspace_id))
    is_chart_exists_in_dashboards = all(all(c.id != chart_id for c in d.charts) for d in dashboards)
    if not dashboards or is_chart_exists_in_dashboards:
        delete_result = g.backend.charts.delete(workspace_id=workspace_id, id_=chart_id)
        if not delete_result:
            logger.info(f"Chart delete failed! Not able to find chart with id: {chart_id}")
        return bool(delete_result)
    logger.info(f"Chart delete failed! Chart is already in one of the dashboards! chart_id={chart_id}")
    return False
