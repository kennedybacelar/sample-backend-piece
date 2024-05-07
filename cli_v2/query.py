from typing import Any, List, Optional
from datetime import datetime
import json
import typer
from fastapi.encoders import jsonable_encoder
from structlog import get_logger
from gitential2.datatypes.stats import MetricName, FilterName, DimensionName, QueryType, Query
from gitential2.core.stats_v2 import collect_stats_v2_raw
from .common import get_context, print_results, OutputFormat

app = typer.Typer()
logger = get_logger(__name__)

FILTERS_WITH_STRING_TYPE = [FilterName.name]


def _prepare_filter_param(name: FilterName, param_str: str) -> Any:
    if name in FILTERS_WITH_STRING_TYPE:
        return param_str
    else:
        return json.loads(param_str)


# pylint: disable=too-many-arguments
@app.command("execute")
def execute_(
    workspace_id: int,
    metrics: List[MetricName] = typer.Option(..., "--metric"),
    dimensions: Optional[List[DimensionName]] = typer.Option(None, "--dimension"),
    filter_names: Optional[List[FilterName]] = typer.Option(None, "--filter-name"),
    filter_params: Optional[List[str]] = typer.Option(None, "--filter-param"),
    from_: Optional[datetime] = typer.Option(None, "--from"),
    to_: Optional[datetime] = typer.Option(None, "--to"),
    query_type: QueryType = typer.Option(QueryType.aggregate, "--type"),
    sort_by: Optional[str] = typer.Option(None, "--sort-by"),
    format_: OutputFormat = typer.Option(OutputFormat.tabulate, "--format"),
    fields: Optional[str] = None,
):
    query = _construct_query(metrics, dimensions, filter_names, filter_params, from_, to_, query_type, sort_by)

    g = get_context()
    results = collect_stats_v2_raw(g, workspace_id, query)
    print_results(results.values, format_=format_, fields=fields)


# pylint: disable=too-many-arguments
@app.command("construct")
def construct_(
    metrics: List[MetricName] = typer.Option(..., "--metric"),
    dimensions: Optional[List[DimensionName]] = typer.Option(None, "--dimension"),
    filter_names: Optional[List[FilterName]] = typer.Option(None, "--filter-name"),
    filter_params: Optional[List[str]] = typer.Option(None, "--filter-param"),
    from_: Optional[datetime] = typer.Option(None, "--from"),
    to_: Optional[datetime] = typer.Option(None, "--to"),
    query_type: QueryType = typer.Option(QueryType.aggregate, "--type"),
    sort_by: Optional[str] = typer.Option(None, "--sort-by"),
):
    query = _construct_query(metrics, dimensions, filter_names, filter_params, from_, to_, query_type, sort_by)
    print(json.dumps(jsonable_encoder(query), indent=2))


def _construct_query(
    metrics: List[MetricName] = typer.Option(..., "--metric"),
    dimensions: Optional[List[DimensionName]] = typer.Option(None, "--dimension"),
    filter_names: Optional[List[FilterName]] = typer.Option(None, "--filter-name"),
    filter_params: Optional[List[str]] = typer.Option(None, "--filter-param"),
    from_: Optional[datetime] = typer.Option(None, "--from"),
    to_: Optional[datetime] = typer.Option(None, "--to"),
    query_type: QueryType = QueryType.aggregate,
    sort_by: Optional[str] = typer.Option(None, "--sort-by"),
):
    if len(filter_names or []) != len(filter_params or []):
        raise typer.Exit(-2)
    filters_ = (
        {name: _prepare_filter_param(name, param) for name, param in zip(filter_names, filter_params)}
        if filter_names and filter_params
        else {}
    )
    if from_ and FilterName.day not in filters_:
        filters_[FilterName.day] = [
            from_.strftime("%Y-%m-%d"),
            to_.strftime("%Y-%m-%d") if to_ else datetime.utcnow().strftime("%Y-%m-%d"),
        ]

    query = Query(
        dimensions=dimensions,
        metrics=metrics,
        filters=filters_,
        type=query_type,
        sort_by=json.loads(sort_by) if sort_by else None,
    )
    logger.debug("Constructed query", query=query)
    return query
