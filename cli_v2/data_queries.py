import sys
from typing import Optional
from pathlib import Path
import typer
from structlog import get_logger
from gitential2.datatypes.data_queries import DataQuery
from gitential2.core.data_queries import execute_data_query
from .common import get_context

app = typer.Typer()
logger = get_logger(__name__)


def _get_query(query_file: Optional[Path], query: Optional[str]) -> str:
    if query_file:
        return open(query_file, "r", encoding="utf-8").read()
    elif query:
        return query
    else:
        return sys.stdin.read()


@app.command("execute")
def execute_(
    workspace_id: int, query_file: Optional[Path] = typer.Option(None), query: Optional[str] = typer.Option(None)
):
    query_str = _get_query(query_file, query)
    dq = DataQuery.parse_raw(query_str)
    g = get_context()
    df, total = execute_data_query(g, workspace_id, dq)
    print(total)
    print(df)
