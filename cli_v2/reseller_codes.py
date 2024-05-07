from typing import Optional
from datetime import datetime

import typer

from structlog import get_logger
from gitential2.core.reseller_codes import generate_reseller_codes
from .common import get_context, print_results, OutputFormat

app = typer.Typer()
logger = get_logger(__name__)


@app.command("list")
def list_rcodes_(
    format_: OutputFormat = typer.Option(OutputFormat.csv, "--format"),
    fields: Optional[str] = "reseller_id,id,expire_at,created_at,updated_at,user_id",
    available_only: bool = False,
):
    g = get_context()
    results = list(g.backend.reseller_codes.all())
    if available_only:
        results = [r for r in results if r.user_id is None]
    print_results(results, format_=format_, fields=fields)


@app.command("generate")
def generate_codes(
    reseller_id: str,
    count: int = 1,
    expire_at: Optional[datetime] = None,
    format_: OutputFormat = typer.Option(OutputFormat.csv, "--format"),
    fields: Optional[str] = "reseller_id,id,expire_at,created_at,updated_at,user_id",
):
    g = get_context()
    results = generate_reseller_codes(g, reseller_id, count, expire_at)
    print_results(results, format_=format_, fields=fields)


@app.command("remove")
def remove_code_(code: str):
    g = get_context()
    print(g.backend.reseller_codes.delete(code))
