import typer
from structlog import get_logger

from gitential2.core.stats import calculate_workspace_usage_statistics, calculate_user_statistics
from .common import get_context, print_results, OutputFormat

app = typer.Typer()
logger = get_logger(__name__)


@app.command("workspace")
def workspace_usage_stats(workspace_id: int, format_: OutputFormat = typer.Option(OutputFormat.json, "--format")):
    g = get_context()
    result = calculate_workspace_usage_statistics(g, workspace_id)
    print_results([result], format_=format_)


@app.command("user")
def user_usage_stats(user_id: int, format_: OutputFormat = typer.Option(OutputFormat.json, "--format")):
    g = get_context()
    result = calculate_user_statistics(g, user_id)
    print_results([result], format_=format_)


@app.command("global")
def usage_stats(format_: OutputFormat = typer.Option(OutputFormat.json, "--format")):
    g = get_context()
    results = []
    for user in g.backend.users.all():
        results.append(calculate_user_statistics(g, user_id=user.id))
    print_results(results, format_=format_)
