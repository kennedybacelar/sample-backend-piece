import json
import typer
from structlog import get_logger

from gitential2.core.tasks import schedule_task, configure_celery
from .common import get_context

app = typer.Typer()
logger = get_logger(__name__)


@app.command("schedule")
def schedule_task_(taks_name: str, params: str = "{}"):
    g = get_context()
    configure_celery(g.settings)

    schedule_task(g, task_name=taks_name, params=json.loads(params))
