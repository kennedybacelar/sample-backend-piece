from datetime import datetime
from typing import Optional
import typer
from structlog import get_logger
from gitential2.core.tasks import send_scheduled_emails, configure_celery
from gitential2.core.users import get_user
from gitential2.core.emails import send_email_to_user

from .common import get_context, OutputFormat, print_results


app = typer.Typer()

logger = get_logger(__name__)


@app.command("list")
def list_scheduled_emails(
    format_: OutputFormat = typer.Option(OutputFormat.tabulate, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    results = list(g.backend.email_log.all())
    print_results(results, format_=format_, fields=fields)


@app.command("schedule")
def schedule_(
    user_id: int,
    template_name: str,
    scheduled_at: datetime = typer.Option(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")),
):
    g = get_context()
    g.backend.email_log.schedule_email(user_id=user_id, template_name=template_name, scheduled_at=scheduled_at)


@app.command("cancel")
def cancel_(
    user_id: int,
    template_name: str,
):
    g = get_context()
    g.backend.email_log.cancel_email(user_id=user_id, template=template_name)


@app.command("send")
def send_(
    user_id: int,
    template_name: str,
):
    g = get_context()
    user = get_user(g, user_id=user_id)
    if user:
        send_email_to_user(g, user, template_name)


@app.command("trigger-sending")
def run_cronjob():
    g = get_context()
    configure_celery(g.settings)
    send_scheduled_emails.apply_async()
