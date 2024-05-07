from datetime import datetime
from typing import Optional, List

import typer
import uvicorn
from structlog import get_logger

from gitential2.core.api_keys import (
    create_personal_access_token,
    delete_personal_access_tokens_for_user,
    create_workspace_api_key,
)
from gitential2.core.authors import fix_author_aliases, fix_author_names
from gitential2.core.deduplication import deduplicate_authors
from gitential2.core.emails import send_email_to_user
from gitential2.core.maintenance import maintenance
from gitential2.core.quick_login import generate_quick_login
from gitential2.core.tasks import configure_celery
from gitential2.core.users import get_user
from gitential2.logging import initialize_logging
from gitential2.settings import load_settings
from .authors import app as authors_app
from .common import OutputFormat, get_context, print_results
from .crud import app as crud_app
from .data_queries import app as data_queries_app
from .deploys import app as deploys_app
from .emails import app as emails_app
from .export import app as export_app
from .extract import app as extraction_app
from .invitation import app as invitation_app
from .its import app as its_app
from .jira import app as jira_app
from .projects import app as projects_app
from .query import app as query_app
from .refresh import app as refresh_app
from .repositories import app as repositories_app
from .reseller_codes import app as reseller_codes
from .status import app as status_app
from .tasks import app as tasks_app
from .usage_stats import app as usage_stats_app
from .users import app as users_app
from .vsts import app as vsts_app
from .auto_export import app as auto_export_app
from .workspaces import app as workspaces_app
from .cache import app as cache_app

logger = get_logger(__name__)

app = typer.Typer()
app.add_typer(export_app, name="export")
app.add_typer(users_app, name="users")
app.add_typer(projects_app, name="projects")
app.add_typer(repositories_app, name="repositories")
app.add_typer(usage_stats_app, name="usage-stats")
app.add_typer(refresh_app, name="refresh")
app.add_typer(tasks_app, name="tasks")
app.add_typer(status_app, name="status")
app.add_typer(query_app, name="query")
app.add_typer(emails_app, name="emails")
app.add_typer(extraction_app, name="extract")
app.add_typer(crud_app, name="crud")
app.add_typer(invitation_app, name="invitation")
app.add_typer(jira_app, name="jira")
app.add_typer(vsts_app, name="vsts")
app.add_typer(its_app, name="its")
app.add_typer(data_queries_app, name="data-query")
app.add_typer(reseller_codes, name="reseller-codes")
app.add_typer(deploys_app, name="deploys")
app.add_typer(authors_app, name="authors")
app.add_typer(auto_export_app, name="auto-export")
app.add_typer(workspaces_app, name="workspaces")
app.add_typer(cache_app, name="cache")


@app.command("public-api")
def public_api(
    host: str = typer.Option("0.0.0.0", "--host", "-h"),
    port: int = typer.Option(7999, "--port", "-p"),
    reload: bool = False,
    update_database: bool = False,
):
    if update_database:
        initialize_database()

    uvicorn.run(
        "gitential2.public_api.main:app",
        host=host,
        port=port,
        log_level="info",
        reload=reload,
        forwarded_allow_ips="*",
        proxy_headers=True,
    )


@app.command("initialize-database")
def initialize_database():
    g = get_context()
    g.backend.initialize()
    g.backend.migrate()
    workspaces_list = g.backend.workspaces.all()
    for w in workspaces_list:
        # logger.info("Initializing workspace schema", workspace_id=w.id)
        try:
            # g.backend.initialize_workspace(w.id)
            # g.backend.migrate_workspace(w.id)

            if g.settings.features.enable_additional_materialized_views:
                g.backend.create_missing_materialized_views(w.id)
            else:
                g.backend.drop_existing_materialized_views(w.id)
        except:  # pylint: disable=bare-except
            logger.exception("Failed to initialize workspace schema", workspace_id=w.id)


@app.command("refresh-materialized-views")
def refresh_materialized_views(workspace_id: Optional[int] = typer.Argument(None)):
    """
    With this command you can refresh materialized views to every workspace in the application OR just for one
    workspace if you provide a specific workspace id.
    """

    def refresh_matview_in_workspaces(wid_list: List[int]):
        for wid in wid_list:
            result = g.backend.refresh_materialized_views_in_workspace(workspace_id=wid)
            logger.info(
                f"Refreshing materialized views in workspace was {'successful' if result else 'failed'}.",
                workspace_id=wid,
            )

    g = get_context()
    workspace = g.backend.workspaces.get(id_=workspace_id) if workspace_id else None
    if workspace:
        refresh_matview_in_workspaces([workspace.id])
    else:
        refresh_matview_in_workspaces([w.id for w in g.backend.workspaces.all()])


@app.command("send-email-to-user")
def send_email_to_user_(
    user_id: int,
    template_name: str,
):
    g = get_context()
    user = get_user(g, user_id=user_id)
    if user:
        send_email_to_user(g, user, template_name)


@app.command("maintenance")
def maintenance_():
    g = get_context()
    configure_celery(g.settings)
    maintenance(g)


@app.command("deduplicate-authors")
def deduplicate_authors_(workspace_id: int, dry_run: bool = False):
    g = get_context()
    configure_celery(g.settings)

    results = deduplicate_authors(g, workspace_id, dry_run)

    for result in results:

        print_results(result, format_=OutputFormat.tabulate, fields=["id", "name", "email", "aliases", "active"])
        print()


@app.command("fix-author-names")
def fix_author_names_(workspace_id: int):
    g = get_context()
    configure_celery(g.settings)
    fix_author_names(g, workspace_id)


@app.command("fix-author-aliases")
def fix_author_aliases_(workspace_id: int):
    g = get_context()
    configure_celery(g.settings)
    fix_author_aliases(g, workspace_id)


@app.command("quick-login")
def quick_login(user_id):
    g = get_context()
    login_hash = generate_quick_login(g, user_id)
    print("Login hash:", login_hash)


@app.command("create-personal-access-token")
def generate_pat_(user_id: int, name: str, expire_at: Optional[datetime] = None):
    g = get_context()
    pat, token = create_personal_access_token(g, user_id, name, expire_at)
    print(pat)
    print("------")
    print(token)
    print("------")


@app.command("delete-personal-access-token")
def delete_pat_for_user_(user_id: int):
    g = get_context()
    delete_personal_access_tokens_for_user(g, user_id)


@app.command("create-workspace-api-key")
def generate_workspace_api_key(workspace_id: int):
    g = get_context()
    workspace_api_key, token = create_workspace_api_key(g, workspace_id)
    print(workspace_api_key)
    print("------")
    print(token)
    print("------")


def main(prog_name: Optional[str] = None):
    initialize_logging(load_settings())
    app(prog_name=prog_name)
