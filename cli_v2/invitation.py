import typer
from structlog import get_logger
from gitential2.core.workspace_invitations import invite_to_workspace, accept_invitation, delete_invitation
from .common import get_context, print_results

logger = get_logger(__name__)
app = typer.Typer()


@app.command("create")
def create_invitation(workspace_id: int, email: str):
    g = get_context()
    invitations = invite_to_workspace(g, workspace_id, invitations=[email])
    print_results(invitations)


@app.command("accept")
def accept_invitation_(invitation_code: str, user_id: int):
    g = get_context()
    user = g.backend.users.get_or_error(user_id)
    result = accept_invitation(g, invitation_code, user)
    print_results([result])


@app.command("delete")
def delete_invitation_(workspace_id: int, invitation_id: int):
    g = get_context()
    delete_invitation(g, workspace_id, invitation_id)
