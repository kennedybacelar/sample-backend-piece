from enum import Enum

from typing import Dict, Optional, Sequence, Any, Type
from pydantic.main import BaseModel
import typer
from structlog import get_logger

from gitential2.datatypes.common import CoreModel
from gitential2.datatypes.subscriptions import SubscriptionCreate, SubscriptionUpdate
from gitential2.datatypes.users import UserCreate, UserUpdate
from gitential2.datatypes.workspaces import WorkspaceCreate, WorkspaceUpdate
from gitential2.datatypes.credentials import CredentialCreate, CredentialUpdate
from gitential2.datatypes.teammembers import TeamMemberCreate, TeamMemberUpdate
from gitential2.datatypes.teams import TeamCreate, TeamUpdate
from gitential2.datatypes.authors import AuthorCreate, AuthorUpdate
from gitential2.datatypes.project_repositories import ProjectRepositoryCreate, ProjectRepositoryUpdate
from gitential2.datatypes.projects import ProjectCreate, ProjectUpdate
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryUpdate
from gitential2.datatypes.workspacemember import WorkspaceMemberCreate, WorkspaceMemberUpdate


from gitential2.core.context import GitentialContext
from .common import get_context, print_results, OutputFormat

app = typer.Typer()
logger = get_logger(__name__)


class EntityType(str, Enum):
    # global entities
    user = "user"
    subscription = "subscription"
    workspace = "workspace"
    workspace_invitation = "workspace_invitation"
    workspace_member = "workspace_member"
    credential = "credential"
    reseller_code = "reseller_code"

    # workspace scoped
    repository = "repository"
    project = "project"
    project_repository = "project_repository"
    author = "author"
    team = "team"
    team_member = "team_member"


global_entitites = [
    EntityType.user,
    EntityType.reseller_code,
    EntityType.subscription,
    EntityType.workspace,
    EntityType.workspace_invitation,
    EntityType.workspace_member,
    EntityType.credential,
]


def _repositories(g: GitentialContext):
    return {
        # global
        EntityType.user: g.backend.users,
        EntityType.reseller_code: g.backend.reseller_codes,
        EntityType.subscription: g.backend.subscriptions,
        EntityType.workspace: g.backend.workspaces,
        EntityType.workspace_invitation: g.backend.workspace_invitations,
        EntityType.workspace_member: g.backend.workspace_members,
        EntityType.credential: g.backend.credentials,
        # workspace scoped
        EntityType.repository: g.backend.repositories,
        EntityType.project: g.backend.projects,
        EntityType.project_repository: g.backend.project_repositories,
        EntityType.author: g.backend.authors,
        EntityType.team: g.backend.teams,
        EntityType.team_member: g.backend.team_members,
    }


def _crud_commons(entity_type: EntityType, workspace_id: Optional[int]):
    if entity_type not in global_entitites and not workspace_id:
        typer.echo("Missing workspace id. (use --workspace or -w)")
        raise typer.Exit(-1)

    workspace_id = workspace_id or 0
    g = get_context()
    repositories = _repositories(g)
    repository = repositories[entity_type]

    return g, repository


@app.command("list")
def list_(
    entity_type: EntityType,
    workspace_id: Optional[int] = typer.Option(None, "--workspace", "-w"),
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    results: Sequence[CoreModel] = []
    _, repository = _crud_commons(entity_type, workspace_id)

    if entity_type in global_entitites:
        results = list(repository.all())
    else:
        results = list(repository.all(workspace_id=workspace_id))

    print_results(results, format_=format_, fields=fields)


@app.command("get")
def get_(
    entity_type: EntityType,
    object_id: int,
    workspace_id: Optional[int] = typer.Option(None, "--workspace", "-w"),
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    _, repository = _crud_commons(entity_type, workspace_id)

    if entity_type in global_entitites:
        result = repository.get(object_id)
    else:
        result = repository.get(workspace_id, object_id)

    print_results([result], format_=format_, fields=fields)


@app.command("delete")
def delete_(
    entity_type: EntityType,
    object_id: int,
    workspace_id: Optional[int] = typer.Option(None, "--workspace", "-w"),
):
    _, repository = _crud_commons(entity_type, workspace_id)

    if entity_type in global_entitites:
        result = repository.delete(object_id)
    else:
        result = repository.delete(workspace_id, object_id)
    print(result)


@app.command("create")
def create_(
    entity_type: EntityType,
    object_json: str,
    workspace_id: Optional[int] = typer.Option(None, "--workspace", "-w"),
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    _create_classes: Dict[Any, Type[BaseModel]] = {
        EntityType.user: UserCreate,
        EntityType.subscription: SubscriptionCreate,
        EntityType.workspace: WorkspaceCreate,
        EntityType.workspace_member: WorkspaceMemberCreate,
        EntityType.credential: CredentialCreate,
        # workspace scoped
        EntityType.repository: RepositoryCreate,
        EntityType.project: ProjectCreate,
        EntityType.project_repository: ProjectRepositoryCreate,
        EntityType.author: AuthorCreate,
        EntityType.team: TeamCreate,
        EntityType.team_member: TeamMemberCreate,
    }

    create_cls: Type[BaseModel] = _create_classes[entity_type]
    create_object = create_cls.parse_raw(object_json)

    _, repository = _crud_commons(entity_type, workspace_id)

    if entity_type in global_entitites:
        result = repository.create(create_object)
    else:
        result = repository.create(workspace_id, create_object)

    print_results([result], format_=format_, fields=fields)


@app.command("update")
def update_(
    entity_type: EntityType,
    object_id: int,
    object_json: str,
    workspace_id: Optional[int] = typer.Option(None, "--workspace", "-w"),
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    _update_classes: Dict[Any, Type[BaseModel]] = {
        EntityType.user: UserUpdate,
        EntityType.subscription: SubscriptionUpdate,
        EntityType.workspace: WorkspaceUpdate,
        EntityType.workspace_member: WorkspaceMemberUpdate,
        EntityType.credential: CredentialUpdate,
        # workspace scoped
        EntityType.repository: RepositoryUpdate,
        EntityType.project: ProjectUpdate,
        EntityType.project_repository: ProjectRepositoryUpdate,
        EntityType.author: AuthorUpdate,
        EntityType.team: TeamUpdate,
        EntityType.team_member: TeamMemberUpdate,
    }

    update_cls: Type[BaseModel] = _update_classes[entity_type]
    update_object = update_cls.parse_raw(object_json)

    _, repository = _crud_commons(entity_type, workspace_id)

    if entity_type in global_entitites:
        result = repository.update(object_id, update_object)
    else:
        result = repository.update(workspace_id, object_id, update_object)

    print_results([result], format_=format_, fields=fields)
