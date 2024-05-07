from typing import List
from fastapi import APIRouter, Depends
from structlog import get_logger
from gitential2.datatypes.workspaces import WorkspacePublic, WorkspaceCreate, WorkspaceUpdate, WorkspaceDuplicate

from gitential2.datatypes.permissions import Entity, Action
from gitential2.datatypes.credentials import CredentialCreate, CredentialInDB, CredentialType
from gitential2.core.context import GitentialContext
from gitential2.core.permissions import check_permission
from gitential2.core.workspaces import (
    get_accessible_workspaces,
    get_workspace,
    get_members,
    remove_workspace_membership,
    update_workspace,
    delete_workspace,
    get_workspace_subscription,
)
from gitential2.core.credentials import (
    create_credential_for_workspace,
    list_credentials_for_workspace,
    delete_credential_from_workspace,
    list_connected_repository_sources,
    list_connected_its_sources,
)
from gitential2.core.refresh_v2 import refresh_workspace
from gitential2.core.workspace_common import create_workspace, duplicate_workspace
from gitential2.core.api_keys import (
    delete_api_keys_for_workspace,
    generate_workspace_token,
    get_api_key_by_workspace_id,
)

from ..dependencies import current_user, gitential_context
from ...core.search import search_entity

logger = get_logger(__name__)

router = APIRouter(tags=["workspaces"])


@router.get("/workspaces", response_model=List[WorkspacePublic], response_model_exclude_unset=True)
def workspaces(
    include_members: bool = False,
    include_projects: bool = False,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    return get_accessible_workspaces(
        g=g, current_user=current_user, include_members=include_members, include_projects=include_projects
    )


@router.get("/workspaces/{workspace_id}", response_model=WorkspacePublic)
def get_workspace_(
    workspace_id: int,
    include_members: bool = False,
    include_projects: bool = False,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_workspace(
        g=g,
        workspace_id=workspace_id,
        current_user=current_user,
        include_members=include_members,
        include_projects=include_projects,
    )


@router.put("/workspaces/{workspace_id}", response_model=WorkspacePublic)
def update_workspace_(
    workspace_id: int,
    workspace_update: WorkspaceUpdate,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.update, workspace_id=workspace_id)
    return update_workspace(
        g=g,
        workspace_id=workspace_id,
        workspace=workspace_update,
        current_user=current_user,
    )


@router.delete("/workspaces/{workspace_id}")
def delete_workspace_(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.delete, workspace_id=workspace_id)
    return delete_workspace(g, workspace_id, current_user)


@router.post("/workspaces", response_model=WorkspacePublic)
def create_workspace_(
    workspace_create: WorkspaceCreate,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.create)
    return create_workspace(g, workspace_create, current_user=current_user, primary=False)


@router.post("/workspaces/duplicate", response_model=WorkspacePublic)
def duplicate_workspace_(
    workspace_duplicate: WorkspaceDuplicate,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.create)
    return duplicate_workspace(g, workspace_duplicate=workspace_duplicate, current_user=current_user)


@router.get("/workspaces/{workspace_id}/repository-sources")
def list_connected_repository_sources_(workspace_id: int, g: GitentialContext = Depends(gitential_context)):
    return list_connected_repository_sources(g, workspace_id)


@router.get("/workspaces/{workspace_id}/its-sources")
def list_connected_its_sources_(workspace_id: int, g: GitentialContext = Depends(gitential_context)):
    return list_connected_its_sources(g, workspace_id)


@router.get("/workspaces/{workspace_id}/subscription")
def get_workspace_owner_subscription(workspace_id: int, g: GitentialContext = Depends(gitential_context)):
    return get_workspace_subscription(g, workspace_id)


@router.get("/workspaces/{workspace_id}/members")
def list_workspace_members(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.membership, Action.read, workspace_id=workspace_id)
    return get_members(g, workspace_id=workspace_id)


@router.delete("/workspaces/{workspace_id}/members/{workspace_member_id}")
def remove_workspace_member(
    workspace_id: int,
    workspace_member_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(
        g,
        current_user,
        Entity.membership,
        Action.delete,
        workspace_id=workspace_id,
        workspace_member_id=workspace_member_id,
    )
    return remove_workspace_membership(g=g, workspace_id=workspace_id, workspace_member_id=workspace_member_id)


@router.get("/workspaces/{workspace_id}/credentials")
def list_workspace_credentials(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.credential, Action.read, workspace_id=workspace_id)
    credentials = list_credentials_for_workspace(g, workspace_id)
    return [_decrypt_credential(credential, g) for credential in credentials]


@router.delete("/workspaces/{workspace_id}/credentials/{credential_id}")
def delete_workspace_credential(
    workspace_id: int,
    credential_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.credential, Action.delete, workspace_id=workspace_id)
    return delete_credential_from_workspace(g, workspace_id, credential_id)


@router.post("/workspaces/{workspace_id}/credentials")
def create_workspace_credential(
    credential_create: CredentialCreate,
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.credential, Action.create, workspace_id=workspace_id)
    return _decrypt_credential(create_credential_for_workspace(g, workspace_id, credential_create=credential_create), g)


def _decrypt_credential(credential: CredentialInDB, g: GitentialContext):
    if credential.type == CredentialType.keypair:
        credential.public_key = (
            g.fernet.decrypt_string(credential.public_key.decode()).encode() if credential.public_key else None
        )
        credential.private_key = (
            g.fernet.decrypt_string(credential.private_key.decode()).encode() if credential.private_key else None
        )
    return credential


@router.get("/workspaces/{workspace_id}/search/{entity_type}")
def search(
    q: str,
    workspace_id: int,
    entity_type: str,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
) -> List[dict]:
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    logger.info("searching for ", q=q, entity_type=entity_type, workspace_id=workspace_id)
    return search_entity(g, q, workspace_id, entity_type)


@router.post("/workspaces/{workspace_id}/refresh")
def refresh_workspace_(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.update, workspace_id=workspace_id)
    refresh_workspace(g, workspace_id)
    return True


@router.get("/workspaces/{workspace_id}/generate-token")
def generate_workspace_token_(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return generate_workspace_token(g, workspace_id)


@router.get("/workspaces/{workspace_id}/api-key")
def get_existing_workspace_token(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_api_key_by_workspace_id(g, workspace_id)


@router.delete("/workspaces/{workspace_id}/api-key")
def delete_workspace_api_key_for_workspace(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.delete, workspace_id=workspace_id)
    delete_api_keys_for_workspace(g, workspace_id)
