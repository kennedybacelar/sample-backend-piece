import contextlib
from datetime import datetime, timedelta
from typing import List, Optional, cast
from structlog import get_logger
from authlib.integrations.base_client.errors import OAuthError
from gitential2.datatypes.credentials import CredentialInDB, CredentialCreate, CredentialType, CredentialUpdate
from gitential2.datatypes.repositories import RepositoryInDB

from gitential2.utils.ssh import create_ssh_keypair
from gitential2.integrations import REPOSITORY_SOURCES, ISSUE_SOURCES
from .context import GitentialContext
from ..exceptions import NotImplementedException, NotFoundException


logger = get_logger(__name__)


@contextlib.contextmanager
def acquire_credential(
    g: GitentialContext,
    credential_id: Optional[int] = None,
    user_id: Optional[int] = None,
    workspace_id: Optional[int] = None,
    integration_name: Optional[str] = None,
    blocking_timeout_seconds=5 * 60,
    timeout_seconds=30 * 60,
):
    credential = _get_credential(g, credential_id, user_id, workspace_id, integration_name)

    if credential:
        logger.info(
            "Acquiring credential",
            credential_id=credential.id,
            credential_name=credential.name,
            owner_id=credential.owner_id,
        )
        with g.kvstore.lock(
            f"credential-lock-{credential.id}", timeout=timeout_seconds, blocking_timeout=blocking_timeout_seconds
        ):

            if credential.type == CredentialType.token:
                integration = g.integrations.get(credential.integration_name)
                token = credential.to_token_dict(g.fernet)
                if integration and token:
                    is_refreshed, updated_token = integration.refresh_token_if_expired(
                        token, update_token=get_update_token_callback(g, credential)
                    )
                    if is_refreshed:
                        logger.debug("Updating credential with the new token")
                        credential.update_token(updated_token, g.fernet)
            logger.info(
                "Giving credential",
                credential_id=credential.id,
                credential_name=credential.name,
                owner_id=credential.owner_id,
            )
            yield credential


def _get_credential(
    g: GitentialContext,
    credential_id: Optional[int] = None,
    user_id: Optional[int] = None,
    workspace_id: Optional[int] = None,
    integration_name: Optional[str] = None,
) -> Optional[CredentialInDB]:
    credential = None

    if credential_id:
        credential = g.backend.credentials.get(credential_id)
    elif integration_name and user_id:
        credential = g.backend.credentials.get_by_user_and_integration(
            owner_id=user_id, integration_name=integration_name
        )
    elif integration_name and workspace_id:
        workspace = g.backend.workspaces.get_or_error(workspace_id)
        credential = g.backend.credentials.get_by_user_and_integration(
            owner_id=workspace.created_by, integration_name=integration_name
        )

    return credential


def get_fresh_credential(
    g: GitentialContext,
    credential_id: Optional[int] = None,
    user_id: Optional[int] = None,
    workspace_id: Optional[int] = None,
    integration_name: Optional[str] = None,
) -> Optional[CredentialInDB]:
    credential = _get_credential(g, credential_id, user_id, workspace_id, integration_name)
    if credential:
        if credential.type == CredentialType.token:
            credential = _refresh_token_credential_if_its_going_to_expire(g, credential)
    return credential


def _refresh_token_credential_if_its_going_to_expire(
    g: GitentialContext,
    credential: CredentialInDB,
    blocking_timeout_seconds=5 * 60,
    timeout_seconds=30 * 60,
    expire_timeout_seconds=10 * 60,
):
    def _token_is_about_to_expire(credential):
        logger.info(
            "credential expire check",
            expires_at=credential.expires_at,
            timeout_at=credential.expires_at - timedelta(seconds=expire_timeout_seconds)
            if credential.expires_at
            else None,
            current_time=datetime.utcnow(),
        )
        return (
            credential.expires_at
            and credential.expires_at - timedelta(seconds=expire_timeout_seconds) < datetime.utcnow()
        )

    def _token_is_invalid(integration, token):
        return not integration.check_token(token)

    integration = g.integrations.get(credential.integration_name)
    if integration:
        with g.kvstore.lock(
            f"credential-lock-{credential.id}", timeout=timeout_seconds, blocking_timeout=blocking_timeout_seconds
        ):
            token = credential.to_token_dict(g.fernet)
            if _token_is_about_to_expire(credential) or _token_is_invalid(integration, token):
                logger.info(
                    "Trying to refresh token", credential_id=credential.id, integration_name=credential.integration_name
                )
                try:
                    updated_token = integration.refresh_token(token)
                    if updated_token:
                        logger.debug("Updating credential with the new token")
                        credential.update_token(updated_token, g.fernet)
                        callback = get_update_token_callback(g, credential)
                        callback(updated_token)
                    else:
                        logger.warning(
                            "Failed to refresh expired token",
                            credential_id=credential.id,
                            integration_name=credential.integration_name,
                        )
                        return None
                except OAuthError:
                    logger.exception("Failed to refresh token, OAuthError")
                    return None
    else:
        logger.info(
            "Skipping token refresh, unknown integration",
            credential_id=credential.id,
            integration_name=credential.integration_name,
        )
        return None
    return credential


def get_update_token_callback(g: GitentialContext, credential: CredentialInDB):
    # pylint: disable=unused-argument
    def callback(token: dict, refresh_token=None, access_token=None) -> Optional[CredentialInDB]:

        logger.info(
            "updating acces_token",
            user_id=credential.owner_id,
            integration_name=credential.integration_name,
            credential_id=credential.id,
        )

        if "access_token" in token:
            return g.backend.credentials.update(
                credential.id,
                CredentialUpdate.from_token(
                    token,
                    g.fernet,
                    owner_id=cast(int, credential.owner_id),
                    integration_name=credential.integration_name,
                    integration_type=credential.integration_type,
                ),
            )

        else:
            logger.error("update_token error", token=token, credential=credential)
            return None

    return callback


def list_credentials_for_user(g: GitentialContext, user_id: int) -> List[CredentialInDB]:
    return g.backend.credentials.get_for_user(user_id)


def list_valid_credentials_for_user(g: GitentialContext, user_id: int) -> List[CredentialInDB]:
    results = []
    for credential in g.backend.credentials.get_for_user(user_id):
        fresh_credential = get_fresh_credential(g, credential_id=credential.id)
        if fresh_credential:
            results.append(fresh_credential)
    return results


def get_workspace_creator_user_id(g: GitentialContext, workspace_id: int):
    return g.backend.workspaces.get_or_error(workspace_id).created_by


def list_credentials_for_workspace(g: GitentialContext, workspace_id: int):
    user_id = get_workspace_creator_user_id(g=g, workspace_id=workspace_id)
    return list_credentials_for_user(g, user_id=user_id)


def list_valid_credentials_for_workspace(g: GitentialContext, workspace_id: int):
    user_id = get_workspace_creator_user_id(g=g, workspace_id=workspace_id)
    return list_valid_credentials_for_user(g, user_id=user_id)


def create_credential(g: GitentialContext, credential_create: CredentialCreate, owner_id: int) -> CredentialInDB:
    if credential_create.type == CredentialType.keypair:
        private_key, public_key = create_ssh_keypair()
        credential_create.private_key = g.fernet.encrypt_bytes(private_key)
        credential_create.public_key = g.fernet.encrypt_bytes(public_key)

        credential_create.owner_id = owner_id
        return g.backend.credentials.create(credential_create)
    else:
        raise NotImplementedException("Only ssh keypair credential creation supported.")


# pylint: disable=too-complex
def delete_credential_from_workspace(g: GitentialContext, workspace_id: int, credential_id: int):
    credential = g.backend.credentials.get(credential_id)
    if credential is not None:
        if credential.type == CredentialType.keypair:

            repo_ids_to_remove = [
                repository.id
                for repository in g.backend.repositories.all(workspace_id)
                if repository.credential_id == credential.id
            ]
            for project in g.backend.projects.all(workspace_id):
                g.backend.project_repositories.remove_repo_ids_from_project(
                    workspace_id, project.id, repo_ids_to_remove
                )
            for repo_id in repo_ids_to_remove:
                g.backend.repositories.delete(workspace_id, repo_id)

            return g.backend.credentials.delete(credential_id)

        elif credential.type == CredentialType.token:

            repo_ids_to_remove = [
                repository.id
                for repository in g.backend.repositories.all(workspace_id)
                if repository.integration_type == credential.integration_type
            ]

            for project in g.backend.projects.all(workspace_id):
                g.backend.project_repositories.remove_repo_ids_from_project(
                    workspace_id, project.id, repo_ids_to_remove
                )

            for repo_id in repo_ids_to_remove:
                g.backend.repositories.delete(workspace_id, repo_id)

            if credential.integration_type in ISSUE_SOURCES:
                its_projects_to_remove = [
                    its_project.id
                    for its_project in g.backend.its_projects.all(workspace_id)
                    if its_project.integration_type == credential.integration_type
                ]

                for proj_its_proj in g.backend.its_projects.all(workspace_id):
                    g.backend.project_its_projects.remove_itsp_ids_from_project(
                        workspace_id, proj_its_proj.id, its_projects_to_remove
                    )

                for its_project in its_projects_to_remove:
                    g.backend.its_projects.delete(workspace_id, its_project)

            return g.backend.credentials.delete(credential_id)

        else:
            raise NotImplementedException("Only ssh keypair credential delete supported.")
    else:
        raise NotFoundException("Credential not found.")


def create_credential_for_workspace(
    g: GitentialContext, workspace_id: int, credential_create: CredentialCreate
) -> CredentialInDB:
    workspace = g.backend.workspaces.get_or_error(workspace_id)
    return create_credential(g, credential_create, owner_id=workspace.created_by)


def get_credential_for_repository(
    g: GitentialContext, workspace_id: int, repository: RepositoryInDB
) -> Optional[CredentialInDB]:
    if repository.credential_id:
        return g.backend.credentials.get(repository.credential_id)
    if repository.integration_name:
        workspace = g.backend.workspaces.get_or_error(workspace_id)
        return g.backend.credentials.get_by_user_and_integration(
            owner_id=workspace.created_by, integration_name=repository.integration_name
        )
    return None


def list_connected_repository_sources(g: GitentialContext, workspace_id: int) -> List[str]:
    return [
        credential.integration_name
        for credential in list_valid_credentials_for_workspace(g, workspace_id)
        if (
            credential.integration_name
            and credential.integration_type in REPOSITORY_SOURCES
            and credential.integration_name in g.integrations
        )
    ]


def list_connected_its_sources(g: GitentialContext, workspace_id: int) -> List[str]:
    return [
        credential.integration_name
        for credential in list_valid_credentials_for_workspace(g, workspace_id)
        if (
            credential.integration_name
            and credential.integration_type in ISSUE_SOURCES
            and credential.integration_name in g.integrations
        )
    ]
