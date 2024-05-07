from typing import List, Optional
from structlog import get_logger
from gitential2.datatypes.credentials import CredentialCreate, CredentialType
from gitential2.core import GitentialContext


logger = get_logger(__name__)


def import_legacy_secrets(g: GitentialContext, legacy_secrets: List[dict]):
    for legacy_secret in legacy_secrets:
        _import_legacy_secret(g, legacy_secret)
    g.backend.credentials.reset_primary_key_id()


def _import_legacy_secret(g: GitentialContext, legacy_secret: dict):
    def _encrypt(s: Optional[str]):
        if s:
            return g.fernet.encrypt_string(s).encode()
        else:
            return None

    owner_id = legacy_secret["owner"]["id"] if "owner" in legacy_secret else legacy_secret["owner_id"]

    logger.info("Importing secret", name=legacy_secret["name"], owner_id=owner_id, type=legacy_secret["type"])

    g.backend.credentials.create(
        CredentialCreate(
            owner_id=owner_id,
            name=legacy_secret["name"],
            created_at=legacy_secret["created_at"],
            updated_at=legacy_secret["updated_at"],
            type=CredentialType.token if legacy_secret["type"] == "token" else CredentialType.keypair,
            integration_name=legacy_secret["name"].split(" ")[0] if legacy_secret["type"] == "token" else None,
            integration_type=legacy_secret["name"].split(" ")[0] if legacy_secret["type"] == "token" else None,
            token=_encrypt(legacy_secret["token"]),
            refresh_token=_encrypt(legacy_secret["refresh_token"]),
            public_key=_encrypt(legacy_secret["public_key"]),
            private_key=_encrypt(legacy_secret["private_key"]),
        )
    )
