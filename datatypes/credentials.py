from abc import ABC
from enum import Enum
from typing import Optional, cast
from datetime import datetime, timezone

from pydantic.dataclasses import dataclass
from pydantic.datetime_parse import parse_datetime
from gitential2.secrets import Fernet
from .common import IDModelMixin, DateTimeModelMixin, CoreModel, ExtraFieldMixin


class CredentialType(str, Enum):
    token = "token"
    keypair = "keypair"
    passphrase = "passphrase"


class RepositoryCredential(ABC):
    pass


@dataclass
class KeypairCredential(RepositoryCredential):
    username: str = "git"
    pubkey: Optional[str] = None
    privkey: Optional[str] = None
    passphrase: Optional[str] = None


@dataclass
class UserPassCredential(RepositoryCredential):
    username: str
    password: str


class CredentialBasePublic(ExtraFieldMixin, CoreModel):
    owner_id: Optional[int] = None
    type: Optional[CredentialType] = None
    integration_name: Optional[str] = None
    integration_type: Optional[str] = None
    name: Optional[str] = None

    expires_at: Optional[datetime] = None


class CredentialBaseSecret(CoreModel):
    token: Optional[bytes] = None
    refresh_token: Optional[bytes] = None
    public_key: Optional[bytes] = None
    private_key: Optional[bytes] = None
    passphrase: Optional[bytes] = None


class CredentialBase(CredentialBasePublic, CredentialBaseSecret):
    def to_repository_credential(self, fernet: Fernet) -> RepositoryCredential:
        if self.type == CredentialType.keypair:
            return KeypairCredential(
                username="git",
                pubkey=fernet.decrypt_string(self.public_key.decode()) if self.public_key else None,
                privkey=fernet.decrypt_string(self.private_key.decode()) if self.private_key else None,
                passphrase=fernet.decrypt_string(self.passphrase.decode()) if self.passphrase else None,
            )
        elif self.type == CredentialType.token:
            token = fernet.decrypt_string(self.token.decode()) if self.token else ""
            if self.integration_type == "gitlab":
                return UserPassCredential(username="oauth2", password=token)
            elif self.integration_type in {"github", "vsts"}:
                return UserPassCredential(username=token, password="x-oauth-basic")
            elif self.integration_type == "bitbucket":
                return UserPassCredential(username="x-token-auth", password=token)
        raise ValueError(f"Don't know how to convert credential {self}")

    # pylint: disable=too-many-arguments
    @classmethod
    def from_token(
        cls,
        token: dict,
        fernet: Fernet,
        owner_id: int,
        integration_name: Optional[str] = None,
        integration_type: Optional[str] = None,
    ):
        return cls(
            type=CredentialType.token,
            integration_name=integration_name,
            integration_type=integration_type,
            owner_id=owner_id,
            name=f"{integration_name} token",
            token=fernet.encrypt_string(token["access_token"]).encode(),
            refresh_token=fernet.encrypt_string(cast(str, token.get("refresh_token"))).encode()
            if token.get("refresh_token")
            else None,
            expires_at=token.get("expires_at"),
        )

    def to_token_dict(self, fernet: Fernet) -> Optional[dict]:
        if self.token:
            ret: dict = {
                "access_token": fernet.decrypt_string(self.token.decode()),
                "refresh_token": fernet.decrypt_string(self.refresh_token.decode()) if self.refresh_token else None,
            }
            if self.expires_at:
                ret["expires_at"] = int(self.expires_at.replace(tzinfo=timezone.utc).timestamp())

            return ret
        else:
            return None

    def update_token(self, token: dict, fernet: Fernet):
        self.token = fernet.encrypt_string(token["access_token"]).encode()
        self.refresh_token = fernet.encrypt_string(cast(str, token.get("refresh_token"))).encode()
        self.expires_at = parse_datetime(token["expires_at"]) if "expires_at" in token else None


class CredentialCreate(CredentialBase):
    # owner_id: int
    type: CredentialType
    name: str


class CredentialUpdate(CredentialBase):
    pass


class CredentialInDB(IDModelMixin, DateTimeModelMixin, CredentialBase):
    pass


class CredentialPublic(IDModelMixin, DateTimeModelMixin, CredentialBasePublic):
    pass
