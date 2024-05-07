import json
import random
import string

from typing import Optional, Tuple
from datetime import datetime
from itsdangerous import URLSafeSerializer, BadData
from structlog import get_logger
from gitential2.datatypes.api_keys import PersonalAccessToken, WorkspaceAPIKey

from .context import GitentialContext


logger = get_logger(__name__)


def _generate_random_hash(size: int = 32):
    return "".join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(size))


def create_personal_access_token(
    g: GitentialContext, user_id: int, name: str, expire_at: Optional[datetime]
) -> Tuple[PersonalAccessToken, str]:
    pat_id = _generate_random_hash(24)
    token = _generate_token(g, pat_id, user_id, name, expire_at)
    pat = g.backend.pats.create(PersonalAccessToken(id=pat_id, user_id=user_id, name=name, expire_at=expire_at))
    return pat, token


def delete_personal_access_tokens_for_user(g: GitentialContext, user_id: int):
    for pat in g.backend.pats.all():
        if pat.user_id == user_id:
            delete_personal_access_token(g, pat.id)


def validate_personal_access_token(g: GitentialContext, token: str) -> Tuple[int, bool]:
    try:
        parsed_token = _parse_token(g, token)
    except (BadData, ValueError):
        logger.warning("Bad or expired workspace-api-key", exc_info=True)
        return 0, False

    pat_id, user_id, name, _, _ = parsed_token
    pat_in_db = g.backend.pats.get(pat_id)
    if pat_in_db:
        if pat_in_db.expire_at and pat_in_db.expire_at < g.current_time():
            return user_id, False
        elif pat_in_db.name.startswith(name):
            return user_id, True
        else:
            return user_id, False
    else:
        return user_id, False


def delete_personal_access_token(g: GitentialContext, pat_id: str) -> int:
    return g.backend.pats.delete(pat_id)


def _generate_token(g: GitentialContext, pat_id: str, user_id: int, name: str, expire_at: Optional[datetime]) -> str:
    salt = _generate_random_hash(8)
    expire_at_repr = json.dumps(expire_at) if expire_at else None
    current_time = g.current_time().isoformat()
    serializer = URLSafeSerializer(g.settings.secret, salt=salt)
    code = serializer.dumps([pat_id, user_id, name[:8], expire_at_repr, current_time])
    return f"g2p_{salt}_{code}"


def _parse_token(g: GitentialContext, token: str):
    _, salt, *code_parts = token.split("_")
    code = "_".join(code_parts)

    serializer = URLSafeSerializer(g.settings.secret, salt=salt)
    pat_id, user_id, pat_name, expire_at_str, created_at_str = serializer.loads(code)
    return (
        pat_id,
        user_id,
        pat_name,
        datetime.fromisoformat(expire_at_str) if expire_at_str else None,
        datetime.fromisoformat(created_at_str),
    )


def create_workspace_api_key(g: GitentialContext, workspace_id: int) -> Tuple[WorkspaceAPIKey, str]:
    workspace_api_key_id = _generate_random_hash(24)
    token = _generate_api_key(g, workspace_api_key_id, workspace_id)
    workspace_api_key = g.backend.workspace_api_keys.create(
        WorkspaceAPIKey(
            id=workspace_api_key_id,
            workspace_id=workspace_id,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
    )
    return workspace_api_key, token


def _generate_api_key(g: GitentialContext, workspace_api_key_id: str, workspace_id: int) -> str:
    salt = _generate_random_hash(8)
    current_time = g.current_time().isoformat()
    serializer = URLSafeSerializer(g.settings.secret, salt=salt)
    code = serializer.dumps([workspace_api_key_id, workspace_id, current_time])
    return f"g2w_{salt}_{code}"


def validate_workspace_api_key(g: GitentialContext, token: str) -> Tuple[int, bool]:
    try:
        parsed_api_key = _parse_api_key(g, token)
    except (BadData, ValueError):
        logger.warning("Bad or expired token", exc_info=True)
        return 0, False

    workspace_api_key_id, workspace_id, _ = parsed_api_key
    workspace_api_key_id_in_db = g.backend.workspace_api_keys.get(workspace_api_key_id)
    if workspace_api_key_id_in_db:
        if workspace_api_key_id_in_db.workspace_id == workspace_id:
            return workspace_id, True
        else:
            return workspace_id, False
    else:
        return workspace_id, False


def delete_workspace_api_key(g: GitentialContext, workspace_api_key_id: str) -> int:
    return g.backend.workspace_api_keys.delete(workspace_api_key_id)


def delete_api_keys_for_workspace(g: GitentialContext, workspace_id: int):
    for workspace_api_key in g.backend.workspace_api_keys.all():
        if workspace_api_key.workspace_id == workspace_id:
            delete_workspace_api_key(g, workspace_api_key.id)


def _parse_api_key(g: GitentialContext, token: str):
    _, salt, *code_parts = token.split("_")
    code = "_".join(code_parts)

    serializer = URLSafeSerializer(g.settings.secret, salt=salt)
    workspace_api_key_id, workspace_id, created_at_str = serializer.loads(code)
    return (
        workspace_api_key_id,
        workspace_id,
        datetime.fromisoformat(created_at_str),
    )


def get_api_key_by_workspace_id(g: GitentialContext, workspace_id: int) -> Optional[WorkspaceAPIKey]:
    workspace_api_key = g.backend.workspace_api_keys.get_single_api_key_by_workspace_id(workspace_id)
    return workspace_api_key


def generate_workspace_token(g: GitentialContext, workspace_id: int) -> str:
    delete_api_keys_for_workspace(g, workspace_id)
    # pylint: disable=unused-variable
    _single_workspace_api_key, token = create_workspace_api_key(g, workspace_id)
    return token
