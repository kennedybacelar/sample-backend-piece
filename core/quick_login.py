import random
import string
from typing import Optional, cast
from gitential2.datatypes.users import UserInDB
from .context import GitentialContext


def get_quick_login_user(g: GitentialContext, login_hash: str) -> Optional[UserInDB]:
    user_id = g.kvstore.get_value(f"quick-login-{login_hash}")
    if user_id:
        return g.backend.users.get(cast(int, user_id))
    else:
        return None


def generate_quick_login(g: GitentialContext, user_id: int):
    login_hash = _generate_hash()
    g.kvstore.set_value(f"quick-login-{login_hash}", user_id, 1200)
    return login_hash


def _generate_hash():
    return "".join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(32))
