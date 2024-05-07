from typing import Optional
from gitential2.datatypes.users import UserInDB
from .context import GitentialContext


def get_user_by_email(g: GitentialContext, email: str) -> Optional[UserInDB]:
    user = g.backend.users.get_by_email(email)
    if user:
        return user
    else:
        user_info = g.backend.user_infos.get_by_email(email)
        if user_info:
            return g.backend.users.get(user_info.user_id)
        else:
            return None
