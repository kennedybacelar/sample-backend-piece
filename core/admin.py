from typing import List, Optional
from gitential2.datatypes.access_approvals import AccessApprovalCreate, AccessApprovalInDB

from gitential2.datatypes.users import UserInAdminRepr, UserInDB
from .context import GitentialContext
from .users import list_users
from .access_log import get_last_interaction_at


def admin_list_users(g: GitentialContext) -> List[UserInAdminRepr]:
    return [_to_user_in_admin_repr(g, user) for user in list_users(g)]


def _to_user_in_admin_repr(g: GitentialContext, user: UserInDB) -> UserInAdminRepr:
    user_dict = user.dict(include={"id", "created_at", "updated_at", "is_admin", "is_active", "email"})
    return UserInAdminRepr(
        name=user.full_name,
        last_interaction_at=get_last_interaction_at(g, user.id),
        access_approved=is_access_approved(g, user),
        **user_dict,
    )


def is_access_approved(g: GitentialContext, user: UserInDB) -> Optional[bool]:
    if (not g.settings.features.enable_access_approval) or (user.is_active and user.is_admin):
        return True
    else:
        last_approval = get_last_approval_status_for_user(g, user.id)
        return last_approval.is_approved if last_approval else None


def get_last_approval_status_for_user(g: GitentialContext, user_id: int) -> Optional[AccessApprovalInDB]:
    ret: Optional[AccessApprovalInDB] = None
    for approval in g.backend.access_approvals.all():
        if approval.user_id == user_id and (
            ret is None or (approval.created_at and ret.created_at and ret.created_at < approval.created_at)
        ):
            ret = approval
    return ret


def admin_create_access_approval(
    g: GitentialContext, access_approval_create: AccessApprovalCreate, admin_user: UserInDB
) -> AccessApprovalInDB:
    access_approval_create.approved_by = admin_user.id
    access_approval_create.created_at = g.current_time()
    print(access_approval_create, access_approval_create.dict())
    return g.backend.access_approvals.create(access_approval_create)
