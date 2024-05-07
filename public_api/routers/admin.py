from typing import List
from structlog import get_logger
from fastapi import APIRouter, Depends
from gitential2.datatypes.access_approvals import AccessApprovalCreate, AccessApprovalInDB
from gitential2.datatypes.users import UserInAdminRepr
from gitential2.core.context import GitentialContext
from gitential2.core.permissions import check_is_admin
from gitential2.core.admin import admin_list_users, admin_create_access_approval

from ..dependencies import gitential_context, current_user

logger = get_logger(__name__)


router = APIRouter()


@router.get("/admin/users", response_model=List[UserInAdminRepr])
def list_users_(
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_is_admin(g, current_user)
    return admin_list_users(g)


@router.post("/admin/access-approvals", response_model=AccessApprovalInDB)
def access_approvals_(
    access_approval_create: AccessApprovalCreate,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_is_admin(g, current_user)
    return admin_create_access_approval(g, access_approval_create, current_user)
