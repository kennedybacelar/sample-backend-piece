from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import HTTPException

from gitential2.core.context import GitentialContext
from gitential2.core.users import update_user, deactivate_user, purge_user_from_database, get_users_ready_for_purging
from gitential2.datatypes.users import UserPublic, UserUpdate
from ..dependencies import gitential_context, current_user

router = APIRouter(tags=["users"])


@router.get("/users")
def list_users():
    pass


@router.get("/users/me", response_model=UserPublic)
def get_current_user(
    current_user=Depends(current_user),
):

    if current_user:
        return current_user
    raise HTTPException(404, "User not found.")


@router.get("/users/is-admin")
async def is_admin(
    current_user=Depends(current_user),
):
    return {"is_admin": current_user and current_user.is_admin}


@router.post("/users/me", response_model=UserPublic)
def update_current_user(
    user_update: UserUpdate,
    g: GitentialContext = Depends(gitential_context),
    current_user=Depends(current_user),
):

    if current_user:
        return update_user(g, user_id=current_user.id, user_update=user_update)
    else:
        raise HTTPException(404, "User not found.")


@router.post("/users/me/deactivate")
def deactivate_current_user(
    request: Request,
    g: GitentialContext = Depends(gitential_context),
    current_user=Depends(current_user),
):
    if current_user:
        deactivate_user(g, user_id=current_user.id)
        del request.session["current_user_id"]
        return True
    else:
        raise HTTPException(404, "User not found.")


@router.delete("/users/me")
def delete_current_user(
    request: Request,
    g: GitentialContext = Depends(gitential_context),
    current_user=Depends(current_user),
):
    if current_user:
        result: bool = purge_user_from_database(g, user_id=current_user.id)
        if result:
            del request.session["current_user_id"]
            return True
        else:
            raise HTTPException(500, "Error occurred while trying to delete user.")
    else:
        raise HTTPException(404, "User not found.")


@router.delete("/users")
def delete_user_by_id(
    request: Request,
    user_id: int,
    g: GitentialContext = Depends(gitential_context),
    current_user=Depends(current_user),
):
    if current_user:
        if current_user.is_admin:
            result: bool = purge_user_from_database(g, user_id=user_id)
            if result:
                del request.session["current_user_id"]
                return True
            else:
                raise HTTPException(500, "Error occurred while trying to delete user.")
        else:
            raise HTTPException(401, "Unauthorized! User is not an admin!")
    else:
        raise HTTPException(404, "User not found.")


@router.get("/users/inactive-users")
def get_inactive_users(g: GitentialContext = Depends(gitential_context), current_user=Depends(current_user)):
    if current_user:
        if current_user.is_admin:
            return get_users_ready_for_purging(g=g)
        else:
            raise HTTPException(401, "Unauthorized! User is not an admin!")
    else:
        raise HTTPException(404, "User not found.")
