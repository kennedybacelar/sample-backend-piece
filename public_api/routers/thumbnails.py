from fastapi import APIRouter, Depends
from structlog import get_logger

from gitential2.core import GitentialContext, check_permission
from gitential2.public_api.dependencies import gitential_context
from ..dependencies import current_user
from ...core.thumbnails import get_thumbnail, create_thumbnail, update_thumbnail, delete_thumbnail
from ...datatypes.permissions import Entity, Action
from ...datatypes.thumbnails import ThumbnailUpdate, ThumbnailPublic, ThumbnailCreate

router = APIRouter(tags=["thumbnails"])

logger = get_logger(__name__)


@router.get("/workspaces/{workspace_id}/thumbnails/{image_id}", response_model=ThumbnailPublic)
def get_thumbnail_(
    workspace_id: int,
    image_id: str,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.thumbnail, Action.read, workspace_id=workspace_id)
    return get_thumbnail(g, workspace_id, image_id)


@router.post("/workspaces/{workspace_id}/thumbnails", response_model=ThumbnailPublic)
def create_thumbnail_(
    workspace_id: int,
    thumbnail_create: ThumbnailCreate,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.thumbnail, Action.read, workspace_id=workspace_id)
    return create_thumbnail(g, workspace_id=workspace_id, thumbnail_create=thumbnail_create)


@router.put("/workspaces/{workspace_id}/thumbnails/{image_id}", response_model=ThumbnailPublic)
def update_thumbnail_(
    workspace_id: int,
    image_id: str,
    thumbnail_update: ThumbnailUpdate,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.thumbnail, Action.read, workspace_id=workspace_id)
    return update_thumbnail(g, workspace_id, image_id, thumbnail_update)


@router.delete("/workspaces/{workspace_id}/thumbnails/{image_id}", response_model=bool)
def delete_thumbnail_(
    workspace_id: int,
    image_id: str,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.thumbnail, Action.read, workspace_id=workspace_id)
    return delete_thumbnail(g, workspace_id, image_id)
