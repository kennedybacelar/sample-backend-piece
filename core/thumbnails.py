from structlog import get_logger

from gitential2.core import GitentialContext
from gitential2.datatypes.thumbnails import ThumbnailInDB, ThumbnailCreate, ThumbnailUpdate

logger = get_logger(__name__)


def get_thumbnail(g: GitentialContext, workspace_id: int, image_id: str) -> ThumbnailInDB:
    return g.backend.thumbnails.get_or_error(workspace_id=workspace_id, id_=image_id)


def create_thumbnail(g: GitentialContext, workspace_id: int, thumbnail_create: ThumbnailCreate) -> ThumbnailInDB:
    logger.info("creating thumbnail", workspace_id=workspace_id, title=thumbnail_create.id)
    return g.backend.thumbnails.create(workspace_id, thumbnail_create)


def update_thumbnail(
    g: GitentialContext, workspace_id: int, image_id: str, thumbnail_update: ThumbnailUpdate
) -> ThumbnailInDB:
    return g.backend.thumbnails.update(workspace_id, image_id, thumbnail_update)


def delete_thumbnail(g: GitentialContext, workspace_id: int, image_id: str) -> bool:
    delete_result = g.backend.thumbnails.delete(workspace_id=workspace_id, id_=image_id)
    if not delete_result:
        logger.info(f"Thumbnail delete failed! Not able to find thumbnail with id: {image_id}")
    return bool(delete_result)
