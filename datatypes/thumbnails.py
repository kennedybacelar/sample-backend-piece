from typing import Tuple, List

from gitential2.datatypes.common import ExtraFieldMixin, CoreModel, DateTimeModelMixin, StringIdModelMixin
from gitential2.datatypes.export import ExportableModel


class ThumbnailBase(StringIdModelMixin, ExtraFieldMixin, CoreModel):
    image: str


class ThumbnailPublic(DateTimeModelMixin, ThumbnailBase):
    pass


class ThumbnailCreate(ThumbnailBase):
    pass


class ThumbnailUpdate(ThumbnailBase):
    pass


class ThumbnailInDB(DateTimeModelMixin, ThumbnailBase, ExportableModel):
    def export_names(self) -> Tuple[str, str]:
        return "thumbnail", "thumbnails"

    def export_fields(self) -> List[str]:
        return [
            "id",
            "created_at",
            "updated_at",
            "extra",
            "image",
        ]
