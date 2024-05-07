from datetime import datetime
from typing import Optional

from .common import CoreModel, ExtraFieldMixin, StringIdModelMixin, DateTimeModelMixin


class PersonalAccessToken(StringIdModelMixin, ExtraFieldMixin, CoreModel):
    user_id: int
    name: str
    expire_at: Optional[datetime] = None


class WorkspaceAPIKey(StringIdModelMixin, ExtraFieldMixin, CoreModel, DateTimeModelMixin):
    workspace_id: int
