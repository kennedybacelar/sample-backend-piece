import random
import string
from typing import Optional
from datetime import datetime, timezone
from .common import CoreModel, StringIdModelMixin, DateTimeModelMixin


class ResellerCodeBase(StringIdModelMixin, DateTimeModelMixin, CoreModel):
    reseller_id: str
    expire_at: Optional[datetime] = None
    user_id: Optional[int] = None

    @property
    def code(self):
        return self.id_


class ResellerCode(ResellerCodeBase):
    pass


def generate_reseller_code(reseller_id: str, expire_at: Optional[datetime]) -> ResellerCode:
    return ResellerCode(
        id=_generate_random_hash(),
        reseller_id=reseller_id,
        expire_at=expire_at,
        created_at=datetime.utcnow().replace(tzinfo=timezone.utc),
        updated_at=datetime.utcnow().replace(tzinfo=timezone.utc),
    )


def _generate_random_hash(size: int = 10) -> str:
    return "".join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(size))
