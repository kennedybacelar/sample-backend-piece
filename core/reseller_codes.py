from typing import Optional, Tuple, List
from datetime import datetime, timezone
from pydantic import BaseModel, validator

from gitential2.datatypes.reseller_codes import ResellerCode, generate_reseller_code
from .context import GitentialContext


def validate_reseller_code(
    g: GitentialContext, reseller_id: Optional[str], reseller_code: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    class RC(BaseModel):
        reseller_id: str
        reseller_code: str

        @validator("reseller_code", allow_reuse=True)
        def rcode_validator(cls, rcode, values):
            reseller_code_obj = g.backend.reseller_codes.get(rcode)
            if (
                reseller_code_obj
                and reseller_code_obj.reseller_id == values["reseller_id"]
                and reseller_code_obj.user_id is None
                and (
                    reseller_code_obj.expire_at is None
                    or reseller_code_obj.expire_at.replace(tzinfo=timezone.utc) > g.current_time()
                )
            ):
                return rcode
            else:
                raise ValueError("Invalid reseller code")

    if (not reseller_id and not reseller_code) or (reseller_id in ["registration", "", None]):
        return None, None
    else:
        rc = RC(reseller_id=reseller_id, reseller_code=reseller_code)  # this is where the validation is happening
        return rc.reseller_id, rc.reseller_code


def generate_reseller_codes(
    g: GitentialContext, reseller_id: str, count: int = 1, expire_at: Optional[datetime] = None
) -> List[ResellerCode]:
    ret = []
    valid_reseller_ids = [r.reseller_id for r in g.settings.resellers or []]
    if reseller_id in valid_reseller_ids:
        for _ in range(count):
            ret.append(
                g.backend.reseller_codes.create(generate_reseller_code(reseller_id=reseller_id, expire_at=expire_at))
            )
        return ret
    else:
        raise ValueError(f"Invalid reselled_id: {reseller_id}")
