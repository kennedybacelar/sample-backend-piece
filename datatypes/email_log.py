import datetime
from typing import Optional

# from enum import Enum
from .common import CoreModel, IDModelMixin, DateTimeModelMixin


# class EmailLogStatus(str, Enum):
#    scheduled = "scheduled"
#    sent = "sent"
#    canceled = "canceled"


# class EmailLogTemplate(str, Enum):
#    free_trial_expiration = "free_trial_expiration"
#    free_trial_ended = "free_trial_ended"
#    invite_member = "invite_member"
#    request_free_trial = "request_free_trial"
#    welcome = "welcome"


class EmailLogBase(CoreModel):
    user_id: int
    template_name: str
    status: str
    scheduled_at: datetime.datetime
    sent_at: Optional[datetime.datetime]


class EmailLogCreate(EmailLogBase):
    pass


class EmailLogUpdate(EmailLogBase):
    pass


class EmailLogInDB(IDModelMixin, DateTimeModelMixin, EmailLogBase):
    pass
