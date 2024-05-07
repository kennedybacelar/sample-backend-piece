import datetime
from typing import Optional
from enum import Enum
from .common import CoreModel, IDModelMixin, DateTimeModelMixin


class SubscriptionType(str, Enum):
    trial = "trial"
    professional = "professional"
    free = "free"


class SubscriptionBase(CoreModel):
    user_id: int
    subscription_start: datetime.datetime
    subscription_end: Optional[datetime.datetime]
    subscription_type: SubscriptionType = SubscriptionType.trial
    number_of_developers: int = 5
    stripe_subscription_id: Optional[str]
    features: Optional[dict] = None


class CreateCheckoutSession(CoreModel):
    number_of_developers: int
    is_monthly: bool


class SubscriptionCreate(SubscriptionBase):
    @classmethod
    def default_for_new_user(cls, user_id, reseller_id, reseller_code):
        if reseller_id and reseller_code:
            return cls(
                user_id=user_id,
                subscription_start=datetime.datetime.utcnow(),
                subscription_end=None,
                subscription_type=SubscriptionType.professional,
                number_of_developers=20,
                features={"reseller_id": reseller_id},
            )
        else:
            return cls(
                user_id=user_id,
                subscription_start=datetime.datetime.utcnow(),
                subscription_end=datetime.datetime.utcnow() + datetime.timedelta(days=14),
                subscription_type=SubscriptionType.trial,
                number_of_developers=5,
                features=None,
            )


class SubscriptionUpdate(SubscriptionBase):
    pass


class SubscriptionInDB(IDModelMixin, DateTimeModelMixin, SubscriptionBase):
    pass


class SubscriptionPublic(IDModelMixin, DateTimeModelMixin, SubscriptionBase):
    pass
