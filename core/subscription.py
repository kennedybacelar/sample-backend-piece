from typing import Optional, cast
from datetime import datetime, timedelta
from xmlrpc.client import Boolean

from structlog import get_logger

from gitential2.datatypes.stats import Query

from gitential2.datatypes.subscriptions import (
    SubscriptionInDB,
    SubscriptionCreate,
    SubscriptionType,
    SubscriptionUpdate,
)
from gitential2.datatypes.stats import FilterName
from gitential2.utils import deep_merge_dicts
from .context import GitentialContext


logger = get_logger(__name__)
TRIAL_FILTER_PERIOD_DAY = 90


def is_free_user(g: GitentialContext, user_id: int):
    sub = get_current_subscription(g, user_id)
    if sub.subscription_type == SubscriptionType.trial:
        return True
    return False


def get_current_subscription(g: GitentialContext, user_id: int) -> SubscriptionInDB:
    if g.license.is_on_premises:
        features: dict = {}
        if g.settings.features.enable_its_analytics:
            features = deep_merge_dicts(features, {"jira": {"enabled": True}})
        else:
            features = deep_merge_dicts(features, {"jira": {"enabled": False}})
        return SubscriptionInDB(
            id=0,
            user_id=user_id,
            subscription_type=SubscriptionType.professional,
            subscription_start=datetime(1970, 1, 1),
            subscription_end=datetime(2099, 12, 31),
            features=features,
        )

    current_subscription_from_db = _get_current_subscription_from_db(g, user_id)
    if current_subscription_from_db:
        return _add_its_integration(g, current_subscription_from_db)
    else:
        return _add_its_integration(
            g,
            SubscriptionInDB(
                id=0,
                user_id=user_id,
                subscription_type=SubscriptionType.free,
                subscription_start=datetime.utcnow(),
            ),
        )


def _add_its_integration(g: GitentialContext, subscription: SubscriptionInDB):
    if g.settings.features.enable_its_analytics:
        features = subscription.features or {}
        subscription.features = deep_merge_dicts(features, {"jira": {"enabled": True}})
    return subscription


def _get_current_subscription_from_db(g: GitentialContext, user_id: int) -> Optional[SubscriptionInDB]:
    current_time = datetime.utcnow()

    def _is_subscription_valid(s: SubscriptionInDB):
        return s.subscription_start < current_time and (s.subscription_end is None or s.subscription_end > current_time)

    subscriptions = g.backend.subscriptions.get_subscriptions_for_user(user_id)

    valid_subscriptions = [s for s in subscriptions if _is_subscription_valid(s)]
    if valid_subscriptions:
        return valid_subscriptions[0]
    else:
        return None


def limit_filter_time(ws_id: int, query: Query) -> Query:
    min_allowed_dt = datetime.now() - timedelta(days=TRIAL_FILTER_PERIOD_DAY)
    if FilterName.day not in query.filters:
        query.filters[FilterName.day].append(
            [
                min_allowed_dt.strftime("%Y-%m-%d"),
                datetime.now().strftime("%Y-%m-%d"),
            ]
        )
        logger.debug("limiting query by adding filters", workspace_id=ws_id)
    else:
        filter_start_dt = datetime.strptime(query.filters[FilterName.day][0], "%Y-%m-%d")
        filter_end_dt = datetime.strptime(query.filters[FilterName.day][1], "%Y-%m-%d")
        if filter_start_dt < min_allowed_dt:
            query.filters[FilterName.day][0] = min_allowed_dt.strftime("%Y-%m-%d")
            logger.debug("limiting query, limit start filtertime", workspace_id=ws_id)
        if filter_end_dt < min_allowed_dt:
            query.filters[FilterName.day][1] = (min_allowed_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            logger.debug("limiting query, limit end filtertime", workspace_id=ws_id)

    return query


def set_as_free(g: GitentialContext, user_id: int, end_time: Optional[datetime] = None) -> Optional[bool]:
    user = g.backend.users.get_or_error(user_id)
    current_subs = _get_current_subscription_from_db(g, user_id=user.id)
    if current_subs and current_subs.subscription_type == SubscriptionType.professional:
        su = SubscriptionUpdate(**current_subs.dict())
        if end_time is not None:
            su.subscription_end = end_time
        else:
            su.subscription_end = datetime.utcnow()
        g.backend.subscriptions.update(current_subs.id, su)
        return True
    else:
        return False


def set_as_professional(
    g: GitentialContext, user_id: int, number_of_developers: int, stripe_event: Optional[dict] = None
) -> SubscriptionInDB:
    user = g.backend.users.get_or_error(user_id)
    current_subs = _get_current_subscription_from_db(g, user_id=user.id)

    if current_subs and current_subs.subscription_type == SubscriptionType.professional:
        su = SubscriptionUpdate(**current_subs.dict())
        su.number_of_developers = number_of_developers
        if (
            stripe_event
            and "cancel_at" in stripe_event["data"]["object"]
            and stripe_event["data"]["object"]["cancel_at"]
        ):
            su.subscription_end = datetime.utcfromtimestamp(int(stripe_event["data"]["object"]["cancel_at"]))
        new_sub = g.backend.subscriptions.update(current_subs.id, su)
    elif current_subs and current_subs.subscription_type != SubscriptionType.professional:
        su = SubscriptionUpdate(**current_subs.dict())
        su.subscription_end = datetime.utcnow()
        g.backend.subscriptions.update(current_subs.id, su)
        cancel_trial_emails(g, user_id)
        new_sub = _create_new_prof_subs(g, user_id, number_of_developers)
    else:
        new_sub = _create_new_prof_subs(g, user_id, number_of_developers)
    if (
        stripe_event
        and "subscription" in stripe_event["data"]["object"]["items"]["data"][0]
        and not new_sub.stripe_subscription_id
    ):
        new_sub.stripe_subscription_id = stripe_event["data"]["object"]["items"]["data"][0]["subscription"]
        g.backend.subscriptions.update(new_sub.id, cast(SubscriptionUpdate, new_sub))
    return new_sub


def _create_new_prof_subs(g: GitentialContext, user_id: int, number_of_developers: int) -> SubscriptionInDB:
    return g.backend.subscriptions.create(
        SubscriptionCreate(
            user_id=user_id,
            number_of_developers=number_of_developers,
            subscription_type=SubscriptionType.professional,
            subscription_start=datetime.utcnow(),
        )
    )


def cancel_trial_emails(g: GitentialContext, user_id: int):
    for template in ["free_trial_expiration", "free_trial_ended"]:
        g.backend.email_log.cancel_email(user_id, template)


def enable_or_disable_jira_integration(
    g: GitentialContext, user_id: int, enable: Boolean
) -> Optional[SubscriptionInDB]:
    user = g.backend.users.get_or_error(user_id)
    current_subs = _get_current_subscription_from_db(g, user_id=user.id)
    if current_subs:
        su = SubscriptionUpdate(**current_subs.dict())
        features = su.features or {}
        su.features = deep_merge_dicts(features, {"jira": {"enabled": enable}})
        return g.backend.subscriptions.update(current_subs.id, su)
    return None
