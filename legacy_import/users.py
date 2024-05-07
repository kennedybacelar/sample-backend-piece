from typing import List
from datetime import timedelta

from structlog import get_logger
from pydantic import ValidationError
from pydantic.datetime_parse import parse_datetime


from gitential2.datatypes.users import UserInDB
from gitential2.datatypes.userinfos import UserInfoCreate
from gitential2.datatypes.subscriptions import SubscriptionCreate, SubscriptionType

from gitential2.core import GitentialContext


logger = get_logger(__name__)


def import_legacy_users(g: GitentialContext, legacy_users: List[dict], users_json: dict):

    for legacy_user in legacy_users:
        _import_legacy_user(g, legacy_user, users_json)
    g.backend.users.reset_primary_key_id()


def _import_legacy_user(g: GitentialContext, legacy_user: dict, users_json: dict):
    info = users_json.get(str(legacy_user["id"]), {})
    try:
        user_create = UserInDB(
            id=legacy_user["id"],
            login=legacy_user["login"],
            email=info.get("Other email")
            or legacy_user["email"]
            or f"{legacy_user['login']}@gitential-missing-email.com",
            is_admin=bool(legacy_user["admin"]),
            login_ready=True,
            is_active=legacy_user["is_active"],
            created_at=legacy_user["created_at"],
            updated_at=legacy_user["updated_at"],
            first_name=info.get("first_name"),
            last_name=info.get("last_name"),
            company_name=info.get("company_name"),
        )
        logger.info("Importing user", user_id=user_create.id_, email=user_create.email)
        g.backend.users.insert(legacy_user["id"], user_create)
        _create_user_infos(g, legacy_user)
        _create_subscription(g, legacy_user)
    except ValidationError as e:
        print(f"Failed to import user {legacy_user['id']}", e)


def _create_user_infos(g, legacy_user: dict):
    if legacy_user["github_id"]:
        g.backend.user_infos.create(
            UserInfoCreate(
                user_id=legacy_user["id"],
                integration_name="github",
                integration_type="github",
                sub=str(legacy_user["github_id"]),
            )
        )
    if legacy_user["gitlab_id"]:
        g.backend.user_infos.create(
            UserInfoCreate(
                user_id=legacy_user["id"],
                integration_name="gitlab",
                integration_type="gitlab",
                sub=str(legacy_user["gitlab_id"]),
            )
        )

    if legacy_user["vsts_id"]:
        g.backend.user_infos.create(
            UserInfoCreate(
                user_id=legacy_user["id"],
                integration_name="vsts",
                integration_type="vsts",
                sub=str(legacy_user["vsts_id"]),
            )
        )

    if legacy_user["bitbucket_id"]:
        g.backend.user_infos.create(
            UserInfoCreate(
                user_id=legacy_user["id"],
                integration_name="bitbucket",
                integration_type="bitbucket",
                sub=str(legacy_user["bitbucket_id"]),
            )
        )


def _create_subscription(g: GitentialContext, legacy_user: dict):
    is_pro, num_of_developers = _is_paying_customer(legacy_user)
    registration_time = parse_datetime(legacy_user["created_at"])
    trial_end = registration_time + timedelta(days=14)
    subscription_create = SubscriptionCreate(
        user_id=legacy_user["id"],
        subscription_start=legacy_user["created_at"],
        subscription_end=trial_end if not is_pro else None,
        subscription_type=SubscriptionType.professional if is_pro else SubscriptionType.trial,
        number_of_developers=num_of_developers,
    )
    g.backend.subscriptions.create(subscription_create)


def _is_paying_customer(legacy_user: dict):
    paying_customers = {
        "chris.khoo@pocketpinata.com": 5,
        "janono@gmail.com": 150,
        "bill@tech9.com": 40,
        "f.sodano@wisr.com.au": 16,
        "jsudbury@vistek.ca": 4,
        "novan@qasir.id": 35,
        "mail@laszloandrasi.com": 100,
    }

    if legacy_user["email"] in paying_customers:
        return True, paying_customers[legacy_user["email"]]
    else:
        return False, 0
