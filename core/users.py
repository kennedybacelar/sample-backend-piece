from datetime import datetime, timedelta
from typing import Iterable, Optional, Tuple, cast, List

from structlog import get_logger

from gitential2.core.workspace_common import create_workspace
from gitential2.datatypes.credentials import CredentialCreate, CredentialUpdate
from gitential2.datatypes.subscriptions import (
    SubscriptionInDB,
    SubscriptionCreate,
)
from gitential2.datatypes.userinfos import UserInfoCreate, UserInfoUpdate
from gitential2.datatypes.users import UserCreate, UserRegister, UserUpdate, UserInDB, InactiveUsers
from gitential2.datatypes.workspacemember import WorkspaceRole
from gitential2.datatypes.workspaces import WorkspaceCreate
from .context import GitentialContext
from .emails import send_email_to_user, send_system_notification_email
from .reseller_codes import validate_reseller_code
from ..datatypes.cli_v2 import CacheRefreshType
from ..exceptions import SettingsException

logger = get_logger(__name__)


def handle_authorize(
    g: GitentialContext, integration_name: str, token, user_info: dict, current_user: Optional[UserInDB] = None
):
    integration = g.integrations[integration_name]

    # normalize the userinfo
    normalized_userinfo: UserInfoCreate = integration.normalize_userinfo(user_info, token=token)

    # update or create a user and the proper user_info in backend
    user, user_info, is_new_user = _create_or_update_user_and_user_info(g, normalized_userinfo, current_user)

    # update or create credentials based on integration and user
    _create_or_update_credential_from(g, user, integration_name, integration.integration_type, token)

    # Create workspace if missing
    _create_primary_workspace_if_missing(g, user)
    return {"ok": True, "user": user, "user_info": user_info, "is_new_user": is_new_user}


def _calc_user_login(user_create: UserCreate):
    if user_create.first_name and user_create.last_name:
        return " ".join([user_create.first_name, user_create.last_name])
    else:
        return user_create.email.split("@")[0]


def register_user(
    g: GitentialContext, registration: UserRegister, current_user: Optional[UserInDB] = None
) -> Tuple[UserInDB, Optional[SubscriptionInDB]]:
    user = UserCreate(**registration.dict(exclude={"reseller_id, reseller_code"}))
    reseller_id, reseller_code = validate_reseller_code(g, registration.reseller_id, registration.reseller_code)

    if not current_user:
        existing_user = g.backend.users.get_by_email(user.email)
        if existing_user:
            raise ValueError("Email already used.")
        user.registration_ready = True
        user.login = _calc_user_login(user)
        user_in_db = g.backend.users.create(user)
        subscription = _create_default_subscription_after_reg(g, user_in_db, reseller_id, reseller_code)
        _create_primary_workspace_if_missing(g, user_in_db)
    else:
        user.registration_ready = True
        user.login = _calc_user_login(user)
        user_in_db = g.backend.users.update(current_user.id, cast(UserUpdate, user))
        subscription = _create_default_subscription_after_reg(g, user_in_db, reseller_id, reseller_code)
    if g.license.is_cloud and subscription:
        send_email_to_user(g, user_in_db, template_name="welcome")
        if g.settings.notifications.request_free_trial:
            send_system_notification_email(g, user_in_db, template_name="request_free_trial")
    if reseller_id and reseller_code:
        g.backend.reseller_codes.set_user_id(reseller_id, reseller_code, user_in_db.id)
    return user_in_db, subscription


def get_user(g: GitentialContext, user_id: int) -> Optional[UserInDB]:
    user = g.backend.users.get(user_id)
    if user and user.is_active:
        return user
    return None


def update_user(g: GitentialContext, user_id: int, user_update: UserUpdate):
    user_from_db = g.backend.users.get_or_error(user_id)
    # Only the email, first_name, last_name, company_name, position can be modified
    user = UserUpdate(
        login=user_from_db.login,
        email=user_update.email,
        is_admin=user_from_db.is_admin,
        marketing_consent_accepted=user_from_db.marketing_consent_accepted,
        first_name=user_update.first_name,
        last_name=user_update.last_name,
        company_name=user_update.company_name,
        position=user_update.position,
        development_team_size=user_from_db.development_team_size,
        registration_ready=user_from_db.registration_ready,
        login_ready=user_from_db.login_ready,
        is_active=user_from_db.is_active,
        stripe_customer_id=user_from_db.stripe_customer_id,
    )
    return g.backend.users.update(user_id, user)


def deactivate_user(g: GitentialContext, user_id: int):
    user = g.backend.users.get(id_=user_id)
    if user:
        return g.backend.deactivate_user(user_id=user_id)
    raise SettingsException(f"Provided user_id=[{user_id}] is invalid!")


def purge_user_from_database(g: GitentialContext, user_id: int) -> bool:
    user = g.backend.users.get(id_=user_id)
    if user:
        result = g.backend.purge_user_from_database(user_id=user_id)
        reset_cache_for_user(g=g, reset_type=CacheRefreshType.everything, user_id=user_id)
        return result
    raise SettingsException(f"Provided user_id=[{user_id}] is invalid!")


def get_users_ready_for_purging(g: GitentialContext):
    exp_days_deactivation = g.backend.settings.cleanup.exp_days_after_user_deactivation
    exp_days_last_login = g.backend.settings.cleanup.exp_days_since_user_last_login

    get_inactive_users_query = (
        "WITH user_selection AS "
        "    (SELECT u.id            AS user_id, "
        "        u.is_active, "
        "        u.login, "
        "        u.email, "
        "        u.first_name, "
        "        u.last_name, "
        "        MAX(a.log_time) AS last_login "
        "    FROM users u "
        "        LEFT JOIN access_log AS a ON u.id = a.user_id "
        "    WHERE u.id NOT IN (SELECT DISTINCT sub.user_id "
        "                       FROM public.subscriptions AS sub "
        "                       WHERE (sub.subscription_type = 'professional' AND "
        "                              (sub.subscription_end IS NULL OR sub.subscription_end >= NOW()))) "
        "    GROUP BY u.id, u.email, u.first_name, u.last_name, u.login) "
        "SELECT DISTINCT us.user_id, us.is_active, us.email, us.first_name, us.last_name, us.login, us.last_login "
        "FROM user_selection AS us "
        f"WHERE (us.is_active IS FALSE AND us.last_login < NOW() - INTERVAL '{exp_days_deactivation} days') "
        f"    OR (us.last_login < NOW() - INTERVAL '{exp_days_last_login} days'); "
    )

    engine = g.backend.users.engine  # type: ignore[attr-defined]
    with engine.connect().execution_options() as conn:
        rows = conn.execute(get_inactive_users_query).fetchall()

    results = [InactiveUsers(**row) for row in rows]

    return results


def reset_cache_for_user(g: GitentialContext, reset_type: CacheRefreshType, user_id: int):
    integration_types: List[str] = list({i.integration_type for i in g.integrations.values()})

    if reset_type in [CacheRefreshType.everything, CacheRefreshType.repos]:
        delete_count_r: int = g.backend.user_repositories_cache.delete_cache_for_user(user_id=user_id)
        for integration_type in integration_types:
            g.kvstore.delete_value(
                name=f"repository_cache_for_user_last_refresh_datetime--{integration_type}--{user_id}"
            )
        logger.info(
            "Repos cache for user deleted.",
            number_of_deleted_rows=delete_count_r,
            user_id=user_id,
        )
    if reset_type in [CacheRefreshType.everything, CacheRefreshType.its_projects]:
        delete_count_its: int = g.backend.user_its_projects_cache.delete_cache_for_user(user_id=user_id)
        for integration_type in integration_types:
            g.kvstore.delete_value(name=f"itsp_cache_for_user_last_refresh_datetime--{integration_type}--{user_id}")
        logger.info(
            "ITS Projects cache for user deleted.",
            number_of_deleted_rows=delete_count_its,
            user_id=user_id,
        )


def get_profile_picture(g: GitentialContext, user: UserInDB) -> Optional[str]:
    user_infos = [
        user_info for user_info in g.backend.user_infos.get_for_user(user.id) if user_info.picture is not None
    ]
    if user_infos:
        user_infos_sorted = sorted(user_infos, key=lambda ui: ui.updated_at or datetime.utcnow())
        return user_infos_sorted[-1].picture
    else:
        return None


def _create_or_update_user_and_user_info(
    g: GitentialContext, normalized_userinfo: UserInfoCreate, current_user: Optional[UserInDB] = None
):
    existing_userinfo = g.backend.user_infos.get_by_sub_and_integration(
        sub=normalized_userinfo.sub, integration_name=normalized_userinfo.integration_name
    )
    if existing_userinfo:
        if current_user and existing_userinfo.user_id != current_user.id:
            raise ValueError("Authentication error, this user's credential already existing in our system")

        user = g.backend.users.get_or_error(existing_userinfo.user_id)

        user_update = user.copy()
        user_update.login_ready = True
        user_update.is_active = True
        user = g.backend.users.update(user.id, cast(UserUpdate, user_update))

        user_info = g.backend.user_infos.update(existing_userinfo.id, cast(UserInfoUpdate, normalized_userinfo))
        return user, user_info, False
    else:
        existing_user = current_user or (
            g.backend.users.get_by_email(normalized_userinfo.email) if normalized_userinfo.email else None
        )
        if existing_user:
            user_update = existing_user.copy()
            user_update.login_ready = True
            user_update.is_active = True
            user = g.backend.users.update(existing_user.id, cast(UserUpdate, user_update))
            is_new_user = False
        else:
            new_user = UserCreate.from_user_info(normalized_userinfo)
            new_user.login_ready = True
            user = g.backend.users.create(new_user)
            is_new_user = True
        user_info_data = normalized_userinfo.dict(exclude_none=True)
        user_info_data.setdefault("user_id", user.id)
        user_info = g.backend.user_infos.create(normalized_userinfo.copy(update={"user_id": user.id}))
        return user, user_info, is_new_user


def _create_or_update_credential_from(
    g: GitentialContext, user: UserInDB, integration_name: str, integration_type: str, token: dict
):
    new_credential = CredentialCreate.from_token(
        token=token,
        fernet=g.fernet,
        owner_id=user.id,
        integration_name=integration_name,
        integration_type=integration_type,
    )

    existing_credential = g.backend.credentials.get_by_user_and_integration(
        owner_id=user.id, integration_name=integration_name
    )
    if existing_credential:
        g.backend.credentials.update(id_=existing_credential.id, obj=CredentialUpdate(**new_credential.dict()))
    else:
        g.backend.credentials.create(new_credential)


def _create_primary_workspace_if_missing(g: GitentialContext, user: UserInDB):
    existing_workspace_memberships = g.backend.workspace_members.get_for_user(user_id=user.id)
    has_primary = any(ewm.role == WorkspaceRole.owner for ewm in existing_workspace_memberships)
    if not has_primary:
        workspace = WorkspaceCreate(name=f"{user.login}'s workspace")
        create_workspace(g, workspace=workspace, current_user=user, primary=True)


def _create_default_subscription_after_reg(
    g: GitentialContext, user, reseller_id: Optional[str], reseller_code: Optional[str]
) -> Optional[SubscriptionInDB]:
    subscriptions = g.backend.subscriptions.get_subscriptions_for_user(user.id)
    if subscriptions:
        return None
    else:
        # TEMPORARY DISABLED
        # _schedule_marketing_emails(g, user)
        return g.backend.subscriptions.create(
            SubscriptionCreate.default_for_new_user(user.id, reseller_id, reseller_code)
        )


def _schedule_marketing_emails(g: GitentialContext, user: UserInDB):
    g.backend.email_log.schedule_email(
        user_id=user.id,
        template_name="getting_started_first_steps",
        scheduled_at=(datetime.utcnow() + timedelta(days=1)),
    )
    g.backend.email_log.schedule_email(
        user_id=user.id,
        template_name="getting_started_metrics",
        scheduled_at=(datetime.utcnow() + timedelta(days=3)),
    )
    g.backend.email_log.schedule_email(
        user_id=user.id,
        template_name="getting_started_use_case_library",
        scheduled_at=(datetime.utcnow() + timedelta(days=5)),
    )
    g.backend.email_log.schedule_email(
        user_id=user.id,
        template_name="getting_started_level_up_your_developers",
        scheduled_at=(datetime.utcnow() + timedelta(days=9)),
    )
    g.backend.email_log.schedule_email(
        user_id=user.id,
        template_name="getting_started_best_coding_practices",
        scheduled_at=(datetime.utcnow() + timedelta(days=11)),
    )
    g.backend.email_log.schedule_email(
        user_id=user.id,
        template_name="getting_started_explore_more_as_a_professional",
        scheduled_at=(datetime.utcnow() + timedelta(days=13)),
    )
    g.backend.email_log.schedule_email(
        user_id=user.id, template_name="free_trial_expiration", scheduled_at=(datetime.utcnow() + timedelta(days=7))
    )
    # TEMPORARY DISABLED
    # g.backend.email_log.schedule_email(
    #     user_id=user.id, template_name="free_trial_ended", scheduled_at=(datetime.utcnow() + timedelta(days=14))
    # )


def set_as_admin(g: GitentialContext, user_id: int, is_admin: bool = True) -> UserInDB:
    user = g.backend.users.get_or_error(user_id)
    user_update = UserUpdate(**user.dict())
    user_update.is_admin = is_admin
    if is_admin:
        logger.info(f"Setting user {user.login}(id: {user.id}) to admin")
    else:
        logger.info(f"Setting user {user.login}(id: {user.id}) to non-admin")
    return g.backend.users.update(user_id, user_update)


def list_users(g: GitentialContext) -> Iterable[UserInDB]:
    return g.backend.users.all()


def send_trial_end_soon_emails(g: GitentialContext, user_id: int):
    send_email_to_user(g, user=g.backend.users.get_or_error(user_id), template_name="free_trial_expiration")


def send_trial_ended_emails(g: GitentialContext, user_id: int):
    send_email_to_user(g, user=g.backend.users.get_or_error(user_id), template_name="free_trial_ended")


def send_getting_started_emails(g: GitentialContext, user_id: int, template_name):
    send_email_to_user(g, user=g.backend.users.get_or_error(user_id), template_name=template_name)
