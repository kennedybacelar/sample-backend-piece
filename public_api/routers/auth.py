import asyncio
from typing import Optional
from structlog import get_logger
from fastapi import APIRouter, Request, HTTPException, Depends, Header
from fastapi.responses import RedirectResponse
from gitential2.datatypes.users import UserRegister
from gitential2.datatypes.subscriptions import SubscriptionType
from gitential2.exceptions import AuthenticationException
from gitential2.core.context import GitentialContext
from gitential2.core.users import handle_authorize, register_user, get_profile_picture
from gitential2.core.subscription import get_current_subscription
from gitential2.core.admin import is_access_approved
from gitential2.core.quick_login import get_quick_login_user

from ..dependencies import gitential_context, OAuth, current_user, verify_recaptcha_token, api_access_log

logger = get_logger(__name__)


router = APIRouter()


async def _get_token(request, remote, code, id_token, oauth_verifier):
    if code:
        token = await remote.authorize_access_token(request)
        logger.debug("has code", token=token)
        if id_token:
            token["id_token"] = id_token
    elif id_token:
        token = {"id_token": id_token}
    elif oauth_verifier:
        # OAuth 1
        token = await remote.authorize_access_token(request)
    else:
        # handle failed
        raise AuthenticationException("failed to get token")
    return token


async def _get_user_info(request, remote, token):
    if "id_token" in token:
        user_info = await remote.parse_id_token(request, token)
    else:
        remote.token = token
        try:
            if "token_type" in token and token["token_type"] == "jwt-bearer":  # VSTS fix
                token["token_type"] = "bearer"
            user_info = await remote.userinfo(token=token)
        except Exception as e:
            logger.exception("error getting user_info", remote=remote, token=token)

            raise e
    return user_info


# pylint: disable=too-many-arguments
@router.get("/auth/{backend}")
async def auth(
    backend: str,
    request: Request,
    id_token: Optional[str] = None,
    code: Optional[str] = None,
    oauth_verifier: Optional[str] = None,
    g: GitentialContext = Depends(gitential_context),
    oauth: OAuth = Depends(),
    current_user=Depends(current_user),
):
    remote = oauth.create_client(backend)
    integration = g.integrations.get(backend)

    if remote is None or integration is None:
        raise HTTPException(404)

    try:
        token = await _get_token(request, remote, code, id_token, oauth_verifier)
        user_info = await _get_user_info(request, remote, token)

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, handle_authorize, g, integration.name, token, user_info, current_user)

        request.session["current_user_id"] = result["user"].id

        redirect_uri = request.session.get("redirect_uri")
        if redirect_uri:
            return RedirectResponse(url=redirect_uri)
        else:
            return result
    except Exception as e:  # pylint: disable=broad-except
        logger.exception("Error during authentication")
        raise AuthenticationException("error during authentication") from e


@router.get("/login/{backend}")
async def login(
    backend: str,
    request: Request,
    oauth: OAuth = Depends(),
    referer: Optional[str] = Header(None),
    redirect_after: Optional[str] = None,
):
    remote = oauth.create_client(backend)
    request.session["redirect_uri"] = redirect_after or referer

    if remote is None:
        raise HTTPException(404)

    redirect_uri = _calculate_oauth_redirect_uri(request, backend)
    return await remote.authorize_redirect(request, redirect_uri)


# pylint: disable=else-if-used
def _calculate_oauth_redirect_uri(request: Request, backend: str) -> str:
    if request.app.state.settings.web.enforce_base_url:
        base_url: str = request.app.state.settings.web.base_url.rstrip("/")
        if request.app.state.settings.web.legacy_login:
            if backend == "vsts":
                redirect_uri = base_url + "/login"
            else:
                redirect_uri = base_url + "/login" + f"?source={backend}"
        else:
            redirect_uri = base_url + f"/v2/auth/{backend}"
    else:
        if request.app.state.settings.web.legacy_login:
            if backend == "vsts":
                redirect_uri = request.url_for("legacy_login")
            else:
                redirect_uri = request.url_for("legacy_login") + f"?source={backend}"
        else:
            redirect_uri = request.url_for("auth", backend=backend)
    return redirect_uri


@router.get("/logout")
async def logout(request: Request):
    if "current_user_id" in request.session:
        del request.session["current_user_id"]

    return {}


@router.get("/session")
def session(
    request: Request,
    g: GitentialContext = Depends(gitential_context),
    current_user=Depends(current_user),
    api_access_log=Depends(api_access_log),
):  # pylint: disable=unused-argument
    if current_user:
        user_in_db = current_user
        if user_in_db:
            # registration_ready = license_.is_on_premises or request.session.get("registration_ready", False)
            subscription = get_current_subscription(g, user_in_db.id)
            access_approved = is_access_approved(g, user_in_db)
            return {
                "user_id": user_in_db.id,
                "login": user_in_db.login,
                "marketing_consent_accepted": user_in_db.marketing_consent_accepted,
                "subscription_details": subscription,
                "subscription": SubscriptionType.professional
                if subscription.subscription_type in [SubscriptionType.trial, SubscriptionType.professional]
                else subscription.subscription_type,
                "registration_ready": user_in_db.registration_ready,
                "login_ready": user_in_db.login_ready,
                "profile_picture": get_profile_picture(g, user_in_db),
                "access_approved": access_approved,
            }
    return {}


@router.post("/registration")
def registration(
    registration_data: UserRegister,
    request: Request,
    g: GitentialContext = Depends(gitential_context),
    verify_recaptcha_token=Depends(verify_recaptcha_token),  # pylint: disable=unused-argument
):

    current_user_id = request.session.get("current_user_id")
    current_user = g.backend.users.get(current_user_id) if current_user_id else None

    user, _ = register_user(g, registration_data, current_user=current_user)
    request.session["current_user_id"] = user.id
    return {}


@router.get("/quick-login/{login_hash}")
def quick_login(
    login_hash: str,
    request: Request,
    g: GitentialContext = Depends(gitential_context),
):
    user = get_quick_login_user(g, login_hash)
    if user:
        request.session["current_user_id"] = user.id
    return RedirectResponse(url="/")
