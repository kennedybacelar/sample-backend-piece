from fastapi import APIRouter
from fastapi import Request
from gitential2.integrations import IntegrationType
from gitential2.license import check_license
from gitential2.settings import GitentialSettings

router = APIRouter()


def _calculate_login_url(request: Request, backend: str) -> str:
    if request.app.state.settings.web.enforce_base_url:
        base_url = request.app.state.settings.web.base_url.rstrip("/")
        return base_url + f"/v2/login/{backend}"
    else:
        return request.url_for("login", backend=backend)


@router.get("/configuration")
def configuration(request: Request):
    license_ = check_license()
    gitential_settings: GitentialSettings = request.app.state.settings
    logins = {}
    sources = []
    frontend_settings = gitential_settings.frontend
    recaptcha_settings = gitential_settings.recaptcha
    integrations_settings = request.app.state.settings.integrations
    sorted_integration_settings = sorted(integrations_settings.items(), key=lambda x: x[1].login_order)
    for name, settings in sorted_integration_settings:
        if settings.login:
            display_name = settings.display_name or name
            logins[name] = {
                "login_text": settings.login_text or f"Login with {display_name}",
                "signup_text": settings.signup_text or f"Sign up with {display_name}",
                "login_top_text": settings.login_top_text or None,
                "type": settings.type_,
                "url": _calculate_login_url(request, name),
            }
        if settings.type_ not in [IntegrationType.linkedin, IntegrationType.dummy]:
            sources.append(
                {
                    "name": name,
                    "display_name": settings.display_name or name,
                    "type": settings.type_,
                    "url": _calculate_login_url(request, name),
                }
            )

    return {
        "maintenance": gitential_settings.maintenance,
        "license": license_.as_config(),
        "frontend": frontend_settings,
        "logins": logins,
        "recaptcha": {"site_key": recaptcha_settings.site_key},
        "sources": sources,
        "contacts": gitential_settings.contacts,
        "sentry": {"dsn": "https://dc5be4ac529146d68d723b5f5be5ae2d@sentry.io/1815669"},
        "debug": "False",
        "publishable_key": gitential_settings.stripe.publishable_key,
        "features": gitential_settings.features,
        "resellers": gitential_settings.resellers if gitential_settings.features.enable_resellers else None,
    }
