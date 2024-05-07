from typing import Optional
from uuid import uuid4

from authlib.integrations.starlette_client import OAuth
from fastapi import FastAPI
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import ValidationError
from starlette.middleware.sessions import SessionMiddleware
from structlog import get_logger

from gitential2.core.context import init_context_from_settings
from gitential2.core.tasks import configure_celery
from gitential2.exceptions import AuthenticationException, NotFoundException, PermissionException
from gitential2.logging import initialize_logging
from gitential2.settings import GitentialSettings, load_settings
from .routers import (
    ping,
    configuration,
    workspaces,
    projects,
    teams,
    repositories,
    stats,
    auth,
    users,
    authors,
    legacy,
    commits_and_prs,
    payment,
    invitations,
    its,
    data_queries,
    admin,
    deploys,
    dashboards,
    charts,
    thumbnails,
)
from ..datatypes.middlewares import ClickjackingMiddleware

logger = get_logger(__name__)


def create_app(settings: Optional[GitentialSettings] = None):
    app = FastAPI(title="Gitential REST API", version="2.1.0")
    settings = settings or load_settings()
    initialize_logging(settings)
    app.state.settings = settings
    _configure_celery(settings)
    _configure_cors(app)
    _configure_routes(app)
    _configure_session(app, settings)
    _configure_gitential_core(app, settings)
    _configure_oauth_authentication(app)
    _configure_error_handling(app)

    return app


def _configure_celery(settings: GitentialSettings):
    configure_celery(settings)


def _configure_cors(app: FastAPI):
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=".*",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(ClickjackingMiddleware)


def _configure_routes(app: FastAPI):
    app.include_router(
        legacy.router,
    )
    for router in [
        ping.router,
        configuration.router,
        workspaces.router,
        projects.router,
        teams.router,
        authors.router,
        repositories.router,
        stats.router,
        auth.router,
        users.router,
        payment.router,
        commits_and_prs.router,
        invitations.router,
        its.router,
        data_queries.router,
        admin.router,
        deploys.router,
        dashboards.router,
        charts.router,
        thumbnails.router,
    ]:
        app.include_router(router, prefix="/v2")


def _configure_session(app: FastAPI, settings: GitentialSettings):
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.secret,
        session_cookie=settings.web.session_cookie,
        same_site=settings.web.session_same_site,
        https_only=settings.web.session_https_only,
        max_age=settings.web.session_max_age,
    )


def _configure_gitential_core(app: FastAPI, settings: GitentialSettings):
    app.state.gitential = init_context_from_settings(settings)


def _configure_oauth_authentication(app: FastAPI):
    oauth = OAuth()
    for integration in app.state.gitential.integrations.values():
        if integration.is_oauth:
            oauth.register(name=integration.name, **integration.oauth_register())
            logger.debug(
                "registering oauth app", integration_name=integration.name, options=integration.oauth_register()
            )
    app.state.oauth = oauth


def _error_page(request, error_code):
    redirect_uri = (request.session.get("redirect_uri") or request.app.state.settings.web.base_url).rstrip("/")
    return redirect_uri + f"/error?code={error_code}"


def _configure_error_handling(app: FastAPI):
    @app.exception_handler(ValidationError)
    async def validation_exception_handler(request, exc):
        print(f"OMG! The client sent invalid data!: {exc}")
        return await request_validation_exception_handler(request, exc)

    @app.exception_handler(500)
    async def custom_http_exception_handler(request, exc):
        error_code = uuid4()

        def _log_exc():
            logger.exception(
                "Internal server error",
                exc=exc,
                error_code=error_code,
                headers=request.headers,
                method=request.method,
                url=request.url,
            )

        if isinstance(exc, AuthenticationException):
            _log_exc()
            return RedirectResponse(url=_error_page(request, error_code))
        elif isinstance(exc, PermissionException):
            response = JSONResponse(content={"error": str(exc)}, status_code=403)
        elif isinstance(exc, NotFoundException):
            response = JSONResponse(content={"error": str(exc)}, status_code=404)
        else:
            _log_exc()
            response = JSONResponse(content={"error": "Something went wrong"}, status_code=500)

        # Since the CORSMiddleware is not executed when an unhandled server exception
        # occurs, we need to manually set the CORS headers ourselves if we want the FE
        # to receive a proper JSON 500, opposed to a CORS error.
        # Setting CORS headers on server errors is a bit of a philosophical topic of
        # discussion in many frameworks, and it is currently not handled in FastAPI.
        # See dotnet core for a recent discussion, where ultimately it was
        # decided to return CORS headers on server failures:
        # https://github.com/dotnet/aspnetcore/issues/2378
        origin = request.headers.get("origin")

        if origin:
            # Have the middleware do the heavy lifting for us to parse
            # all the config, then update our response headers
            cors = CORSMiddleware(
                app=app, allow_origin_regex=".*", allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
            )

            # Logic directly from Starlette's CORSMiddleware:
            # https://github.com/encode/starlette/blob/master/starlette/middleware/cors.py#L152

            response.headers.update(cors.simple_headers)
            has_cookie = "cookie" in request.headers

            # If request includes any cookie headers, then we must respond
            # with the specific origin instead of '*'.
            if cors.allow_all_origins and has_cookie:
                response.headers["Access-Control-Allow-Origin"] = origin

            # If we only allow specific origins, then we have to mirror back
            # the Origin header in the response.
            elif not cors.allow_all_origins and cors.is_allowed_origin(origin=origin):
                response.headers["Access-Control-Allow-Origin"] = origin
                response.headers.add_vary_header("Origin")

        return response

    return validation_exception_handler, custom_http_exception_handler
