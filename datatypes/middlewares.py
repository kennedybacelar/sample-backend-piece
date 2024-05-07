from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request


class ClickjackingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        # process the request and get the response
        response = await call_next(request)
        response.headers["X-Frame-Options"] = "DENY"
        return response
