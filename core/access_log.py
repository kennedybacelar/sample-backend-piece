from typing import Optional
from datetime import datetime
from structlog import get_logger

from gitential2.datatypes.access_log import AccessLog

from .context import GitentialContext


logger = get_logger(__name__)


def create_access_log(
    g: GitentialContext,
    user_id: int,
    path: str,
    method: str,
    ip_address: Optional[str],
    log_time: Optional[datetime] = None,
    **extra,
) -> Optional[AccessLog]:
    if g.settings.web.access_log:
        logger.info(
            "access log",
            user_id=user_id,
            ip_address=ip_address,
            path=path,
            method=method,
            log_time=log_time,
            extra=extra,
        )
        return g.backend.access_logs.create(
            AccessLog(
                user_id=user_id,
                path=path,
                method=method,
                ip_address=ip_address,
                log_time=log_time or datetime.utcnow(),
                extra=extra,
            )
        )
    return None


def get_last_interaction_at(g: GitentialContext, user_id: int) -> Optional[datetime]:
    last_interaction = g.backend.access_logs.last_interaction(user_id)
    if last_interaction:
        return last_interaction.log_time
    else:
        return None
