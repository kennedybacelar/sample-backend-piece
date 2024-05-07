from structlog import get_logger
from gitential2.settings import GitentialSettings, BackendType
from .base import GitentialBackend
from .in_memory import InMemGitentialBackend
from .sql import SQLGitentialBackend


logger = get_logger(__name__)


def init_backend(settings: GitentialSettings) -> GitentialBackend:
    if settings.backend == BackendType.in_memory:
        logger.debug("Creating in memory backend")
        return InMemGitentialBackend(settings)
    elif settings.backend == BackendType.sql:
        logger.debug("Creating SQL backend")
        return SQLGitentialBackend(settings)
    else:
        raise ValueError("Cannot initialize backend")
