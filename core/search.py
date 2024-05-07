from enum import Enum
from typing import List, Set

from structlog import get_logger
from gitential2.core import GitentialContext


logger = get_logger(__name__)


class EntityType(str, Enum):
    projects = "projects"
    repos = "repos"
    developers = "developers"

    @classmethod
    def get_class(cls, entity: str) -> str:
        return {"projects": "projects", "repos": "repositories", "developers": "authors"}[entity]

    @classmethod
    def get_fields(cls, entity: str) -> Set[str]:
        return {"projects": {"id", "name"}, "repos": {"id", "name", "namespace"}, "developers": {"name", "id"}}[entity]


def search_entity(g: GitentialContext, q: str, workspace_id: int, entity_type: str) -> List[dict]:
    try:
        return [
            item.dict(include=EntityType.get_fields(entity_type))
            for item in getattr(g.backend, EntityType.get_class(entity_type)).search(workspace_id, q)
        ]
    except AttributeError:
        logger.info(f"Search failed for: entity_type: {entity_type}, workspace_id: {workspace_id}, query: {q}")
        raise
