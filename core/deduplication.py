from typing import List
from itertools import combinations
from structlog import get_logger
from gitential2.datatypes.refresh import RefreshType
from gitential2.datatypes.authors import AuthorInDB
from .refresh_v2 import refresh_workspace
from .authors import authors_matching, merge_authors, delete_author
from .context import GitentialContext

logger = get_logger(__name__)


def deduplicate_authors(g: GitentialContext, workspace_id: int, dry_run: bool = False) -> List[List[AuthorInDB]]:
    all_authors = list(g.backend.authors.all(workspace_id))
    clusters: List[List[AuthorInDB]] = _create_author_clusters(all_authors)
    if not dry_run:
        if clusters:
            for cluster in clusters:
                merge_authors(g, workspace_id, cluster)
        removed = remove_empty_authors(g, workspace_id)
        if clusters or removed:
            refresh_workspace(g, workspace_id, refresh_type=RefreshType.commit_calculations_only)
    return clusters


def _create_author_clusters(all_authors: List[AuthorInDB]) -> List[List[AuthorInDB]]:
    clusters: List[List[AuthorInDB]] = []

    def _add_to_the_cluster(first, second):
        for cluster in clusters:
            if first in cluster and second in cluster:
                break
            if first in cluster and second not in cluster:
                cluster.append(second)
                break
            if second in cluster and first not in cluster:
                cluster.append(first)
                break
        else:
            # Create a new cluster
            clusters.append([first, second])

    for first, second in combinations(all_authors, 2):
        if first.id != second.id and authors_matching(first, second):
            _add_to_the_cluster(first, second)
    return clusters


def remove_empty_authors(g, workspace_id) -> List[int]:
    removed = []
    for author in g.backend.authors.all(workspace_id):
        if not author.aliases:
            logger.info("Removing author with no aliases", author=author, workspace_id=workspace_id)
            delete_author(g, workspace_id, author.id)
            removed.append(author.id)
    return removed
