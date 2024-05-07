from typing import List, Optional

from fastapi import APIRouter, Depends, Query, Body

from gitential2.core.authors import (
    list_authors,
    update_author,
    delete_author,
    create_author,
    list_active_authors,
    get_author,
    get_author_names_and_emails,
    authors_count,
    get_authors_by_name_pattern,
    move_emails_and_logins_to_author,
    retrieve_and_merge_authors_by_id,
)
from gitential2.core.context import GitentialContext
from gitential2.core.deduplication import deduplicate_authors
from gitential2.core.legacy import authors_in_projects
from gitential2.core.permissions import check_permission
from gitential2.datatypes.authors import (
    AuthorCreate,
    AuthorPublic,
    AuthorUpdate,
    AuthorFilters,
    AuthorsPublicExtendedSearchResult,
    AuthorPublicExtended,
    AuthorInDB,
)
from gitential2.datatypes.permissions import Entity, Action
from ..dependencies import current_user, gitential_context
from ...core.authors_list import (
    list_extended_committer_authors,
    get_extended_committer_author,
    get_authors_extended_by_author_ids,
)

router = APIRouter(tags=["authors"])


@router.get("/workspaces/{workspace_id}/authors-count")
def authors_count_(
    workspace_id: int,
    is_only_git_active_authors: Optional[bool] = Query(None, alias="is_only_git_active_authors"),
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return authors_count(g=g, workspace_id=workspace_id, is_only_git_active_authors=is_only_git_active_authors)


@router.get("/workspaces/{workspace_id}/authors-names-emails")
def list_authors_names_emails_(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return get_author_names_and_emails(g=g, workspace_id=workspace_id)


@router.get("/workspaces/{workspace_id}/authors", response_model=List[AuthorPublic])
def list_authors_(
    workspace_id: int,
    emails_and_logins: Optional[List[str]] = Query(None, alias="emails_and_logins"),
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return list_authors(g=g, workspace_id=workspace_id, emails_and_logins=emails_and_logins)


@router.get("/workspaces/{workspace_id}/authors-extended")
def get_authors_extended_by_author_ids_(
    workspace_id: int,
    developer_ids: List[int] = Query(None, alias="developer_ids"),
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return get_authors_extended_by_author_ids(g=g, workspace_id=workspace_id, developer_ids=developer_ids)


@router.post("/workspaces/{workspace_id}/authors-extended-committer", response_model=AuthorsPublicExtendedSearchResult)
def list_extended_committer_authors_(
    workspace_id: int,
    author_filters: Optional[AuthorFilters] = None,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return list_extended_committer_authors(g=g, workspace_id=workspace_id, author_filters=author_filters)


@router.post("/workspaces/{workspace_id}/authors", response_model=AuthorPublic)
def create_author_(
    author_create: AuthorCreate,
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return create_author(g, workspace_id, author_create)


@router.put("/workspaces/{workspace_id}/authors/{author_id}", response_model=AuthorPublic)
def update_author_(
    author_update: AuthorUpdate,
    workspace_id: int,
    author_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.update, workspace_id=workspace_id)
    return update_author(g, workspace_id, author_id, author_update)


@router.get("/workspaces/{workspace_id}/authors/{author_id}", response_model=AuthorPublic)
def get_author_(
    workspace_id: int,
    author_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return get_author(g, workspace_id, author_id)


@router.get("/workspaces/{workspace_id}/authors-search-by-name", response_model=List[AuthorPublic])
def get_author_by_name_(
    workspace_id: int,
    author_name: str = Query(None, alias="authorName"),
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return get_authors_by_name_pattern(g, workspace_id, author_name)


@router.get("/workspaces/{workspace_id}/authors/{author_id}/extended-committer", response_model=AuthorPublicExtended)
def get_author_extended_(
    workspace_id: int,
    author_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return get_extended_committer_author(g, workspace_id, author_id)


@router.delete("/workspaces/{workspace_id}/authors/{author_id}")
def delete_author_(
    workspace_id: int,
    author_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.delete, workspace_id=workspace_id)
    return delete_author(g, workspace_id, author_id)


@router.get("/workspaces/{workspace_id}/authors-duplicated")
def get_potential_duplicate_authors(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return deduplicate_authors(g=g, workspace_id=workspace_id, dry_run=True)


@router.post("/workspaces/{workspace_id}/merge-authors", response_model=AuthorInDB)
def merge_authors(
    workspace_id: int,
    author_ids: List[int] = Body(None, alias="author_ids", embed=True),
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.update, workspace_id=workspace_id)
    return retrieve_and_merge_authors_by_id(g, workspace_id, author_ids)


@router.get("/workspaces/{workspace_id}/developers-with-projects")
def developers_with_projects(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    authors_and_projects = authors_in_projects(g, workspace_id)
    authors = list_active_authors(g, workspace_id)
    ret = []
    for author in authors:
        author_dict = author.dict()
        author_dict["projects"] = authors_and_projects.get(author.id, {}).get("project_ids", [])
        ret.append(author_dict)
    return ret


@router.post("/workspaces/{workspace_id}/authors/{destination_author_id}/move-aliases")
def move_emails_and_logins_to_author_(
    workspace_id: int,
    destination_author_id: int,
    emails_and_logins: List[str],
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.update, workspace_id=workspace_id)
    return move_emails_and_logins_to_author(g, workspace_id, emails_and_logins, destination_author_id)
