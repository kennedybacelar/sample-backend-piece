import contextlib
import re
from itertools import product
from typing import Iterable, Dict, List, Optional, cast, Tuple, Union

from structlog import get_logger
from unidecode import unidecode

from gitential2.datatypes.authors import (
    AuthorAlias,
    AuthorInDB,
    AuthorCreate,
    AuthorUpdate,
    AuthorNamesAndEmails,
)
from gitential2.utils import levenshtein_ratio, is_list_not_empty, is_email_valid, is_string_not_empty
from .context import GitentialContext
from ..datatypes.teammembers import TeamMemberInDB
from ..datatypes.teams import TeamInDB
from ..exceptions import NotFoundException

logger = get_logger(__name__)


def list_active_authors(g: GitentialContext, workspace_id: int) -> List[AuthorInDB]:
    return [author for author in list_authors(g, workspace_id) if author.active]


def list_active_author_ids(g: GitentialContext, workspace_id: int) -> List[int]:
    ret = cast(list, g.kvstore.get_value(f"active-authors-{workspace_id}"))
    if not ret:
        ret = [author.id for author in list_active_authors(g, workspace_id=workspace_id)]
        g.kvstore.set_value(f"active-authors-{workspace_id}", ret, 60)

    return ret


@contextlib.contextmanager
def authors_change_lock(
    g: GitentialContext,
    workspace_id: int,
    blocking_timeout_seconds=None,
    timeout_seconds=30,
):
    with g.kvstore.lock(
        f"authors-change-lock{workspace_id}", timeout=timeout_seconds, blocking_timeout=blocking_timeout_seconds
    ):
        yield


def _get_team_title(team_member: TeamMemberInDB, teams_in_workspace: List[TeamInDB]) -> Union[str, None]:
    result = None
    for team in teams_in_workspace:
        if team.id == team_member.team_id:
            result = team.name
            break
    return result


def list_authors(
    g: GitentialContext, workspace_id: int, emails_and_logins: Optional[List[str]] = None
) -> List[AuthorInDB]:
    return (
        get_authors_by_email_and_login(g=g, workspace_id=workspace_id, emails_and_logins=emails_and_logins)
        if emails_and_logins is not None and len(emails_and_logins) > 0
        else list(g.backend.authors.all(workspace_id))
    )


def get_authors_by_email_and_login(
    g: GitentialContext, workspace_id: int, emails_and_logins: List[str]
) -> List[AuthorInDB]:
    return g.backend.authors.get_authors_by_email_and_login(
        workspace_id=workspace_id, emails_and_logins=emails_and_logins
    )


def get_author_names_and_emails(g: GitentialContext, workspace_id: int) -> AuthorNamesAndEmails:
    return g.backend.authors.get_author_names_and_emails(workspace_id=workspace_id)


def authors_count(g: GitentialContext, workspace_id: int, is_only_git_active_authors: Optional[bool] = True) -> int:
    return (
        g.backend.calculated_commits.count_distinct_author_ids(workspace_id=workspace_id)
        if is_only_git_active_authors
        else g.backend.authors.count(workspace_id=workspace_id)
    )


def update_author(g: GitentialContext, workspace_id: int, author_id: int, author_update: AuthorUpdate):
    # existing_author = g.backend.authors.get_or_error(workspace_id, author_id)
    logger.debug("Updating author.", workspace_id=workspace_id, author_id=author_id, author_update=author_update)
    with authors_change_lock(g, workspace_id):
        _reset_all_authors_from_cache(g, workspace_id)
        return g.backend.authors.update(workspace_id, author_id, author_update)


def merge_authors(g: GitentialContext, workspace_id: int, authors: List[AuthorInDB]) -> AuthorInDB:
    first, *rest = authors
    author_update = AuthorUpdate(**first.dict())
    author_ids_to_be_deleted: List[int] = []
    with authors_change_lock(g, workspace_id):
        for other in rest:
            author_update.aliases += other.aliases  # pylint: disable=no-member
            # delete_author(g, workspace_id, other.id)
            author_ids_to_be_deleted.append(other.id)
        author_update.aliases = _remove_duplicate_aliases(author_update.aliases)
        _reset_all_authors_from_cache(g, workspace_id)

    # delete function has been taken out of the with block due lock error - not able to acquire lock
    for author_id in author_ids_to_be_deleted:
        delete_author(g, workspace_id, author_id)
    return g.backend.authors.update(workspace_id, first.id, author_update)


def retrieve_and_merge_authors_by_id(g: GitentialContext, workspace_id: int, author_ids: List[int]) -> AuthorInDB:
    authors = g.backend.authors.get_authors_by_author_ids(workspace_id, author_ids)
    if len(authors) >= 2:
        return merge_authors(g, workspace_id, authors)
    raise NotFoundException(f"Not possible to merge authors {author_ids} from workspace {workspace_id}")


def developer_map_callback(
    alias: AuthorAlias,
    g: GitentialContext,
    workspace_id: int,
) -> Optional[int]:
    author = get_or_create_optional_author_for_alias(g, workspace_id, alias)
    if author:
        return author.id
    else:
        return None


def delete_author(g: GitentialContext, workspace_id: int, author_id: int) -> int:
    with authors_change_lock(g, workspace_id):
        team_ids = g.backend.team_members.get_author_team_ids(workspace_id, author_id)
        for team_id in team_ids:
            g.backend.team_members.remove_members_from_team(workspace_id, team_id, [author_id])
        _reset_all_authors_from_cache(g, workspace_id)
        return g.backend.authors.delete(workspace_id, author_id)


def create_author(g: GitentialContext, workspace_id: int, author_create: AuthorCreate):
    _reset_all_authors_from_cache(g, workspace_id)
    return g.backend.authors.create(workspace_id, author_create)


def get_author(g: GitentialContext, workspace_id: int, author_id: int) -> Optional[AuthorInDB]:
    return g.backend.authors.get(workspace_id, author_id)


def get_authors_by_name_pattern(g: GitentialContext, workspace_id: int, author_name: str) -> List[AuthorInDB]:
    return g.backend.authors.get_by_name_pattern(workspace_id=workspace_id, author_name=author_name)


def fix_author_names(g: GitentialContext, workspace_id: int):
    with authors_change_lock(g, workspace_id):
        fixed_count = 0
        for author in g.backend.authors.all(workspace_id):
            if not author.name:
                logger.info("Fixing author name", author=author, workspace_id=workspace_id)
                possible_names = [alias.name for alias in author.aliases] + [alias.login for alias in author.aliases]
                if not possible_names:
                    logger.warning("No names for author", author=author, workspace_id=workspace_id)
                    continue
                # pylint: disable=unnecessary-lambda
                sorted_names = sorted(possible_names, key=lambda x: len(x) if x else 0, reverse=True)
                author.name = sorted_names[0]
                logger.info("Updating author name", workspace_id=workspace_id, author=author)
                g.backend.authors.update(workspace_id, author.id, cast(AuthorUpdate, author))
                fixed_count += 1
        if fixed_count > 0:
            _reset_all_authors_from_cache(g, workspace_id)


def fix_author_aliases(g: GitentialContext, workspace_id: int):
    with authors_change_lock(g, workspace_id):
        fixed_count = 0
        for author in g.backend.authors.all(workspace_id):
            aliases = _remove_duplicate_aliases(author.aliases)
            if len(aliases) != len(author.aliases):
                author.aliases = aliases
                g.backend.authors.update(workspace_id, author.id, cast(AuthorUpdate, author))
                logger.info("Fixed author aliases", workspace_id=workspace_id, author=author)
                fixed_count += 1
        if fixed_count > 0:
            _reset_all_authors_from_cache(g, workspace_id)


def _get_all_authors_from_cache(g: GitentialContext, workspace_id: int) -> List[AuthorInDB]:
    def _to_list(authors: List[AuthorInDB]) -> List[dict]:
        return [a.dict() for a in authors]

    def _from_list(l: list) -> List[AuthorInDB]:
        return [AuthorInDB(**author_dict) for author_dict in l]

    cache_key = f"authors_ws_{workspace_id}"

    from_cache = g.kvstore.get_value(cache_key)
    if from_cache:
        return _from_list(cast(list, from_cache))
    else:
        all_authors = list(g.backend.authors.all(workspace_id))
        g.kvstore.set_value(cache_key, _to_list(all_authors))
        return all_authors


def _reset_all_authors_from_cache(g: GitentialContext, workspace_id: int):
    cache_key = f"authors_ws_{workspace_id}"
    return g.kvstore.delete_value(cache_key)


def get_or_create_author_for_alias(g: GitentialContext, workspace_id: int, alias: AuthorAlias) -> AuthorInDB:
    with authors_change_lock(g, workspace_id):
        all_authors = _get_all_authors_from_cache(g, workspace_id)
        alias_to_author_map = _build_alias_author_map(all_authors)
        alias_tuple = (alias.name, alias.email, alias.login)
        if alias_tuple in alias_to_author_map:
            return alias_to_author_map[alias_tuple]
        else:
            _reset_all_authors_from_cache(g, workspace_id)
            for author in all_authors:
                if alias_matching_author(alias, author):
                    logger.debug(
                        "Matching author for alias by L-distance", alias=alias, author=author, workspace_id=workspace_id
                    )
                    return add_alias_to_author(g, workspace_id, author, alias)

            new_author = g.backend.authors.create(workspace_id, _new_author_from_alias(alias))
            logger.debug("Creating new author for alias", alias=alias, author=new_author)

            return new_author


def get_or_create_optional_author_for_alias(
    g: GitentialContext, workspace_id: int, alias: AuthorAlias
) -> Optional[AuthorInDB]:
    if alias.name or alias.email or alias.login:
        return get_or_create_author_for_alias(g, workspace_id, alias)
    else:
        logger.debug("Skipping author matching, empty alias")
        return None


def add_alias_to_author(g: GitentialContext, workspace_id: int, author: AuthorInDB, alias: AuthorAlias) -> AuthorInDB:
    author_update = AuthorUpdate(**author.dict())
    author_update.aliases = _remove_duplicate_aliases(author_update.aliases + [alias])
    return g.backend.authors.update(workspace_id, author.id, author_update)


def _build_alias_author_map(
    author_list: Iterable[AuthorInDB],
) -> Dict[Tuple[Optional[str], Optional[str], Optional[str]], AuthorInDB]:
    ret = {}
    for author in author_list:
        for alias in author.aliases or []:
            if not alias.is_empty():
                ret[(alias.name, alias.email, alias.login)] = author
    return ret


def _new_author_from_alias(alias: AuthorAlias) -> AuthorCreate:
    return AuthorCreate(active=True, name=alias.name, email=alias.email, aliases=[alias])


def alias_matching_author(alias: AuthorAlias, author: AuthorInDB):
    return any(aliases_matching(author_alias, alias) for author_alias in author.aliases)


def aliases_matching(first: AuthorAlias, second: AuthorAlias) -> bool:
    if first.email and second.email and first.email == second.email:
        return True
    elif first.login and second.login and first.login == second.login:
        return True
    for first_token, second_token in product(tokenize_alias(first), tokenize_alias(second)):
        if first_token and second_token:
            if levenshtein_ratio(first_token, second_token) > 0.8:
                return True
    return False


def authors_matching(first: AuthorInDB, second: AuthorInDB):
    for alias in second.aliases:
        if alias_matching_author(alias, first):
            return True
    return False


def tokenize_alias(alias: AuthorAlias) -> List[str]:
    def _tokenize_str(s: str) -> str:
        _lower_ascii = unidecode(s.lower())
        _replaced_special = re.sub(r"\W+", " ", _lower_ascii)
        _splitted = _replaced_special.split()
        return " ".join(sorted(_splitted))

    def _remove_duplicates(l: List[str]) -> List[str]:
        ret = []
        for s in l:
            if s not in ret:
                ret.append(s)
        return ret

    def _remove_common_words(l: List[str]) -> List[str]:
        return [s for s in l if s not in _COMMON_WORDS]

    ret = []
    if alias.name:
        ret.append(_tokenize_str(alias.name))
    if alias.email:
        email_first_part = _tokenize_str(alias.email.split("@")[0])
        if len(_remove_common_words(email_first_part.split())) > 1 or len(email_first_part) >= 10:
            ret.append(email_first_part)
    if alias.login:
        ret.append(_tokenize_str(alias.login))
    ret = _remove_common_words(_remove_duplicates(ret))
    # logger.debug("Tokenized alias", alias=alias, tokens=ret)
    return ret


def move_emails_and_logins_to_author(
    g: GitentialContext, workspace_id: int, emails_and_logins: List[str], destination_author_id: int
) -> List[AuthorInDB]:
    logger.debug(
        "Move emails and logins to author.",
        workspace_id=workspace_id,
        emails_and_logins=emails_and_logins,
        destination_author_id=destination_author_id,
    )
    authors: List[AuthorInDB] = g.backend.authors.get_authors_by_email_and_login(
        workspace_id=workspace_id, emails_and_logins=emails_and_logins
    )
    logger.debug(
        "Authors found for search with emails and logins.", emails_and_logins=emails_and_logins, authors=authors
    )

    if all(a.id != destination_author_id for a in authors):
        logger.debug(
            "Destination author is not in the search results. Trying to get author by destination author id.",
            destination_author_id=destination_author_id,
        )
        destination_author = g.backend.authors.get_or_error(workspace_id=workspace_id, id_=destination_author_id)
        logger.debug(
            "Got destination author from database. Appending authors with the author.",
            destination_author=destination_author,
        )
        authors.append(destination_author)

    authors_to_update: List[AuthorInDB] = []
    if is_list_not_empty(emails_and_logins) and len(authors) > 1:
        logger.debug("Start to move emails and logins to destination author.")
        for author in authors:
            is_author_changed: bool = (
                __move_email_or_login_to_destination_author(author, emails_and_logins)
                if author.id is destination_author_id
                else __move_email_or_login_from_author(author, emails_and_logins)
            )
            if is_author_changed:
                authors_to_update.append(author)

    updated_authors: List[AuthorInDB] = []
    if len(authors_to_update) > 0:
        for author_to_update in authors_to_update:
            author_update: AuthorUpdate = get_author_update(author_to_update)
            update = update_author(
                g=g, workspace_id=workspace_id, author_id=author_to_update.id, author_update=author_update
            )
            updated_authors.append(update)

    return updated_authors


def __move_email_or_login_to_destination_author(author: AuthorInDB, emails_and_logins: List[str]) -> bool:
    result: bool = False
    logger.debug("Move email or login to destination author", author=author, emails_and_logins=emails_and_logins)
    for eol in emails_and_logins:
        is_email: bool = is_email_valid(eol)
        key: str = "email" if is_email else "login"
        is_email_or_login_not_in_author_aliases: bool = all(
            getattr(alias, key, None) != eol for alias in author.aliases
        )
        if is_email_or_login_not_in_author_aliases:
            author_alias = AuthorAlias(email=eol) if is_email else AuthorAlias(login=eol)
            logger.debug(
                f"{key} '{eol}' not found in author aliases! Appending author with new Alias!",
                new_author_alias=author_alias,
            )
            author.aliases.append(author_alias)
            result = True
        else:
            logger.debug(f"{key} '{eol}' found in aliases, no need for creating new alias for author.")
    return result


def __move_email_or_login_from_author(author: AuthorInDB, emails_and_logins: List[str]) -> bool:
    logger.debug("Moving email or login from author.", author=author, emails_and_logins=emails_and_logins)
    result: bool = False
    for eol in emails_and_logins:
        alias_indexes_to_remove: List[int] = []
        for index, author_alias in enumerate(author.aliases):
            if getattr(author_alias, "email", None) == eol:
                logger.debug("Removing email from author alias.", author=author, email=eol)
                author_alias.email = None
                result = True
            elif getattr(author_alias, "login", None) == eol:
                logger.debug("Removing login from author alias.", author=author, login=eol)
                author_alias.login = None
                result = True
            if all(not is_string_not_empty(v) for v in author_alias.dict().values()):
                alias_indexes_to_remove.append(index)
        if len(alias_indexes_to_remove) > 0:
            logger.debug(
                "Removing empty author aliases.", author=author, alias_indexes_to_remove=alias_indexes_to_remove
            )
            author.aliases = [a for i, a in enumerate(author.aliases) if i not in alias_indexes_to_remove]
    return result


def get_author_update(author_in_db: AuthorInDB) -> AuthorUpdate:
    return AuthorUpdate(
        active=author_in_db.active,
        name=author_in_db.name,
        email=author_in_db.email,
        aliases=author_in_db.aliases,
        extra=author_in_db.extra,
    )


def _remove_duplicate_aliases(aliases: List[AuthorAlias]) -> List[AuthorAlias]:
    ret: List[AuthorAlias] = []
    for alias in aliases:
        if alias.email and alias.email in [r.email for r in ret]:
            continue
        elif alias.login and alias.login in [r.login for r in ret]:
            continue
        elif any(a.name == alias.name and a.login == alias.login and a.email == alias.email for a in ret):
            continue
        elif not alias.is_empty():
            ret.append(alias)
    return ret


def force_filling_of_author_names(g: GitentialContext, workspace_id: int):

    # Since the validation is already implemented within the dataclass AuthorBase we do not need extra logic
    # to correct it se the only thing needed is to reparse the object into AuthorBase object or any other
    # class which inherits from it

    authors_with_null_name_or_email = g.backend.authors.get_authors_with_null_name_or_email(workspace_id)
    logger.info(
        "Authors found with null name or email.", authors_with_null_name_or_email=authors_with_null_name_or_email
    )
    for author in authors_with_null_name_or_email:
        g.backend.authors.update(workspace_id=workspace_id, id_=author.id, obj=get_author_update(author))


_COMMON_WORDS = ["mail", "info", "noreply", "email", "user", "test", "github", "github com"]
