import re
from copy import deepcopy
from datetime import datetime, timedelta
from json import dumps
from typing import Optional, List, Dict, TypeGuard, Union, Any, Literal
from urllib.parse import urlencode, unquote, urlparse, parse_qsl, ParseResult

from gitential2.exceptions import SettingsException


def levenshtein(s1: str, s2: str):
    if len(s1) < len(s2):
        return levenshtein(s2, s1)  # pylint: disable=arguments-out-of-order
    # len(s1) >= len(s2)
    if not s2:
        return len(s1)
    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = (
                previous_row[j + 1] + 1
            )  # j+1 instead of j since previous_row and current_row are one character longer
            deletions = current_row[j] + 1  # than s2
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    return previous_row[-1]


def levenshtein_ratio(s1: str, s2: str) -> float:
    distance = levenshtein(s1, s2)
    return 1.0 - (distance / max(len(s1), len(s2)))


def find_first(predicate, iterable):
    for i in iterable:
        if predicate(i):
            return i
    return None


def remove_none(iterable):
    return [e for e in iterable if e is not None]


def rchop(s, sub):
    return s[: -len(sub)] if s.endswith(sub) else s


def lchop(s, sub):
    return s[len(sub) :] if s.startswith(sub) else s


def calc_repo_namespace(clone_url: str) -> str:
    def _remove_last_part(path):
        _ignored_parts = ["_git"]
        return "/".join([e for e in path.split("/")[:-1] if e not in _ignored_parts]).strip("/")

    if "://" in clone_url:
        if clone_url.startswith("ssh://") and len(clone_url.split(":")) > 2:
            # bad, messed up uri + ssh clone_url ssh://git@xxxx:yyyyy.git ...
            return calc_repo_namespace(clone_url[6:])
        parsed_url = urlparse(clone_url)

        return _remove_last_part(parsed_url.path)
    else:
        _, path = clone_url.split(":")
        return _remove_last_part(path)


def split_timerange(from_: datetime, to_: datetime, parts: int = 2):
    time_delta = to_ - from_
    step = time_delta / parts
    start_dt = from_
    end_dt = from_ + step
    yield start_dt, end_dt
    while to_ - end_dt > step:
        start_dt, end_dt = end_dt, end_dt + step
        yield start_dt, end_dt
    yield end_dt, to_


def common_elements_if_not_none(l1: Optional[list], l2: Optional[list]) -> Optional[list]:
    if l1 is None:
        return l2
    elif l2 is None:
        return l1
    else:
        ret = []
        for e in l1 + l2:
            if (e in l1) and (e in l2):
                ret.append(e)
        return ret


def deep_merge_dicts(a: dict, b: dict) -> dict:
    """Deep merge two dictionaries"""

    result = deepcopy(a)
    for bk, bv in b.items():
        av = result.get(bk)
        if isinstance(av, dict) and isinstance(bv, dict):
            result[bk] = deep_merge_dicts(av, bv)
        else:
            result[bk] = deepcopy(bv)
    return result


def add_url_params(url, params):

    url = unquote(url)
    parsed_url = urlparse(url)
    get_args = parsed_url.query
    parsed_get_args = dict(parse_qsl(get_args))
    parsed_get_args.update(params)

    # Bool and Dict values should be converted to json-friendly values
    # you may throw this part away if you don't like it :)
    parsed_get_args.update({k: dumps(v) for k, v in parsed_get_args.items() if isinstance(v, (bool, dict))})

    # Converting URL argument to proper query string
    encoded_get_args = urlencode(parsed_get_args, doseq=True)

    new_url = ParseResult(
        parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, encoded_get_args, parsed_url.fragment
    ).geturl()

    return new_url


def get_schema_name(workspace_id: int):
    return f"ws_{workspace_id}" if workspace_id else None


def is_list_not_empty(arg: Optional[Any] = None) -> TypeGuard[list]:
    return isinstance(arg, list) and len(arg) > 0


def is_string_not_empty(arg: Optional[Any] = None) -> TypeGuard[str]:
    return isinstance(arg, str) and len(arg) > 0


def is_dict_not_empty(arg: Optional[Any] = None) -> TypeGuard[dict]:
    return isinstance(arg, dict) and len(arg) > 0


def get_filtered_dict(
    dict_obj: Dict,
    callback=None,
    keys_to_include: Optional[List[str]] = None,
    keys_to_exclude: Optional[List[str]] = None,
):
    new_dict = dict_obj

    if dict_obj is not None and isinstance(dict_obj, dict) and bool(dict_obj):

        def is_key_filtered(k: str, v) -> bool:
            result = False
            if callback:
                result = callback(k, v)
            elif keys_to_include is not None and len(keys_to_include) > 0:
                result = k in keys_to_include
            elif keys_to_exclude is not None and len(keys_to_exclude) > 0:
                result = k not in keys_to_exclude
            return result

        new_dict = {}
        for (key, value) in dict_obj.items():
            if is_key_filtered(key, value):
                new_dict[key] = value

    return new_dict


regex = re.compile(
    r"([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\"([]!#-[^-~ \t]|(\\[\t -~]))+\")@([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\[[\t -Z^-~]*])"
)


def is_email_valid(email: str) -> bool:
    return bool(re.fullmatch(regex, email))


def is_timestamp_within_days(timestamp: Union[int, float], number_of_days_diff: int) -> bool:
    if not number_of_days_diff or number_of_days_diff < 1:
        raise SettingsException("Number of days difference is invalid!")
    return datetime.fromtimestamp(timestamp) >= datetime.utcnow() - timedelta(days=number_of_days_diff)


def get_user_id_or_raise_exception(
    g,
    is_at_least_one_id_is_needed: bool = True,
    user_id: Optional[int] = None,
    workspace_id: Optional[int] = None,
) -> Optional[int]:

    if user_id:
        user = g.backend.users.get(user_id)
        if user:
            return user_id
        raise SettingsException(f"Provided user_id is invalid. Can not find user with id={user_id}")
    if workspace_id:
        workspace = g.backend.workspaces.get(workspace_id)
        if workspace:
            return workspace.created_by
        raise SettingsException(f"Provided workspace_id is invalid. Can not find workspace with id={workspace_id}")
    if is_at_least_one_id_is_needed:
        raise SettingsException(
            "It is set in the parameters that either user_id or workspace_id is needed in order to perform this operation"
        )
    return None
