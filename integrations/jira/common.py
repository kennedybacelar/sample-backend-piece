from typing import Optional, Callable, Tuple
from datetime import datetime
from gitential2.datatypes.its_projects import ITSProjectInDB
from gitential2.datatypes.authors import AuthorAlias
from gitential2.utils import add_url_params


def get_all_pages_from_paginated(client, start_url: str, start_at=0, max_results=25, values_key="values") -> list[dict]:
    def _get_resp(client, url, start_at, max_results):
        resp = client.get(url)
        if resp.status_code == 200:
            # print(url, resp, resp.json().keys())
            resp_json = resp.json()
            items = resp_json.get(values_key, [])
            start_at = resp_json.get("startAt", start_at)
            max_results = resp_json.get("maxResults", max_results)
            total = resp_json.get("total", 0)
            return items, start_at, max_results, total
        else:
            print(resp.status_code, resp.json())
            return [], 0, 0, 0

    ret = []
    total = 0

    while True:
        start_url = add_url_params(start_url, {"startAt": start_at, "maxResults": max_results})
        items, start_at, max_results, total = _get_resp(client, start_url, start_at, max_results)

        if items:
            ret += items

        if start_at + max_results < total:
            start_at = start_at + max_results
        else:
            break

    return ret


def get_db_issue_id(its_project: ITSProjectInDB, issue_dict: dict) -> str:
    return f"{its_project.id}-{issue_dict['id']}"


def get_rest_api_base_url_from_project_api_url(api_url: str) -> str:
    if api_url.startswith("https://api.atlassian.com/ex/jira/"):
        splitted = api_url.split("/")
        return "https://api.atlassian.com/ex/jira/" + splitted[5]
    raise ValueError(f"Don't know how to parse jira project api url: {api_url}")


def parse_account(
    account_dict: Optional[dict], developer_map_callback: Callable
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
    account_dict = account_dict or {}

    api_id = account_dict.get("accountId")
    email = account_dict.get("emailAddress")
    name = account_dict.get("displayName")
    if account_dict.get("accountType", "user") == "app":
        dev_id = None
    else:
        dev_id = developer_map_callback(AuthorAlias(name=name, email=email)) if email or name else None
    return api_id, email, name, dev_id


def format_datetime_for_jql(dt: Optional[datetime] = None) -> str:
    if not dt:
        return "1970-01-01 00:00"
    else:
        return dt.strftime("%Y-%m-%d %H:%M")
