from typing import Optional, List

from dateutil import parser
from requests import Response
from requests.utils import parse_header_links
from structlog import get_logger

from gitential2.utils import is_timestamp_within_days, is_list_not_empty, is_string_not_empty

logger = get_logger(__name__)


def walk_next_link(
    client,
    starting_url,
    acc=None,
    max_pages=100,
    integration_name=None,
    repo_analysis_limit_in_days: Optional[int] = None,
    time_restriction_check_key: Optional[str] = None,
):
    def _get_next_link(link_header) -> Optional[str]:
        if link_header:
            header_links = parse_header_links(link_header)
            for link in header_links:
                if link["rel"] == "next":
                    return link["url"]
        return None

    logger.debug(
        "walking_next_link_of_integration", integration_name=integration_name, url=starting_url, max_pages=max_pages
    )

    acc = acc or []
    response = client.request("GET", starting_url)
    if response.status_code == 200:
        items, headers = response.json(), response.headers

        logger.debug(
            "walking_next_link_response",
            integration_name=integration_name,
            headers=headers,
            response_items_list_length=len(items),
            response_items=items,
        )

        acc = acc + items
        next_url = _get_next_link(headers.get("Link"))
        if __is_able_to_continue_walk_next_link(
            items=items,
            max_pages=max_pages,
            next_url=next_url,
            repo_analysis_limit_in_days=repo_analysis_limit_in_days,
            time_restriction_check_key=time_restriction_check_key,
        ):
            return walk_next_link(client, next_url, acc, max_pages=max_pages - 1, integration_name=integration_name)
        else:
            return acc
    else:
        log_api_error(response)
        return acc


def __is_able_to_continue_walk_next_link(
    items: List[dict],
    max_pages: int,
    next_url: Optional[str] = None,
    repo_analysis_limit_in_days: Optional[int] = None,
    time_restriction_check_key: Optional[str] = None,
):
    time_of_last_el = get_time_of_last_element(items, time_restriction_check_key)
    return (
        next_url
        and max_pages > 0
        and (
            not repo_analysis_limit_in_days
            or repo_analysis_limit_in_days
            and time_of_last_el
            and is_timestamp_within_days(time_of_last_el, repo_analysis_limit_in_days)
        )
    )


def get_time_of_last_element(items: List[dict], key: Optional[str] = None) -> Optional[float]:
    result = None
    if is_string_not_empty(key):
        date_time_str = items[-1][key] if is_list_not_empty(items) else None
        if is_string_not_empty(date_time_str):
            try:
                result = parser.parse(date_time_str).timestamp()
            except ValueError as e:
                logger.error(
                    f"Not able to parse key='{key}' from last element in last items from walk_next_link", exception=e
                )
    return result


def log_api_error(response: Response):
    logger.error(
        "Failed to get API resource",
        url=response.request.url,
        status_code=response.status_code,
        response_text=response.text,
        response_headers=response.headers,
    )
