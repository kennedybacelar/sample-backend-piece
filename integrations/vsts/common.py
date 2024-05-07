from typing import List, Tuple, Optional
from urllib.parse import urlparse

from gitential2.datatypes import RepositoryInDB
from gitential2.datatypes.authors import AuthorAlias
from gitential2.datatypes.its import ITSIssueStatusCategory, ITSIssueChangeType
from gitential2.datatypes.its_projects import ITSProjectInDB
from gitential2.integrations.common import get_time_of_last_element
from gitential2.utils import is_timestamp_within_days


def _get_organization_and_project_from_namespace(namespace: str) -> Tuple[str, str]:
    if len(namespace.split("/")) == 2:
        organization, project = namespace.split("/")
        return (organization, project)
    raise ValueError(f"Don't know how to parse vsts {namespace} namespace")


def get_db_issue_id(issue_dict: dict, its_project: ITSProjectInDB) -> str:
    return f"{its_project.id}-{issue_dict['id']}"


def _parse_labels(labels: str) -> List[str]:
    if labels:
        return [label.strip() for label in labels.split(";")]
    return []


def _parse_status_category(status_category_api: Optional[str]) -> ITSIssueStatusCategory:
    assignment_state_category_api_to_its = {
        "Proposed": "new",
        "InProgress": "in_progress",
        "Resolved": "done",
        "Completed": "done",
        "Removed": "done",
    }
    if status_category_api in assignment_state_category_api_to_its:
        return ITSIssueStatusCategory(assignment_state_category_api_to_its[status_category_api])
    return ITSIssueStatusCategory.unknown


def _parse_its_issue_change_type(field_name: str) -> ITSIssueChangeType:
    assignment_issue_change_type = {
        "System.IterationLevel2": "sprint",
        "System.State": "status",
        "System.AssignedTo": "assignee",
    }
    if field_name in assignment_issue_change_type:
        return ITSIssueChangeType(assignment_issue_change_type[field_name])
    return ITSIssueChangeType.other


def _get_project_organization_and_repository(
    repository: RepositoryInDB, repo_field_identifier: str = "name"
) -> Tuple[str, str, str]:
    organization, project = _get_organization_and_project_from_namespace(repository.namespace)
    if repo_field_identifier == "name":
        repo = repository.name
    elif repo_field_identifier == "id":
        repo = repository.extra.get("id", repository.name) if repository.extra else repository.name
    return organization, project, repo


def _parse_azure_repository_url(url: str) -> Tuple[str, str, str]:
    parsed_url = urlparse(url)

    if parsed_url.hostname and parsed_url.path:
        _splitted_path = parsed_url.path.split("/")

        if "visualstudio.com" in parsed_url.hostname and "_apis/git/repositories" in parsed_url.path:
            # "https://ORGANIZATION_NAME.visualstudio.com/PROJECT_ID/_apis/git/repositories/REPOSITORY_ID"
            organization_name = parsed_url.hostname.split(".")[0]
            project_id = _splitted_path[1]
            repository_id = _splitted_path[-1]
            return organization_name, project_id, repository_id
        elif "dev.azure.com" in parsed_url.hostname:
            organization_name = _splitted_path[1]

    raise ValueError(f"Don't know how to parse AZURE Resource URL: {url}")


# pylint: disable=unused-argument
def _parse_clone_url(url: str) -> Tuple[str, str, str]:
    return ("", "", "")


def _paginate_with_skip_top(
    client,
    starting_url,
    top=100,
    repo_analysis_limit_in_days: Optional[int] = None,
    time_restriction_check_key: Optional[str] = None,
) -> list:
    ret: list = []
    skip = 0

    while True:
        url = starting_url + f"&$top={top}&$skip={skip}"
        resp = client.get(url)
        if resp.status_code != 200:
            return ret
        elif resp.status_code == 200:
            json_resp = resp.json()
            count = json_resp["count"]
            value = json_resp["value"]
            ret += value
            if __is_able_to_continue_walking(
                values=value,
                count=count,
                top=top,
                repo_analysis_limit_in_days=repo_analysis_limit_in_days,
                time_restriction_check_key=time_restriction_check_key,
            ):
                skip = skip + top
            else:
                return ret


def __is_able_to_continue_walking(
    values: List,
    count: int,
    top: int,
    repo_analysis_limit_in_days: Optional[int] = None,
    time_restriction_check_key: Optional[str] = None,
) -> bool:
    time_of_last_el = get_time_of_last_element(values, time_restriction_check_key)
    return bool(
        count >= top
        and (
            not repo_analysis_limit_in_days
            or repo_analysis_limit_in_days
            and time_of_last_el
            and is_timestamp_within_days(time_of_last_el, repo_analysis_limit_in_days)
        )
    )


def to_author_alias(raw_user):
    return AuthorAlias(
        name=raw_user.get("displayName"),
        email=raw_user.get("uniqueName"),
    )
