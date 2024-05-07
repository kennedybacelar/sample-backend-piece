from datetime import datetime, timezone, timedelta
from typing import Optional, Callable, List, Tuple
from urllib.parse import parse_qs

from authlib.integrations.requests_client import OAuth2Session
from pydantic.datetime_parse import parse_datetime
from structlog import get_logger

from gitential2.datatypes import UserInfoCreate, RepositoryCreate, RepositoryInDB, GitProtocol
from gitential2.datatypes.its import (
    ITSIssueHeader,
    ITSIssueAllData,
    ITSIssue,
    ITSIssueComment,
    ITSIssueChange,
    ITSIssueTimeInStatus,
    ITSIssueChangeType,
)
from gitential2.datatypes.its_projects import ITSProjectCreate, ITSProjectInDB
from gitential2.datatypes.pull_requests import PullRequest, PullRequestComment, PullRequestCommit, PullRequestState
from .common import (
    _get_project_organization_and_repository,
    _get_organization_and_project_from_namespace,
    _paginate_with_skip_top,
    to_author_alias,
    _parse_status_category,
    get_db_issue_id,
    _parse_labels,
)
from .transformations import (
    _transform_to_its_ITSIssueAllData,
    _transform_to_its_ITSIssueComment,
    _transform_to_ITSIssueChange,
    _initial_status_transform_to_ITSIssueChange,
)
from ..base import BaseIntegration, OAuthLoginMixin, GitProviderMixin, PullRequestData, ITSProviderMixin
from ..common import log_api_error
from ...utils.is_bugfix import calculate_is_bugfix

logger = get_logger(__name__)


class VSTSIntegration(OAuthLoginMixin, GitProviderMixin, BaseIntegration, ITSProviderMixin):
    base_url = "https://app.vssps.visualstudio.com"

    def get_client(self, token, update_token) -> OAuth2Session:
        return self.get_oauth2_client(token=token, update_token=update_token)

    def _auth_client_secret_uri(self, client, method, uri, headers, body):
        logger.debug(
            "vsts._auth_client_secret_uri inputs", client=client, method=method, uri=uri, headers=headers, body=body
        )
        body_original = parse_qs(body, encoding="utf8")

        body_ = {
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": self.settings.oauth.client_secret,
            "redirect_uri": self.settings.options.get("redirect_url"),
        }

        if b"code" in body_original:
            body_["grant_type"] = "urn:ietf:params:oauth:grant-type:jwt-bearer"
            body_["assertion"] = body_original[b"code"][0].decode()

        elif "refresh_token" in body_original:
            body_["grant_type"] = "refresh_token"
            body_["assertion"] = body_original["refresh_token"][0]

        body_str = "&".join([f"{k}={v}" for (k, v) in body_.items()])

        headers["content-length"] = str(len(body_str))
        logger.debug("vsts._auth_client_secret_uri outputs", uri=uri, headers=headers, body_str=body_str)
        return uri, headers, body_str

    def oauth_register(self):
        return {
            "api_base_url": self.base_url,
            "access_token_url": f"{self.base_url}/oauth2/token",
            "authorize_url": f"{self.base_url}/oauth2/authorize",
            "userinfo_endpoint": f"{self.base_url}/_apis/profile/profiles/me?api-version=4.1",
            "client_kwargs": {
                "scope": "vso.code vso.project vso.work",
                "response_type": "Assertion",
                "token_endpoint_auth_method": self._auth_client_secret_uri,
            },
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        return UserInfoCreate(
            integration_type="vsts",
            integration_name=self.name,
            sub=data["id"],
            preferred_username=data["displayName"],
            email=data["emailAddress"],
            extra=data,
        )

    def _collect_raw_pull_requests(
        self, repository: RepositoryInDB, client, repo_analysis_limit_in_days: Optional[int] = None
    ) -> list:
        organization, project, repo = _get_project_organization_and_repository(
            repository=repository, repo_field_identifier="id"
        )
        pull_requests = _paginate_with_skip_top(
            client,
            f"https://dev.azure.com/{organization}/{project}/_apis/git/pullrequests?api-version=6.0&searchCriteria.repositoryId={repo}&searchCriteria.status=all",
            repo_analysis_limit_in_days=repo_analysis_limit_in_days,
            time_restriction_check_key="creationDate",
        )
        return pull_requests

    def _raw_pr_number_and_updated_at(self, raw_pr: dict) -> Tuple[int, datetime]:
        return (
            raw_pr["pullRequestId"],
            parse_datetime(raw_pr["closedDate"])
            if "closedDate" in raw_pr
            else datetime.utcnow().replace(tzinfo=timezone.utc),
        )

    def _collect_raw_pull_request(
        self, repository: RepositoryInDB, pr_number: int, client, repo_analysis_limit_in_days: Optional[int] = None
    ) -> dict:
        def _get_json_response(url):
            resp = client.get(url)
            resp.raise_for_status()
            return resp.json()

        organization, project, repo = _get_project_organization_and_repository(repository)

        pr_details = _get_json_response(
            f"https://dev.azure.com/{organization}/{project}/_apis/git/pullrequests/{pr_number}?api-version=6.0"
        )
        iterations = _get_json_response(
            f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo}/pullRequests/{pr_number}/iterations?api-version=6.0"
        )
        threads = _get_json_response(
            f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo}/pullRequests/{pr_number}/threads?api-version=6.0"
        )

        commits = _get_json_response(
            f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo}/pullRequests/{pr_number}/commits?api-version=6.0"
        )

        return {"pr": pr_details, "threads": threads, "commits": commits, "iterations": iterations}

    def _tranform_to_pr_data(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict, author_callback: Callable
    ) -> PullRequestData:
        return PullRequestData(
            pr=self._tranform_to_pr(repository, pr_number, raw_data, author_callback),
            comments=self._tranform_to_comments(repository, pr_number, raw_data, author_callback),
            commits=self._transform_to_commits(repository, pr_number, raw_data),
            labels=[],
        )

    def _tranform_to_pr(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict, author_callback: Callable
    ) -> PullRequest:

        all_commit_dates = [c["author"]["date"] for c in raw_data.get("commits", {}).get("value", [])]
        all_commit_dates.sort()
        first_commit_authored_at = all_commit_dates[0] if all_commit_dates else None

        all_interactions_dates = []

        for thread in raw_data.get("threads", {}).get("value", []):
            for comment in thread.get("comments", []):
                if "publishedDate" in comment and comment["publishedDate"]:
                    all_interactions_dates.append(comment["publishedDate"])

        if raw_data["pr"].get("closedDate"):
            all_interactions_dates.append(raw_data["pr"].get("closedDate"))

        all_interactions_dates.sort()
        first_interaction_at = all_interactions_dates[0] if all_interactions_dates else None

        return PullRequest(
            repo_id=repository.id,
            number=pr_number,
            title=raw_data["pr"].get("title", "<missing title>"),
            platform="vsts",
            id_platform=raw_data["pr"]["pullRequestId"],
            api_resource_uri=raw_data["pr"]["url"],
            state_platform=raw_data["pr"]["status"],
            state=PullRequestState.from_vsts(raw_data["pr"]["status"]),
            created_at=raw_data["pr"]["creationDate"],
            closed_at=raw_data["pr"].get("closedDate"),
            updated_at=raw_data["pr"].get("closedDate") or None,
            merged_at=raw_data["pr"].get("closedDate") if raw_data["pr"]["status"] == "completed" else None,
            additions=0,
            deletions=0,
            changed_files=0,
            draft=raw_data["pr"]["isDraft"],
            user=raw_data["pr"]["createdBy"]["uniqueName"],
            user_name_external=raw_data["pr"]["createdBy"]["displayName"],
            user_username_external=raw_data["pr"]["createdBy"]["uniqueName"],
            user_aid=author_callback(to_author_alias(raw_data["pr"]["createdBy"])),
            commits=len(raw_data["commits"].get("value", [])),
            merged_by=raw_data["pr"]["closedBy"]["uniqueName"]
            if "closedBy" in raw_data["pr"]
            and PullRequestState.from_vsts(raw_data["pr"]["status"]) == PullRequestState.merged
            else None,
            merged_by_aid=author_callback(to_author_alias(raw_data["pr"]["closedBy"]))
            if "closedBy" in raw_data["pr"]
            and PullRequestState.from_vsts(raw_data["pr"]["status"]) == PullRequestState.merged
            else None,
            first_reaction_at=first_interaction_at,
            first_commit_authored_at=first_commit_authored_at,
            extra=raw_data,
            is_bugfix=calculate_is_bugfix([], raw_data["pr"].get("title", "<missing title>")),
        )

    def _tranform_to_comments(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict, author_callback: Callable
    ) -> List[PullRequestComment]:
        ret = []
        for thread in raw_data.get("threads", {}).get("value", []):
            for comment in thread.get("comments", []):
                pr_comment = PullRequestComment(
                    repo_id=repository.id,
                    pr_number=pr_number,
                    comment_type=str(comment["commentType"]),
                    comment_id="-".join([str(thread["id"]), str(comment["id"])]),
                    thread_id=str(thread["id"]),
                    parent_comment_id=str(comment["parentCommentId"]),
                    author_id_external=comment["author"]["id"],
                    author_name_external=comment["author"]["displayName"],
                    author_username_external=comment["author"]["uniqueName"],
                    author_aid=author_callback(to_author_alias(comment["author"])),
                    content=comment.get("content", ""),
                    extra=comment,
                    created_at=comment["publishedDate"],
                    updated_at=comment["lastUpdatedDate"],
                    published_at=comment["publishedDate"],
                )
                ret.append(pr_comment)

        return ret

    def _transform_to_commits(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict
    ) -> List[PullRequestCommit]:
        ret = []
        for commit_raw in raw_data.get("commits", {}).get("value", []):
            commit = PullRequestCommit(
                repo_id=repository.id,
                pr_number=pr_number,
                commit_id=commit_raw["commitId"],
                author_name=commit_raw.get("author", {}).get("name", None),
                author_email=commit_raw.get("author", {}).get("email", None),
                author_date=commit_raw.get("author", {}).get("date", None),
                author_login=None,
                committer_name=commit_raw.get("committer", {}).get("name", None),
                committer_email=commit_raw.get("committer", {}).get("email", None),
                committer_date=commit_raw.get("committer", {}).get("date", None),
                committer_login=None,
                created_at=commit_raw.get("committer", {}).get("date", None),
                updated_at=commit_raw.get("committer", {}).get("date", None),
                extra=commit_raw,
            )
            ret.append(commit)
        return ret

    def refresh_token_if_expired(self, token, update_token: Callable) -> Tuple[bool, dict]:
        new_token = self.refresh_token(token)
        if new_token:
            update_token(new_token)
            return True, new_token
        else:
            return False, {}

    def refresh_token(self, token):
        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)
        refresh_response = client.refresh_token(
            self.oauth_register()["access_token_url"], refresh_token=token["refresh_token"]
        )

        client.close()
        if "access_token" in refresh_response:
            return {f: refresh_response[f] for f in ["access_token", "refresh_token", "expires_at"]}
        else:
            return None

    def get_newest_repos_since_last_refresh(
        self,
        token,
        update_token,
        last_refresh: datetime,
        provider_user_id: Optional[str],
        user_organization_names: Optional[List[str]],
    ) -> List[RepositoryCreate]:
        # TODO: Currently it is not possible to get the newest repositories from Azure DevOps API.
        #  So we just returning back all of the available private repositories for the user.
        #  We can not do anything more until this feature is not developed for Microsoft.
        return self.list_available_private_repositories(
            token=token,
            update_token=update_token,
            provider_user_id=provider_user_id,
            user_organization_name_list=user_organization_names,
        )

    def get_raw_single_repo_data(self, repository: RepositoryInDB, token, update_token: Callable) -> Optional[dict]:

        client = self.get_oauth2_client(
            token=token, update_token=update_token, token_endpoint_auth_method=self._auth_client_secret_uri
        )
        organization, project, repo_name = _get_project_organization_and_repository(repository=repository)
        get_repo_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_name}"
        response = client.get(get_repo_url)
        client.close()

        if response.status_code == 200:
            return response.json()
        else:
            log_api_error(response)
            return None

    def last_push_at_repository(self, repository: RepositoryInDB, token, update_token: Callable) -> Optional[datetime]:
        client = self.get_oauth2_client(
            token=token, update_token=update_token, token_endpoint_auth_method=self._auth_client_secret_uri
        )
        organization, project, repo_name = _get_project_organization_and_repository(repository=repository)
        last_commit_url = f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo_name}/commits?$top=1&api-version=6.0"
        response = client.get(last_commit_url)
        client.close()

        if response.status_code == 200:
            last_commit_raw = response.json().get("value")
            if last_commit_raw:
                last_pushed_raw = last_commit_raw[0].get("committer", {}).get("date")
                if last_pushed_raw:
                    last_pushed = parse_datetime(last_pushed_raw).replace(tzinfo=timezone.utc)
                    return last_pushed
        else:
            log_api_error(response)
        return None

    def list_available_private_repositories(
        self, token, update_token, provider_user_id: Optional[str], user_organization_name_list: Optional[List[str]]
    ) -> List[RepositoryCreate]:

        if not provider_user_id:
            logger.warn("Cannot list vsts repositories, provider_user_id is missing", token=token)
            return []
        # token = self.refresh_token(token)

        client = self.get_oauth2_client(
            token=token, update_token=update_token, token_endpoint_auth_method=self._auth_client_secret_uri
        )

        accounts = self._get_all_accounts(client, provider_user_id)
        repos = []
        for account in accounts:
            account_repo_url = f"https://{account['accountName']}.visualstudio.com/DefaultCollection/_apis/git/repositories?api-version=1.0"
            repo_resp = client.get(account_repo_url)

            if repo_resp.status_code != 200:
                log_api_error(repo_resp)
                continue

            response_json = repo_resp.json()
            if "value" in response_json:
                repos += [self._repo_to_create_repo(repo, account) for repo in response_json["value"]]
            else:
                logger.debug("No private repositories found for VSTS integration.")

        return repos

    def _repo_to_create_repo(self, repo_dict, account_dict):
        return RepositoryCreate(
            clone_url=repo_dict["webUrl"],
            protocol=GitProtocol.https,
            name=repo_dict["name"],
            namespace=f"{account_dict['accountName']}/{repo_dict['project']['name']}",
            private=repo_dict["project"]["visibility"] == "private",
            integration_type="vsts",
            integration_name=self.name,
            extra=repo_dict,
        )

    def search_public_repositories(
        self, query: str, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        return []

    def _get_all_accounts(self, client, provider_user_id: Optional[str]) -> List[dict]:

        api_base_url = self.oauth_register()["api_base_url"]
        accounts_resp = client.get(f"{api_base_url}/_apis/accounts?memberId={provider_user_id}&api-version=6.0")

        if accounts_resp.status_code != 200:
            log_api_error(accounts_resp)
            return []

        accounts = accounts_resp.json().get("value", [])
        return accounts

    def _get_all_teams(self, client, organization: str) -> List[dict]:

        all_teams_per_organization_url = f"https://dev.azure.com/{organization}/_apis/teams?api-version=4.1-preview.2"
        # Organization>Settings>Security>Policies>Third-party application access via OAuth

        teams_resp = client.get(all_teams_per_organization_url)

        if teams_resp.status_code != 200:
            log_api_error(teams_resp)
            return []

        teams_resp_json = teams_resp.json()["value"]
        return teams_resp_json

    def _getting_project_process_id(self, token, organization: str, project_id: str) -> dict:

        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)

        project_properties_url = (
            f"https://dev.azure.com/{organization}/_apis/projects/{project_id}/properties?api-version=6.0-preview.1"
        )
        project_properties_response = client.get(project_properties_url)

        if project_properties_response.status_code != 200:
            log_api_error(project_properties_response)
            return {}

        project_properties_response_json = project_properties_response.json().get("value")

        for single_property in project_properties_response_json:
            if single_property["name"] == "System.ProcessTemplateType":
                return {"process_id": single_property["value"]}
        return {}

    def list_available_its_projects(
        self, token, update_token, provider_user_id: Optional[str]
    ) -> List[ITSProjectCreate]:

        if not provider_user_id:
            logger.warn("Cannot list vsts repositories, provider_user_id is missing", token=token)
            return []

        client = self.get_oauth2_client(
            token=token, update_token=update_token, token_endpoint_auth_method=self._auth_client_secret_uri
        )

        accounts = self._get_all_accounts(client, provider_user_id)
        ret = []

        for account in accounts:
            organization = account["accountName"]
            teams_resp_json = self._get_all_teams(client, organization)

            for team in teams_resp_json:
                team["organization"] = organization
                ret.append(self._transform_to_its_project(token=token, project_dict=team))
        return ret

    def _transform_to_its_project(self, token, project_dict: dict) -> ITSProjectCreate:
        return ITSProjectCreate(
            name=project_dict["name"],
            namespace=f"{project_dict['organization']}/{project_dict['projectName']}",
            private=True,
            api_url=project_dict["identityUrl"],
            key=project_dict["id"],
            integration_type="vsts",
            integration_name=self.name,
            integration_id=project_dict["id"],
            extra=self._getting_project_process_id(
                token=token, organization=project_dict["organization"], project_id=project_dict["projectId"]
            ),
        )

    def _raw_fetching_all_issues_per_project(
        self,
        token,
        its_project: ITSProjectInDB,
        fields: Optional[List[str]] = None,
        date_from: Optional[datetime] = None,
    ) -> List[dict]:

        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)

        organization, project = _get_organization_and_project_from_namespace(its_project.namespace)
        team = its_project.name

        fields = fields or [
            "System.Id, System.WorkItemType, System.Description, System.AssignedTo, System.State, System.AreaPath,System.Tags, System.CommentCount, System.ChangedDate"
        ]
        if_date_from = (
            f" AND System.ChangedDate > '{date_from.year}-{date_from.month}-{date_from.day}'" if date_from else ""
        )

        body_work_items_by_teams = {
            "query": f"SELECT {','.join(fields)} FROM workitems WHERE [System.TeamProject] = '{project}'{if_date_from} ORDER BY [System.ChangedDate] DESC"
        }

        work_items_url = (
            f"https://dev.azure.com/{organization}/{project}/{team}/_apis/wit/wiql?api-version=6.0-preview.2"
        )

        wit_by_teams_response = client.post(work_items_url, json=body_work_items_by_teams)
        if wit_by_teams_response.status_code != 200:
            log_api_error(wit_by_teams_response)
            return []

        all_work_items_per_its_project = wit_by_teams_response.json().get("workItems", [])

        if all_work_items_per_its_project:
            full_list_of_work_items_ids = []
            for single_work_item in all_work_items_per_its_project:
                full_list_of_work_items_ids.append(single_work_item["id"])

            max_number_work_items_per_batch_request = 200
            ret = []
            for work_item_ids in range(0, len(full_list_of_work_items_ids), max_number_work_items_per_batch_request):

                sliced_list_of_work_items_ids = full_list_of_work_items_ids[
                    work_item_ids : work_item_ids + max_number_work_items_per_batch_request
                ]

                body_query_work_items_details_batch = {"ids": sliced_list_of_work_items_ids}

                get_work_items_details_batch_url = (
                    f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitemsbatch?api-version=6.0"
                )

                wit_by_details_batch_response = client.post(
                    get_work_items_details_batch_url, json=body_query_work_items_details_batch
                )
                if wit_by_details_batch_response.status_code != 200:
                    log_api_error(wit_by_details_batch_response)
                    return []

                ret.extend(wit_by_details_batch_response.json()["value"])
            return ret
        return []

    def get_its_issue_updates(
        self, token, its_project: ITSProjectInDB, issue_id_or_key: str, developer_map_callback: Callable
    ) -> List[ITSIssueChange]:

        # wit is used as a short for workitems in this function

        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)
        organization, project = _get_organization_and_project_from_namespace(its_project.namespace)

        workitems_updates_url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workItems/{issue_id_or_key}/updates?api-version=6.0"

        response_workitems_updates_response = client.get(workitems_updates_url)
        if response_workitems_updates_response.status_code != 200:
            log_api_error(response_workitems_updates_response)
            return []

        # If the key <count> is equal to 1 it means that the wit has not been changed, therefore there is no data to be computed.
        wit_updates = response_workitems_updates_response.json()
        if wit_updates.get("count") == 1:
            return []

        wit_update_values = wit_updates["value"]
        ret = []

        filter_out_fields = [
            "System.Rev",
            "System.RevisedDate",
            "System.ChangedDate",
        ]

        for idx, workitem_update in enumerate(wit_update_values):

            # When just links are added into the wit, it does not change the ChangedDate, hence it is not being considered as a change
            if not workitem_update.get("fields", {}).get("System.ChangedDate"):
                continue

            # It is needed to get the value for those fields at the moment of the wit creation "System.State" & "System.WorkItemType"
            if not idx:
                for field in workitem_update["fields"].items():
                    if field[0] in ["System.State", "System.WorkItemType"]:
                        ret.append(
                            _transform_to_ITSIssueChange(
                                its_project=its_project,
                                single_update=workitem_update,
                                single_field=field,
                                developer_map_callback=developer_map_callback,
                            )
                        )

            for field in workitem_update["fields"].items():
                if field[0] not in filter_out_fields:
                    ret.append(
                        _transform_to_ITSIssueChange(
                            its_project=its_project,
                            single_update=workitem_update,
                            single_field=field,
                            developer_map_callback=developer_map_callback,
                        )
                    )
        return ret

    def _get_single_work_item_all_data(
        self, token, its_project: ITSProjectInDB, issue_id_or_key: str, request_params: Optional[dict] = None
    ) -> dict:

        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)
        organization, project = _get_organization_and_project_from_namespace(its_project.namespace)

        single_work_item_details_url = (
            f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitems/{issue_id_or_key}?api-version=6.0"
        )

        single_work_item_details_response = client.get(single_work_item_details_url, params=request_params)

        if single_work_item_details_response.status_code != 200:
            log_api_error(single_work_item_details_response)
            return {}

        return single_work_item_details_response.json()

    def _get_issue_comments(
        self, token, its_project: ITSProjectInDB, issue_id_or_key: str, developer_map_callback: Callable
    ) -> List[ITSIssueComment]:

        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)
        organization, project = _get_organization_and_project_from_namespace(its_project.namespace)

        issue_comments_url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workItems/{issue_id_or_key}/comments?api-version=6.0-preview.3"

        issue_comments_response = client.get(issue_comments_url)

        if issue_comments_response.status_code != 200:
            log_api_error(issue_comments_response)
            return []

        ret = []
        list_issue_comments_response = issue_comments_response.json().get("comments", [])
        for single_comment in list_issue_comments_response:
            ret.append(
                _transform_to_its_ITSIssueComment(
                    comment_dict=single_comment, its_project=its_project, developer_map_callback=developer_map_callback
                )
            )
        return ret

    # def _get_linked_issues(self, token, its_project: ITSProjectInDB, issue_id_or_key: str) -> List[ITSIssueLinkedIssue]:

    #     linked_issues_response = self._get_single_work_item_all_data(
    #         token=token,
    #         its_project=its_project,
    #         issue_id_or_key=issue_id_or_key,
    #         request_params={"$expand": "relations"},
    #     )

    #     list_linked_issues_response = linked_issues_response.get("relations")
    #     if not list_linked_issues_response:
    #         return []

    #     ret = []
    #     for single_linked_issue in list_linked_issues_response:
    #         ret.append(
    #             _transform_to_its_ITSIssueLinkedIssue(
    #                 its_project=its_project, issue_id_or_key=issue_id_or_key, single_linked_issue=single_linked_issue
    #             )
    #         )
    #     return ret

    def get_work_item_type_id(self, token, its_project: ITSProjectInDB, wit_ref_name: Optional[str]) -> Optional[str]:

        organization, _project = _get_organization_and_project_from_namespace(its_project.namespace)
        process_id = its_project.extra.get("process_id") if its_project.extra else None  # type: ignore[index]

        if not process_id:
            return None

        get_work_item_type_url = f"https://dev.azure.com/{organization}/_apis/work/processdefinitions/{process_id}/workitemtypes/{wit_ref_name}?api-version=4.1-preview.1"

        wit_id = self.http_get_json_and_cache(
            get_work_item_type_url, token=token, token_endpoint_auth_method=self._auth_client_secret_uri
        )

        if not wit_id:
            return None

        res = wit_id.get("id")
        return res

    def _mapping_status_id(
        self, token, its_project: ITSProjectInDB, issue_state: Optional[str], wit_ref_name: Optional[str]
    ) -> dict:

        organization, _project = _get_organization_and_project_from_namespace(its_project.namespace)
        if not its_project.extra:
            return {}

        process_id = its_project.extra.get("process_id")  # type: ignore[index]
        if not process_id:
            return {}

        list_of_statuses_url = f"https://dev.azure.com/{organization}/_apis/work/processes/{process_id}/workItemTypes/{wit_ref_name}/states?api-version=6.0-preview.1"

        statuses = self.http_get_json_and_cache(
            list_of_statuses_url, token=token, token_endpoint_auth_method=self._auth_client_secret_uri
        )

        if not statuses:
            return {}

        statuses_list = statuses.get("value", [])

        for single_status in statuses_list:
            if issue_state == single_status["name"]:
                return single_status
        return {}

    def get_work_item_type_reference_name(
        self, token, its_project: ITSProjectInDB, work_item_type: Optional[str] = None
    ) -> Optional[str]:

        if not work_item_type:
            return None

        organization, project = _get_organization_and_project_from_namespace(its_project.namespace)
        single_work_item_type_url = (
            f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitemtypes/{work_item_type}?api-version=6.0"
        )

        single_work_item_type_response = self.http_get_json_and_cache(
            single_work_item_type_url, token=token, token_endpoint_auth_method=self._auth_client_secret_uri
        )

        if not single_work_item_type_response:
            return None

        res = single_work_item_type_response.get("referenceName")
        return res

    def _transform_to_its_issues_header(self, token, issue_dict: dict, its_project: ITSProjectInDB) -> ITSIssueHeader:

        wit_reference_name = self.get_work_item_type_reference_name(
            token=token, its_project=its_project, work_item_type=issue_dict["fields"].get("System.WorkItemType")
        )

        status_category_api_mapped = self._mapping_status_id(
            token=token,
            its_project=its_project,
            issue_state=issue_dict["fields"].get("System.State"),
            wit_ref_name=wit_reference_name,
        )

        return ITSIssueHeader(
            id=get_db_issue_id(issue_dict, its_project),
            itsp_id=its_project.id,
            api_url=issue_dict["url"],
            api_id=issue_dict["id"],
            key=issue_dict["id"],
            status_name=issue_dict["fields"].get("System.State"),
            status_id=status_category_api_mapped.get("id"),
            status_category=_parse_status_category(status_category_api_mapped.get("stateCategory")),
            summary=issue_dict["fields"].get("System.Title"),
            created_at=parse_datetime(issue_dict["fields"].get("System.CreatedDate")),
            updated_at=parse_datetime(issue_dict["fields"].get("System.ChangedDate")),
        )

    def _transform_to_its_ITSIssueTimeInStatus(
        self, token, changes: List[ITSIssueChange], its_project: ITSProjectInDB, issue_id_or_key: str
    ) -> List[ITSIssueTimeInStatus]:

        if not changes:
            return []

        work_item_type = None
        previous_change = None
        ret: List[ITSIssueTimeInStatus] = []

        for current_change in changes:

            if current_change.field_name == "System.WorkItemType":
                work_item_type = current_change.field_name
                wit_reference_name = self.get_work_item_type_reference_name(
                    token=token, its_project=its_project, work_item_type=work_item_type
                )
            if not work_item_type:
                continue

            if current_change.change_type == ITSIssueChangeType.status:

                if not previous_change:
                    previous_change = current_change
                    continue

                status_category_api_mapped = self._mapping_status_id(
                    token=token,
                    its_project=its_project,
                    issue_state=previous_change.v_to_string,
                    wit_ref_name=wit_reference_name,
                )

                timeSpent = ITSIssueTimeInStatus(
                    issue_id=issue_id_or_key,
                    itsp_id=previous_change.itsp_id,
                    created_at=previous_change.updated_at,
                    updated_at=current_change.updated_at,
                    id=f"{issue_id_or_key}-{previous_change.api_id}",
                    status_name=previous_change.v_to_string,
                    status_id=status_category_api_mapped.get("id"),
                    status_category_api=status_category_api_mapped.get("stateCategory"),
                    status_category=_parse_status_category(status_category_api_mapped.get("stateCategory")),
                    started_issue_change_id=previous_change.id,
                    started_at=previous_change.updated_at,
                    ended_issue_change_id=current_change.id,
                    ended_at=current_change.updated_at,
                    ended_with_status_name=current_change.v_to_string,
                    ended_with_status_id=current_change.v_to_string,
                    seconds_in_status=(current_change.updated_at - previous_change.updated_at).total_seconds()
                    if (current_change.updated_at and previous_change.updated_at)
                    else 0,
                )
                ret.append(timeSpent)
                previous_change = current_change
        return ret

    def _transform_to_its_issue(
        self,
        token,
        issue_dict: dict,
        its_project: ITSProjectInDB,
        developer_map_callback: Callable,
        comment: Optional[ITSIssueComment] = None,
    ) -> ITSIssue:

        # wit = work item type

        wit_reference_name = self.get_work_item_type_reference_name(
            token=token, its_project=its_project, work_item_type=issue_dict["fields"].get("System.WorkItemType")
        )

        wit_id = self.get_work_item_type_id(token=token, its_project=its_project, wit_ref_name=wit_reference_name)

        status_category_api_mapped = self._mapping_status_id(
            token=token,
            its_project=its_project,
            issue_state=issue_dict["fields"].get("System.State"),
            wit_ref_name=wit_reference_name,
        )

        return ITSIssue(
            id=get_db_issue_id(issue_dict, its_project),
            itsp_id=its_project.id,
            api_url=issue_dict["url"],
            api_id=issue_dict["id"],
            key=issue_dict["id"],
            status_name=issue_dict["fields"].get("System.State"),
            status_id=status_category_api_mapped.get("id"),
            status_category_api=status_category_api_mapped.get("stateCategory"),
            status_category=_parse_status_category(status_category_api_mapped.get("stateCategory")),
            issue_type_name=issue_dict["fields"].get("System.WorkItemType"),
            issue_type_id=wit_id,
            resolution_name=issue_dict["fields"]["System.Reason"]
            if issue_dict["fields"].get("Microsoft.VSTS.Common.ClosedDate")
            else None,
            resolution_id=None,
            resolution_date=issue_dict["fields"].get("Microsoft.VSTS.Common.ClosedDate"),
            priority_name=issue_dict["fields"].get("Microsoft.VSTS.Common.Priority"),
            priority_id=None,
            priority_order=None,
            summary=issue_dict["fields"].get("System.Title", ""),
            description=issue_dict["fields"].get("System.Description", ""),
            creator_api_id=issue_dict["fields"]["System.CreatedBy"].get("id"),
            creator_email=issue_dict["fields"]["System.CreatedBy"].get("uniqueName"),
            creator_name=issue_dict["fields"]["System.CreatedBy"].get("displayName"),
            creator_dev_id=developer_map_callback(to_author_alias(issue_dict["fields"].get("System.CreatedBy")))
            if issue_dict["fields"].get("System.CreatedBy")
            else None,
            reporter_api_id=issue_dict["fields"]["System.CreatedBy"].get("id"),
            reporter_email=issue_dict["fields"]["System.CreatedBy"].get("uniqueName"),
            reporter_name=issue_dict["fields"]["System.CreatedBy"].get("displayName"),
            reporter_dev_id=developer_map_callback(to_author_alias(issue_dict["fields"].get("System.CreatedBy")))
            if issue_dict["fields"].get("System.CreatedBy")
            else None,
            assignee_api_id=issue_dict["fields"]["System.AssignedTo"].get("id")
            if issue_dict["fields"].get("System.AssignedTo")
            else None,
            assignee_email=issue_dict["fields"]["System.AssignedTo"].get("uniqueName")
            if issue_dict["fields"].get("System.AssignedTo")
            else None,
            assignee_name=issue_dict["fields"]["System.AssignedTo"].get("displayName")
            if issue_dict["fields"].get("System.AssignedTo")
            else None,
            assignee_dev_id=developer_map_callback(to_author_alias(issue_dict["fields"].get("System.AssignedTo")))
            if issue_dict["fields"].get("System.AssignedTo")
            else None,
            labels=_parse_labels(issue_dict["fields"].get("System.Tags")),
            is_started=bool(issue_dict["fields"].get("Microsoft.VSTS.Common.ActivatedDate")),
            started_at=parse_datetime(issue_dict["fields"]["Microsoft.VSTS.Common.ActivatedDate"])
            if issue_dict["fields"].get("ActivatedDate")
            else None,
            is_closed=bool(issue_dict["fields"].get("Microsoft.VSTS.Common.ClosedDate")),
            closed_at=parse_datetime(issue_dict["fields"]["Microsoft.VSTS.Common.ClosedDate"])
            if issue_dict["fields"].get("Microsoft.VSTS.Common.ClosedDate")
            else None,
            comment_count=issue_dict["fields"].get("System.CommentCount"),
            last_comment_at=parse_datetime(comment.created_at) if hasattr(comment, "created_at") else None,  # type: ignore[union-attr,arg-type]
            change_count=issue_dict.get("rev"),
            last_change_at=parse_datetime(issue_dict["fields"].get("System.ChangedDate")),
            story_points=None,
            created_at=parse_datetime(issue_dict["fields"].get("System.CreatedDate")),
            updated_at=parse_datetime(issue_dict["fields"].get("System.ChangedDate")),
        )

    def list_recently_updated_issues(
        self, token, its_project: ITSProjectInDB, date_from: Optional[datetime] = None
    ) -> List[ITSIssueHeader]:

        number_of_days_since_last_change_to_be_considered_recent = 7

        date_from = date_from or (
            datetime.today() - timedelta(number_of_days_since_last_change_to_be_considered_recent)
        )
        ret: List[ITSIssueHeader] = []

        wit_by_details_batch_response_json = self._raw_fetching_all_issues_per_project(
            token=token, its_project=its_project, date_from=date_from
        )

        for single_issue in wit_by_details_batch_response_json:
            ret.append(
                self._transform_to_its_issues_header(
                    token=token,
                    issue_dict=single_issue,
                    its_project=its_project,
                )
            )
        return ret

    def list_all_issues_for_project(
        self,
        token,
        its_project: ITSProjectInDB,
        date_from: Optional[datetime] = None,
    ) -> List[ITSIssueHeader]:

        wit_by_details_batch_response_json = self._raw_fetching_all_issues_per_project(
            token=token, its_project=its_project, date_from=date_from
        )
        ret = []

        for single_issue in wit_by_details_batch_response_json:
            ret.append(
                self._transform_to_its_issues_header(
                    token=token,
                    issue_dict=single_issue,
                    its_project=its_project,
                )
            )
        return ret

    def get_all_data_for_issue(
        self, token, its_project: ITSProjectInDB, issue_id_or_key: str, developer_map_callback: Callable
    ) -> ITSIssueAllData:

        # raw data of single work item
        single_work_item_details_response_json = self._get_single_work_item_all_data(
            token=token, its_project=its_project, issue_id_or_key=issue_id_or_key
        )

        comments: List[ITSIssueComment] = self._get_issue_comments(
            token=token,
            its_project=its_project,
            issue_id_or_key=issue_id_or_key,
            developer_map_callback=developer_map_callback,
        )

        changes: List[ITSIssueChange] = self.get_its_issue_updates(
            token=token,
            its_project=its_project,
            issue_id_or_key=issue_id_or_key,
            developer_map_callback=developer_map_callback,
        )

        times_in_statuses: List[ITSIssueTimeInStatus] = self._transform_to_its_ITSIssueTimeInStatus(
            token=token, changes=changes, its_project=its_project, issue_id_or_key=issue_id_or_key
        )

        # linked_issues: List[ITSIssueLinkedIssue] = self._get_linked_issues(
        #     token=token, its_project=its_project, issue_id_or_key=issue_id_or_key
        # )

        issue: ITSIssue = self._transform_to_its_issue(
            token=token,
            issue_dict=single_work_item_details_response_json,
            its_project=its_project,
            developer_map_callback=developer_map_callback,
            comment=comments[0] if comments else None,
        )

        return _transform_to_its_ITSIssueAllData(
            issue=issue,
            comments=comments,
            changes=changes,
            times_in_statuses=times_in_statuses,
            linked_issues=[],
        )
