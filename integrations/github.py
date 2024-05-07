from datetime import datetime, timezone
from typing import Callable, Optional, List, Tuple

from authlib.integrations.requests_client import OAuth2Session
from pydantic.datetime_parse import parse_datetime
from structlog import get_logger

from gitential2.datatypes import UserInfoCreate, RepositoryInDB, RepositoryCreate, GitProtocol
from gitential2.datatypes.authors import AuthorAlias
from gitential2.datatypes.pull_requests import (
    PullRequest,
    PullRequestState,
    PullRequestData,
    PullRequestCommit,
    PullRequestComment,
)
from .base import OAuthLoginMixin, BaseIntegration, GitProviderMixin
from .common import log_api_error, walk_next_link
from ..utils import is_list_not_empty, is_string_not_empty
from ..utils.is_bugfix import calculate_is_bugfix

logger = get_logger(__name__)


class GithubIntegration(OAuthLoginMixin, GitProviderMixin, BaseIntegration):
    def get_client(self, token, update_token) -> OAuth2Session:
        return self.get_oauth2_client(token=token, update_token=update_token)

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        if not data.get("email"):
            logger.warning("GitHub: Getting all emails because of private email setting.", userinfo=data)
            client = self.get_oauth2_client(token=token)
            response = client.get(self.oauth_register()["api_base_url"] + "user/emails")
            response.raise_for_status()
            emails = response.json()
            data["email"] = next(email["email"] for email in emails if email["primary"])

        logger.info("user info data:", data=data)
        return UserInfoCreate(
            integration_name=self.name,
            integration_type="github",
            sub=str(data["id"]),
            name=data["name"],
            email=data["email"],
            preferred_username=data["login"],
            profile=data["html_url"],
            picture=data["avatar_url"],
            website=data.get("blog"),
            extra=data,
        )

    def oauth_register(self):
        api_base_url = self.settings.options.get("api_base_url", "https://api.github.com/")
        git_hub_oauth_registration_params = {
            "api_base_url": api_base_url,
            "access_token_url": self.settings.options.get(
                "access_token_url", "https://github.com/login/oauth/access_token"
            ),
            "authorize_url": self.settings.options.get("authorize_url", "https://github.com/login/oauth/authorize"),
            "client_kwargs": {"scope": "user:email repo"},
            "userinfo_endpoint": self.settings.options.get("userinfo_endpoint", api_base_url + "user"),
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }
        logger.info("github_oauth_registration_params", params=git_hub_oauth_registration_params)
        return git_hub_oauth_registration_params

    def refresh_token_if_expired(self, token, update_token: Callable) -> Tuple[bool, dict]:
        return False, token

    def get_rate_limit(self, token, update_token: Callable):
        api_base_url = self.oauth_register()["api_base_url"]
        client = self.get_oauth2_client(token=token, update_token=update_token)
        response = client.get(f"{api_base_url}rate_limit")
        if response.status_code == 200:
            rate_limit, headers = response.json(), response.headers

            logger.info("Github API rate limit", rate_limit=rate_limit, headers=headers)
            return rate_limit.get("resources", {}).get("core", None)
        return None

    def get_raw_single_repo_data(self, repository: RepositoryInDB, token, update_token: Callable) -> Optional[dict]:
        api_base_url = self.oauth_register()["api_base_url"]
        client = self.get_oauth2_client(token=token, update_token=update_token)
        response = client.get(f"{api_base_url}repos/{repository.namespace}/{repository.name}")
        client.close()

        if response.status_code == 200:
            return response.json()
        else:
            log_api_error(response)
            return None

    def last_push_at_repository(self, repository: RepositoryInDB, token, update_token: Callable) -> Optional[datetime]:
        raw_single_repo_data = self.get_raw_single_repo_data(repository, token, update_token) or {}
        last_pushed_raw = raw_single_repo_data.get("pushed_at")
        if last_pushed_raw:
            last_push = parse_datetime(last_pushed_raw).replace(tzinfo=timezone.utc)
            return last_push
        return None

    def _collect_raw_pull_requests(
        self, repository: RepositoryInDB, client, repo_analysis_limit_in_days: Optional[int] = None
    ) -> list:
        api_base_url = self.oauth_register()["api_base_url"]
        pr_list_url = f"{api_base_url}repos/{repository.namespace}/{repository.name}/pulls?per_page=100&state=all"
        prs = walk_next_link(
            client,
            pr_list_url,
            integration_name="github_prs_",
            repo_analysis_limit_in_days=repo_analysis_limit_in_days,
            time_restriction_check_key="created_at",
        )
        return prs

    def _raw_pr_number_and_updated_at(self, raw_pr: dict) -> Tuple[int, datetime]:
        return raw_pr["number"], parse_datetime(raw_pr["updated_at"])

    def _check_rate_limit(self, token, update_token):
        rate_limit = self.get_rate_limit(token, update_token)

        if rate_limit and rate_limit["remaining"] > 500:
            return True
        else:
            logger.warn(
                "Skipping because API rate limit",
                rate_limit=rate_limit,
            )
            return False

    def _collect_raw_pull_request(
        self, repository: RepositoryInDB, pr_number: int, client, repo_analysis_limit_in_days: Optional[int] = None
    ) -> dict:
        users: dict = {}

        def _collect_user_data(user_url):
            if user_url not in users:
                resp = client.get(user_url)
                users[user_url] = resp.json()

        api_base_url = self.oauth_register()["api_base_url"]
        pr_url = f"{api_base_url}repos/{repository.namespace}/{repository.name}/pulls/{pr_number}"
        resp = client.get(pr_url)
        resp.raise_for_status()
        pr_details = resp.json()
        _collect_user_data(pr_details["user"]["url"])

        commits = walk_next_link(
            client, pr_details["_links"]["commits"]["href"], integration_name="github_raw_pr_commits"
        )
        review_comments = walk_next_link(
            client, pr_details["_links"]["review_comments"]["href"], integration_name="github_pr_review_comments"
        )

        for c in review_comments:
            if c.get("user"):
                _collect_user_data(c["user"]["url"])

        return {
            "pr": pr_details,
            "commits": commits,
            "review_comments": review_comments,
            "users": users,
        }

    def _tranform_to_pr_data(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict, author_callback: Callable
    ) -> PullRequestData:
        pull_request = self._transform_to_pr(raw_data, repository=repository, author_callback=author_callback)
        commits = self._transform_to_commits(raw_data["commits"], raw_data, repository)
        comments = self._transform_to_comments(
            raw_data["review_comments"],
            raw_data,
            repository,
            author_callback=author_callback,
        )

        return PullRequestData(pr=pull_request, comments=comments, commits=commits, labels=[])

    def _transform_to_pr(self, raw_data, repository, author_callback: Callable):
        def _calc_first_commit_authored_at(raw_commits):
            author_times = [c["commit"]["author"]["date"] for c in raw_commits]
            author_times.sort()
            return author_times[0] if author_times else None

        def _calc_first_reaction_at(raw_pr, review_comments):
            human_note_creation_times = [rc["created_at"] for rc in review_comments]
            human_note_creation_times.sort()
            return (
                human_note_creation_times[0]
                if human_note_creation_times
                else raw_pr.get("merged_at", raw_pr.get("closed_at"))
            )

        # print("*******", raw_data["pr"]["user"])

        user_data = raw_data["users"][raw_data["pr"]["user"]["url"]]

        user_author_id = author_callback(
            AuthorAlias(
                name=user_data.get("name"),
                login=raw_data["pr"]["user"]["login"],
            )
        )

        merged_by_author_id = (
            author_callback(AuthorAlias(login=raw_data["pr"]["merged_by"]["login"]))
            if "merged_by" in raw_data["pr"] and raw_data["pr"]["merged_by"] is not None
            else None
        )

        return PullRequest(
            repo_id=repository.id,
            number=raw_data["pr"]["number"],
            title=raw_data["pr"].get("title", "<missing title>"),
            platform="github",
            id_platform=raw_data["pr"]["id"],
            api_resource_uri=raw_data["pr"]["url"],
            state_platform=raw_data["pr"]["state"],
            state=PullRequestState.from_github(raw_data["pr"]["state"], raw_data["pr"].get("merged_at")),
            created_at=raw_data["pr"]["created_at"],
            closed_at=raw_data["pr"].get("closed_at"),
            updated_at=raw_data["pr"]["updated_at"],
            merged_at=raw_data["pr"].get("merged_at"),
            additions=raw_data["pr"]["additions"],
            deletions=raw_data["pr"]["deletions"],
            changed_files=raw_data["pr"]["changed_files"],
            draft=raw_data["pr"]["draft"],
            user=raw_data["pr"]["user"]["login"],
            user_name_external=user_data.get("name"),
            user_username_external=raw_data["pr"]["user"]["login"],
            user_aid=user_author_id,
            commits=len(raw_data["commits"]),
            merged_by=raw_data["pr"]["merged_by"]["login"]
            if "merged_by" in raw_data["pr"] and raw_data["pr"]["merged_by"] is not None
            else None,
            merged_by_aid=merged_by_author_id,
            first_reaction_at=_calc_first_reaction_at(raw_data["pr"], raw_data["review_comments"]),
            first_commit_authored_at=_calc_first_commit_authored_at(raw_data["commits"]),
            extra=raw_data,
            is_bugfix=calculate_is_bugfix([], raw_data["pr"].get("title", "<missing title>")),
        )

    @staticmethod
    def get_repos_for_github_user_organization(client: OAuth2Session, api_base_url: str, user_organization_name: str):
        results = []
        if is_string_not_empty(user_organization_name):
            logger.debug(
                "Starting to get repositories for user organization.", user_organization_name=user_organization_name
            )
            url = f"{api_base_url}orgs/{user_organization_name}/repos?per_page=100&type=all"
            results = walk_next_link(client, url, integration_name="github_repos_for_given_user_organization")
            logger.debug(
                "Repositories in provided user organization.",
                user_organization_name=user_organization_name,
                number_of_repos_in_organization=len(results),
            )
        return results

    @staticmethod
    def get_repos_for_list_of_github_user_organizations(
        client: OAuth2Session, api_base_url: str, user_organization_name_list: Optional[List[str]]
    ):
        user_orgs_repos: List[dict] = []
        if is_list_not_empty(user_organization_name_list) and all(
            is_string_not_empty(org) for org in user_organization_name_list
        ):
            for user_org_name in user_organization_name_list:
                org_repos = GithubIntegration.get_repos_for_github_user_organization(
                    client, api_base_url, user_org_name
                )
                if is_list_not_empty(org_repos):
                    org_repos_reduced = [
                        repo
                        for repo in org_repos
                        if all(r.get("clone_url", None) != repo.clone_url for r in user_orgs_repos)
                    ]
                    user_orgs_repos += org_repos_reduced
            logger.debug(
                "Repositories results for user organization name list.",
                user_organization_name_list=user_organization_name_list,
                number_of_user_orgs_repos=len(user_orgs_repos),
            )
        else:
            logger.warning(
                "User organization name list is empty!",
            )
        return user_orgs_repos

    @staticmethod
    def get_organization_names_for_github_user(client: OAuth2Session, api_base_url: str) -> List[str]:
        result = []
        get_list_of_organizations_url = f"{api_base_url}user/orgs?per_page=100"
        logger.debug("Starting to get organizations for GitHub user.", url=get_list_of_organizations_url)
        list_of_user_organizations = walk_next_link(
            client, get_list_of_organizations_url, integration_name="github_organizations_for_user"
        )
        if is_list_not_empty(list_of_user_organizations):
            result = [
                org.get("login", None)
                for org in list_of_user_organizations
                if is_string_not_empty(org.get("login", None))
            ]
        logger.debug(
            "List of GitHub organizations for user.",
            number_of_organizations=len(list_of_user_organizations),
            list_of_organization_names=result,
        )
        return result

    @staticmethod
    def get_organization_repos_for_github_user(
        client: OAuth2Session, api_base_url: str, user_organization_names: Optional[List[str]]
    ):
        results = []
        org_names = user_organization_names
        if is_list_not_empty(user_organization_names):
            logger.debug(
                "Starting to get github repos for organization names.",
                user_organization_name_list=user_organization_names,
            )
            results = GithubIntegration.get_repos_for_list_of_github_user_organizations(
                client, api_base_url, user_organization_names
            )
        else:
            org_names_response = GithubIntegration.get_organization_names_for_github_user(client, api_base_url)
            if is_list_not_empty(org_names_response):
                org_names = org_names_response
                logger.debug(
                    "Starting to get all repos of GitHub user organizations.",
                    user_organization_names=org_names_response,
                )
                results = GithubIntegration.get_repos_for_list_of_github_user_organizations(
                    client, api_base_url, org_names_response
                )
        logger.info(
            "GitHub repositories from user organizations.",
            number_of_repositories_from_user_organizations=len(results),
            user_organization_names=org_names,
        )
        return results

    @staticmethod
    def get_merged_repos(repo_list, user_orgs_repos):
        if is_list_not_empty(user_orgs_repos):
            clone_urls: List[str] = [r.get("clone_url", None) for r in repo_list]
            user_orgs_repos_reduced = [
                repo_from_org
                for repo_from_org in user_orgs_repos
                if is_string_not_empty(repo_from_org.get("clone_url", None))
                and repo_from_org.get("clone_url", None) not in clone_urls
            ]
            repo_list += user_orgs_repos_reduced
        return repo_list

    def get_newest_repos_since_last_refresh(
        self,
        token,
        update_token,
        last_refresh: datetime,
        provider_user_id: Optional[str],
        user_organization_names: Optional[List[str]],
    ) -> List[RepositoryCreate]:
        logger.info("Starting to get newest repositories for GitHub.")
        org_repos = []
        last_refresh_formatted = last_refresh.strftime("%Y-%m-%d")
        client = self.get_oauth2_client(token=token, update_token=update_token)
        api_base_url = self.oauth_register()["api_base_url"]

        user_orgs: List[str] = (
            user_organization_names
            if is_list_not_empty(user_organization_names)
            else GithubIntegration.get_organization_names_for_github_user(client, api_base_url)
        )
        if is_list_not_empty(user_orgs):
            logger.debug(
                "Starting to get newest github repos for organization names since last refresh date.",
                user_organization_name_list=user_organization_names,
                last_refresh=last_refresh,
            )
            for org in user_orgs:
                query = f"org:{org} created:>{last_refresh_formatted}"
                repos = self.search_public_repositories(query, token, update_token, None)
                org_repos.extend(repos)
            logger.info(
                "Newest GitHub repositories from user organizations which were created since the last refresh date.",
                number_of_repositories_from_organizations=len(org_repos),
                user_organization_names=user_orgs,
            )

        starting_url = f"{api_base_url}user/repos?per_page=100&type=all&since={last_refresh_formatted}"
        repository_list = walk_next_link(client, starting_url, integration_name="github_private_repos_newly_created")

        merged_repos = GithubIntegration.get_merged_repos(repository_list, org_repos)
        client.close()
        return [self._repo_to_create_repo(repo) for repo in merged_repos]

    def list_available_private_repositories(
        self, token, update_token, provider_user_id: Optional[str], user_organization_name_list: Optional[List[str]]
    ) -> List[RepositoryCreate]:
        client = self.get_oauth2_client(token=token, update_token=update_token)
        api_base_url = self.oauth_register()["api_base_url"]

        user_orgs_repos = GithubIntegration.get_organization_repos_for_github_user(
            client, api_base_url, user_organization_name_list
        )
        starting_url = f"{api_base_url}user/repos?per_page=100&type=all"
        repository_list = walk_next_link(client, starting_url, integration_name="github_private_repos")
        logger.info("GitHub repositories for authenticated user.", number_of_repositories=len(repository_list))

        merged_repos = GithubIntegration.get_merged_repos(repository_list, user_orgs_repos)
        logger.info("All repos (merged) for GitHub user.", number_of_repos_for_github_user=len(merged_repos))

        client.close()
        return [self._repo_to_create_repo(repo) for repo in merged_repos]

    def _repo_to_create_repo(self, repo_dict) -> RepositoryCreate:

        return RepositoryCreate(
            clone_url=repo_dict["clone_url"],
            protocol=GitProtocol.https if repo_dict["clone_url"].startswith("https") else GitProtocol.ssh,
            name=repo_dict["name"],
            namespace=repo_dict["owner"]["login"],
            private=repo_dict.get("private", False),
            integration_type="github",
            integration_name=self.name,
            extra=repo_dict,
        )

    def search_public_repositories(
        self, query: str, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        client = self.get_oauth2_client(token=token, update_token=update_token)
        api_base_url = self.oauth_register()["api_base_url"]
        response = client.get(f"{api_base_url}search/repositories?q={query}")

        if response.status_code == 200:
            repository_list = response.json().get("items", [])
            return [self._repo_to_create_repo(repo) for repo in repository_list]
        else:
            log_api_error(response)
            return []

    def _transform_to_commits(
        self,
        commits_raw: list,
        raw_data: dict,
        repository: RepositoryInDB,
    ) -> List[PullRequestCommit]:
        ret = []
        for commit_raw in commits_raw:
            commit = PullRequestCommit(
                repo_id=repository.id,
                pr_number=raw_data["pr"]["number"],
                commit_id=commit_raw["sha"],
                # author data
                author_name=commit_raw["commit"]["author"]["name"],
                author_email=commit_raw["commit"]["author"].get("email", ""),
                author_date=commit_raw["commit"]["author"]["date"],
                author_login=commit_raw.get("author", {}).get("login", "") if commit_raw.get("author") else None,
                # committer data
                committer_name=commit_raw["commit"]["committer"]["name"],
                committer_email=commit_raw["commit"]["committer"].get("email", ""),
                committer_date=commit_raw["commit"]["committer"]["date"],
                committer_login=commit_raw.get("committer", {}).get("login", "")
                if commit_raw.get("commiter")
                else None,
                # general dates
                created_at=commit_raw["commit"]["author"]["date"],
                updated_at=commit_raw["commit"]["committer"]["date"],
            )
            ret.append(commit)
        return ret

    def _transform_to_comments(
        self,
        notes_raw: list,
        raw_data: dict,
        repository: RepositoryInDB,
        author_callback: Callable,
    ) -> List[PullRequestComment]:
        ret = []
        for note_raw in notes_raw:
            # print(note_raw)
            if note_raw.get("user"):
                user_data = raw_data["users"][note_raw["user"]["url"]]
                # print("!!!***!!!", note_raw["user"])
                author_name_external = user_data.get("name")
                author_aid = author_callback(AuthorAlias(name=user_data.get("name"), login=note_raw["user"]["login"]))
            else:
                author_name_external = None
                author_aid = None

            comment = PullRequestComment(
                repo_id=repository.id,
                pr_number=raw_data["pr"]["number"],
                comment_type="review_comments",
                comment_id=str(note_raw["id"]),
                author_id_external=note_raw["user"]["id"] if note_raw.get("user") else None,
                author_name_external=author_name_external,
                author_username_external=note_raw["user"]["login"] if note_raw.get("user") else None,
                author_aid=author_aid,
                content=note_raw["body"],
                extra=note_raw,
                created_at=note_raw["created_at"],
                updated_at=note_raw["updated_at"],
            )
            ret.append(comment)
        return ret
