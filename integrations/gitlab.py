from datetime import datetime, timezone
from typing import Optional, Callable, List, Tuple
from urllib import parse as parse_url

from authlib.integrations.requests_client import OAuth2Session
from dateutil import parser
from pydantic.datetime_parse import parse_datetime
from structlog import get_logger

from gitential2.datatypes import UserInfoCreate, RepositoryCreate, GitProtocol, RepositoryInDB
from gitential2.datatypes.authors import AuthorAlias
from gitential2.datatypes.pull_requests import (
    PullRequest,
    PullRequestComment,
    PullRequestCommit,
    PullRequestData,
    PullRequestState,
)
from .base import BaseIntegration, OAuthLoginMixin, GitProviderMixin
from .common import log_api_error, walk_next_link
from ..utils.is_bugfix import calculate_is_bugfix

logger = get_logger(__name__)


class GitlabIntegration(OAuthLoginMixin, GitProviderMixin, BaseIntegration):
    def __init__(self, name, settings, kvstore):
        super().__init__(name, settings, kvstore)
        self.base_url = self.settings.base_url or "https://gitlab.com"
        self.api_base_url = f"{self.base_url}/api/v4"
        self.authorize_url = f"{self.base_url}/oauth/authorize"
        self.token_url = f"{self.base_url}/oauth/token"

    def get_client(self, token, update_token) -> OAuth2Session:
        return self.get_oauth2_client(token=token, update_token=update_token)

    def oauth_register(self):

        return {
            "api_base_url": self.api_base_url,
            "access_token_url": self.token_url,
            "authorize_url": self.authorize_url,
            "userinfo_endpoint": self.api_base_url + "/user",
            "client_kwargs": {"scope": "api read_repository email read_user profile"},
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }

    def refresh_token_if_expired(self, token, update_token: Callable) -> Tuple[bool, dict]:
        return False, token

    def refresh_token(self, token):
        client = self.get_oauth2_client(token=token)
        new_token = client.refresh_token(self.token_url, refresh_token=token["refresh_token"])
        client.close()
        return new_token

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        return UserInfoCreate(
            integration_name=self.name,
            integration_type="gitlab",
            sub=str(data["id"]),
            name=data["name"],
            email=data.get("email"),
            preferred_username=data["username"],
            profile=data["web_url"],
            picture=data["avatar_url"],
            website=data.get("website_url"),
            extra=data,
        )

    def get_newest_repos_since_last_refresh(
        self,
        token,
        update_token,
        last_refresh: datetime,
        provider_user_id: Optional[str],
        user_organization_names: Optional[List[str]],
    ) -> List[RepositoryCreate]:
        last_refresh_formatted = last_refresh.strftime("%Y-%m-%d")
        client = self.get_oauth2_client(token=token, update_token=update_token)
        # Allowed values are asc or desc only. If not set, results are sorted by created_at in descending
        # order for basic search
        query_params = {"membership": 1, "per_page": 100, "last_activity_after": last_refresh_formatted}
        url = f"{self.api_base_url}/projects?{parse_url.urlencode(query_params)}"
        projects = walk_next_link(client, url, integration_name="gitlab_private_newest_repos_since_last_refresh")
        client.close()
        return [self._project_to_repo_create(p) for p in projects if parser.parse(p["created_at"]) > last_refresh]

    def list_available_private_repositories(
        self, token, update_token, provider_user_id: Optional[str], user_organization_name_list: Optional[List[str]]
    ) -> List[RepositoryCreate]:
        client = self.get_oauth2_client(token=token, update_token=update_token)
        url = f"{self.api_base_url}/projects?membership=1&pagination=keyset&order_by=id&per_page=100"
        projects = walk_next_link(client, url, integration_name="gitlab_private_repos")
        client.close()
        return [self._project_to_repo_create(p) for p in projects]

    def search_public_repositories(
        self, query: str, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        client = self.get_oauth2_client(token=token, update_token=update_token)
        response = client.get(f"{self.api_base_url}/search?scope=projects&search={query}")
        client.close()

        if response.status_code == 200:
            projects = response.json()
            return [self._project_to_repo_create(p) for p in projects]
        else:
            log_api_error(response)
            return []

    def get_raw_single_repo_data(self, repository: RepositoryInDB, token, update_token: Callable) -> Optional[dict]:
        api_base_url = self.oauth_register()["api_base_url"]
        client = self.get_oauth2_client(token=token, update_token=update_token)

        # For some reason, for gitlab, the forward slash has to be encoded in this API call
        # Otherwise we get 404 error
        response = client.get(f"{api_base_url}/projects/{repository.namespace}%2F{repository.name}")
        client.close()

        if response.status_code == 200:
            return response.json()
        else:
            log_api_error(response)
            return None

    def last_push_at_repository(self, repository: RepositoryInDB, token, update_token: Callable) -> Optional[datetime]:
        raw_single_repo_data = self.get_raw_single_repo_data(repository, token, update_token) or {}
        last_pushed_raw = raw_single_repo_data.get("last_activity_at")
        if last_pushed_raw:
            last_push = parse_datetime(last_pushed_raw).replace(tzinfo=timezone.utc)
            return last_push
        return None

    def _project_to_repo_create(self, project):
        return RepositoryCreate(
            clone_url=project["http_url_to_repo"],
            protocol=GitProtocol.https,
            name=project["path"],
            namespace=project["namespace"]["full_path"],
            private=project.get("visibility") == "private",
            integration_type="gitlab",
            integration_name=self.name,
            extra=project,
        )

    def _collect_raw_pull_requests(
        self, repository: RepositoryInDB, client, repo_analysis_limit_in_days: Optional[int] = None
    ) -> list:
        if repository.extra and "id" in repository.extra:
            project_id = repository.extra["id"]
            merge_requests = walk_next_link(
                client,
                f"{self.api_base_url}/projects/{project_id}/merge_requests?state=all&per_page=100&view=simple",
                integration_name="gitlab_raw_prs",
                repo_analysis_limit_in_days=repo_analysis_limit_in_days,
                time_restriction_check_key="created_at",
            )
            return merge_requests
        else:
            logger.warning(
                "GitLab project_id missing for repository", repository_id=repository.id, repository_name=repository.name
            )
            return []

    def _raw_pr_number_and_updated_at(self, raw_pr: dict) -> Tuple[int, datetime]:
        return raw_pr["iid"], parse_datetime(raw_pr["updated_at"])

    def _collect_raw_pull_request(
        self, repository: RepositoryInDB, pr_number: int, client, repo_analysis_limit_in_days: Optional[int] = None
    ) -> dict:
        if repository.extra and "id" in repository.extra:
            project_id = repository.extra["id"]
        else:
            logger.warning(
                "GitLab project_id missing for repository", repository_id=repository.id, repository_name=repository.name
            )
            return {}
        merge_request = client.get(f"{self.api_base_url}/projects/{project_id}/merge_requests/{pr_number}").json()
        merge_request_changes = client.get(
            f"{self.api_base_url}/projects/{project_id}/merge_requests/{pr_number}/changes?access_raw_diffs=yes"
        ).json()
        merge_request_commits = walk_next_link(
            client,
            f"{self.api_base_url}/projects/{project_id}/merge_requests/{pr_number}/commits",
            integration_name="gitlab_merge_request_commits",
        )
        merge_request_notes = walk_next_link(
            client,
            f"{self.api_base_url}/projects/{project_id}/merge_requests/{pr_number}/notes",
            integration_name="gitlab_merge_request_notes",
        )
        return {
            "project_id": project_id,
            "iid": pr_number,
            "mr": merge_request,
            "mr_changes": merge_request_changes,
            "mr_commits": merge_request_commits,
            "mr_notes": merge_request_notes,
        }

    def _tranform_to_pr_data(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict, author_callback: Callable
    ) -> PullRequestData:
        pull_request = self._transform_to_pr(raw_data, repository=repository, author_callback=author_callback)
        commits = self._transform_to_commits(raw_data["mr_commits"], raw_data, repository)
        comments = self._transform_to_comments(
            raw_data["mr_notes"], raw_data, repository, author_callback=author_callback
        )
        return PullRequestData(pr=pull_request, comments=comments, commits=commits, labels=[])

    def _transform_to_pr(
        self,
        raw_data: dict,
        repository: RepositoryInDB,
        author_callback: Callable,
    ) -> PullRequest:
        def _calc_first_reaction_at(raw_notes):
            human_note_creation_times = [note["created_at"] for note in raw_notes if not note["system"]]
            human_note_creation_times.sort()
            return human_note_creation_times[0] if human_note_creation_times else None

        def _calc_first_commit_authored_at(raw_commits):
            author_times = [c["created_at"] for c in raw_commits]
            author_times.sort()
            return author_times[0] if author_times else None

        def _calc_addition_and_deletion_changed_files(raw_changes):
            additions, deletions, changed_files = 0, 0, 0
            for change in raw_changes["changes"]:
                changed_files += 1
                for line in change["diff"].split("\n"):
                    if line.startswith(("---", "@@")):
                        continue
                    if line.startswith("+"):
                        additions += 1
                    elif line.startswith("-"):
                        deletions += 1

            return additions, deletions, changed_files

        additions, deletions, changed_files = _calc_addition_and_deletion_changed_files(raw_data["mr_changes"])

        user_author_id = author_callback(
            AuthorAlias(name=raw_data["mr"]["author"]["name"], login=raw_data["mr"]["author"]["username"])
        )
        merged_by_author_id = (
            author_callback(
                AuthorAlias(name=raw_data["mr"]["merged_by"]["name"], login=raw_data["mr"]["merged_by"]["username"])
            )
            if raw_data["mr"]["merged_by"]
            else None
        )
        return PullRequest(
            repo_id=repository.id,
            number=raw_data["iid"],
            title=raw_data["mr"].get("title", "<missing title>"),
            platform="gitlab",
            id_platform=raw_data["mr"]["id"],
            api_resource_uri=f"{self.api_base_url}/projects/{raw_data['project_id']}/merge_requests/{raw_data['iid']}",
            state_platform=raw_data["mr"]["state"],
            state=PullRequestState.from_gitlab(raw_data["mr"]["state"]),
            created_at=raw_data["mr"]["created_at"],
            closed_at=raw_data["mr"]["created_at"],
            updated_at=raw_data["mr"]["updated_at"],
            merged_at=raw_data["mr"]["merged_at"],
            additions=additions,
            deletions=deletions,
            changed_files=changed_files,
            draft=raw_data["mr"]["work_in_progress"],
            user=raw_data["mr"]["author"]["username"],
            user_id_external=str(raw_data["mr"]["author"]["id"]),
            user_name_external=raw_data["mr"]["author"]["name"],
            user_username_external=raw_data["mr"]["author"]["username"],
            user_aid=user_author_id,
            commits=len(raw_data["mr_commits"]),
            merged_by=raw_data["mr"]["merged_by"]["username"] if raw_data["mr"]["merged_by"] else None,
            merged_by_aid=merged_by_author_id,
            first_reaction_at=_calc_first_reaction_at(raw_data["mr_notes"]) or raw_data["mr"]["merged_at"],
            first_commit_authored_at=_calc_first_commit_authored_at(raw_data["mr_commits"]),
            extra=raw_data,
            is_bugfix=calculate_is_bugfix([], raw_data["mr"].get("title", "<missing title>")),
        )

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
                pr_number=raw_data["iid"],
                commit_id=commit_raw["id"],
                author_name=commit_raw["author_name"],
                author_email=commit_raw.get("author_email", ""),
                author_date=commit_raw["authored_date"],
                author_login=None,
                committer_name=commit_raw["committer_name"],
                committer_email=commit_raw.get("committer_email", ""),
                committer_date=commit_raw["committed_date"],
                committer_login=None,
                created_at=commit_raw["created_at"],
                updated_at=commit_raw["created_at"],
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
            author_aid = author_callback(
                AuthorAlias(name=note_raw["author"]["name"], login=note_raw["author"]["username"])
            )
            comment = PullRequestComment(
                repo_id=repository.id,
                pr_number=raw_data["iid"],
                comment_type=str(note_raw["type"]),
                comment_id=str(note_raw["id"]),
                author_id_external=note_raw["author"]["id"],
                author_name_external=note_raw["author"]["name"],
                author_username_external=note_raw["author"]["username"],
                author_aid=author_aid,
                content=note_raw["body"],
                extra=note_raw,
                created_at=note_raw["created_at"],
                updated_at=note_raw["updated_at"],
                published_at=note_raw["updated_at"],
            )
            ret.append(comment)
        return ret
