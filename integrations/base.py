import typing
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable, List, Optional, Tuple, Union

from authlib.integrations.base_client.errors import InvalidTokenError
from authlib.integrations.requests_client import OAuth2Session
from dateutil import parser
from pydantic import BaseModel
from pydantic.datetime_parse import parse_datetime
from structlog import get_logger

from gitential2.datatypes import UserInfoCreate, RepositoryInDB
from gitential2.datatypes.extraction import ExtractedKind
from gitential2.datatypes.its import ITSIssueHeader, ITSIssueAllData
from gitential2.datatypes.its_projects import ITSProjectCreate
from gitential2.datatypes.its_projects import ITSProjectInDB
from gitential2.datatypes.pull_requests import PullRequestData
from gitential2.datatypes.repositories import RepositoryCreate
from gitential2.extraction.output import OutputHandler
from gitential2.kvstore import KeyValueStore
from gitential2.settings import IntegrationSettings
from gitential2.utils import is_timestamp_within_days

logger = get_logger(__name__)


class BaseIntegration:
    def __init__(self, name, settings: IntegrationSettings, kvstore: KeyValueStore):
        self.name = name
        self.settings = settings
        self.integration_type = settings.type_
        self.kvstore = kvstore

    @property
    def is_oauth(self) -> bool:
        return False


ONE_HOUR_IN_SECONDS = 60 * 60


class OAuthLoginMixin(ABC):

    if typing.TYPE_CHECKING:
        kvstore: KeyValueStore

    @property
    def is_oauth(self) -> bool:
        return True

    @abstractmethod
    def oauth_register(self) -> dict:
        pass

    @property
    def oauth_config(self) -> dict:
        return self.oauth_register()

    def get_oauth2_client(self, **kwargs):
        params = self.oauth_register()
        params.update(kwargs)
        return OAuth2Session(**params)

    def http_get_json(self, url: str, **kwargs) -> Union[dict, list]:
        client = self.get_oauth2_client(**kwargs)
        try:
            resp = client.get(url)
            return resp.json()
        finally:
            client.close()

    def http_get_json_and_cache(self, url: str, ex_seconds: int = ONE_HOUR_IN_SECONDS, **kwargs):
        key = f"http-get-{url}"
        value = self.kvstore.get_value(key)
        if value:
            return value
        else:
            value = self.http_get_json(url, **kwargs)
            self.kvstore.set_value(key, value, ex=ex_seconds)
            return value

    @abstractmethod
    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        pass

    @abstractmethod
    def refresh_token_if_expired(self, token, update_token: Callable) -> Tuple[bool, dict]:
        pass

    def refresh_token(self, token) -> Optional[dict]:
        client = self.get_oauth2_client(token=token)
        refresh_response = client.refresh_token(
            self.oauth_config["access_token_url"], refresh_token=token["refresh_token"]
        )
        client.close()
        if "access_token" in refresh_response:
            return {f: refresh_response.get(f) for f in ["access_token", "refresh_token", "expires_at"]}
        else:
            return None

    def check_token(self, token) -> bool:
        client = self.get_oauth2_client(token=token)
        try:
            resp = client.get(self.oauth_config["userinfo_endpoint"])
        except InvalidTokenError:
            return False
        finally:
            client.close()
        return resp.status_code == 200


class CollectPRsResult(BaseModel):
    prs_collected: List[int]
    prs_left: List[int]
    prs_failed: List[int]


class GitProviderMixin(ABC):
    @abstractmethod
    def get_client(self, token, update_token) -> OAuth2Session:
        pass

    def collect_pull_requests(
        self,
        repository: RepositoryInDB,
        token: dict,
        update_token: Callable,
        output: OutputHandler,
        author_callback: Callable,
        prs_we_already_have: Optional[dict] = None,
        limit: int = 200,
        repo_analysis_limit_in_days: Optional[int] = None,
    ) -> CollectPRsResult:
        client = self.get_client(token=token, update_token=update_token)
        ret = CollectPRsResult(prs_collected=[], prs_left=[], prs_failed=[])

        logger.info(
            "Started collecting PRs",
            repository_name=repository.name,
            repository_id=repository.id,
        )

        if not self._check_rate_limit(token, update_token):
            return ret

        raw_prs = self._collect_raw_pull_requests(repository, client, repo_analysis_limit_in_days)
        logger.debug("Raw PRs collected", raw_prs=raw_prs)

        prs_needs_update = [
            pr
            for pr in raw_prs
            if self._is_pr_need_to_be_updated(
                pr=pr, prs_we_already_have=prs_we_already_have, repo_analysis_limit_in_days=repo_analysis_limit_in_days
            )
        ]

        logger.info(
            "PRs needs update/collect",
            repository_name=repository.name,
            repository_id=repository.id,
            pr_numbers=[self._raw_pr_number_and_updated_at(pr)[0] for pr in prs_needs_update],
        )
        if not self._check_rate_limit(token, update_token):
            ret.prs_left = [self._raw_pr_number_and_updated_at(pr)[0] for pr in prs_needs_update]
            return ret

        counter = 0
        for pr in prs_needs_update:
            pr_number, _ = self._raw_pr_number_and_updated_at(pr)
            if counter >= limit:
                ret.prs_left.append(pr_number)
            else:
                pr_data = self.collect_pull_request(repository, token, update_token, output, author_callback, pr_number)
                if pr_data:
                    ret.prs_collected.append(pr_number)
                else:
                    ret.prs_failed.append(pr_number)
            counter += 1

        return ret

    def collect_pull_request(
        self,
        repository: RepositoryInDB,
        token: dict,
        update_token: Callable,
        output: OutputHandler,
        author_callback: Callable,
        pr_number: int,
    ) -> Optional[PullRequestData]:
        client = self.get_client(token=token, update_token=update_token)
        raw_data = None
        logger.info(
            "Started collection data for PR",
            repository_name=repository.name,
            repository_id=repository.id,
            pr_number=pr_number,
        )
        try:
            raw_data = self._collect_raw_pull_request(repository, pr_number, client)
            pr_data = self._tranform_to_pr_data(repository, pr_number, raw_data, author_callback)

            output.write(ExtractedKind.PULL_REQUEST, pr_data.pr)
            for commit in pr_data.commits:
                output.write(ExtractedKind.PULL_REQUEST_COMMIT, commit)
            for comment in pr_data.comments:
                output.write(ExtractedKind.PULL_REQUEST_COMMENT, comment)
            for label in pr_data.labels:
                output.write(ExtractedKind.PULL_REQUEST_LABEL, label)

            logger.debug("Updated/extracted pr", pr_number=pr_number, pr_data=pr_data)
            return pr_data
        except Exception:  # pylint: disable=broad-except
            logger.exception("Failed to extract PR", pr_number=pr_number, raw_data=raw_data)
            return None
        finally:
            client.close()

    def _is_pr_need_to_be_updated(
        self,
        pr,
        prs_we_already_have: Optional[dict] = None,
        repo_analysis_limit_in_days: Optional[int] = None,
    ) -> bool:
        def get_created_at_timestamp_of_pr() -> Optional[float]:
            result = None
            date_time_str = pr.get("created_at") or pr.get("created_on") or pr.get("creationDate")
            try:
                result = parser.parse(date_time_str).timestamp()
            except ValueError as e:
                logger.error("Not able to parse created_at for pr!", exception=e)
            return result

        def is_pr_within_date_limit() -> bool:
            result = True
            if repo_analysis_limit_in_days:
                pr_created_at_timestamp = get_created_at_timestamp_of_pr()
                result = (
                    is_timestamp_within_days(pr_created_at_timestamp, repo_analysis_limit_in_days)
                    if pr_created_at_timestamp
                    else False
                )
            return result

        def is_pr_up_to_date() -> bool:
            pr_number, updated_at = self._raw_pr_number_and_updated_at(pr)
            return (
                prs_we_already_have is not None
                and pr_number in prs_we_already_have
                and parse_datetime(prs_we_already_have[pr_number]) == updated_at
            )

        return not is_pr_up_to_date() and is_pr_within_date_limit()

    # pylint: disable=unused-argument
    def _check_rate_limit(self, token, update_token):
        return True

    @abstractmethod
    def _collect_raw_pull_requests(
        self, repository: RepositoryInDB, client, repo_analysis_limit_in_days: Optional[int] = None
    ) -> list:
        pass

    @abstractmethod
    def _raw_pr_number_and_updated_at(self, raw_pr: dict) -> Tuple[int, datetime]:
        pass

    @abstractmethod
    def _collect_raw_pull_request(
        self, repository: RepositoryInDB, pr_number: int, client, repo_analysis_limit_in_days: Optional[int] = None
    ) -> dict:
        pass

    @abstractmethod
    def _tranform_to_pr_data(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict, author_callback: Callable
    ) -> PullRequestData:
        pass

    # @abstractmethod
    # def recalculate_pull_request(
    #     self,
    #     pr: PullRequest,
    #     repository: RepositoryInDB,
    #     token: dict,
    #     update_token: Callable,
    #     output: OutputHandler,
    # ) -> CollectPRsResult:
    #     pass

    @abstractmethod
    def get_newest_repos_since_last_refresh(
        self,
        token,
        update_token,
        last_refresh: datetime,
        provider_user_id: Optional[str],
        user_organization_names: Optional[List[str]],
    ) -> List[RepositoryCreate]:
        pass

    @abstractmethod
    def list_available_private_repositories(
        self, token, update_token, provider_user_id: Optional[str], user_organization_name_list: Optional[List[str]]
    ) -> List[RepositoryCreate]:
        pass

    @abstractmethod
    def search_public_repositories(
        self, query: str, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        pass


class ITSProviderMixin(ABC):
    @abstractmethod
    def list_available_its_projects(
        self, token: dict, update_token, provider_user_id: Optional[str]
    ) -> List[ITSProjectCreate]:
        pass

    @abstractmethod
    def list_recently_updated_issues(
        self, token, its_project: ITSProjectInDB, date_from: Optional[datetime] = None
    ) -> List[ITSIssueHeader]:
        pass

    @abstractmethod
    def list_all_issues_for_project(
        self,
        token,
        its_project: ITSProjectInDB,
        date_from: Optional[datetime] = None,
    ) -> List[ITSIssueHeader]:
        pass

    @abstractmethod
    def get_all_data_for_issue(
        self, token, its_project: ITSProjectInDB, issue_id_or_key: str, developer_map_callback: Callable
    ) -> ITSIssueAllData:
        pass
