from typing import Callable
from gitential2.kvstore import KeyValueStore

from gitential2.settings import IntegrationType, GitentialSettings
from .gitlab import GitlabIntegration
from .github import GithubIntegration
from .linkedin import LinkedinIntegration
from .bitbucket import BitBucketIntegration
from .vsts import VSTSIntegration
from .jira import JiraIntegration

REPOSITORY_SOURCES = [IntegrationType.github, IntegrationType.gitlab, IntegrationType.bitbucket, IntegrationType.vsts]
ISSUE_SOURCES = [IntegrationType.jira, IntegrationType.vsts]


def integration_type_to_class(type_: IntegrationType) -> Callable:
    if type_ == IntegrationType.gitlab:
        return GitlabIntegration
    if type_ == IntegrationType.github:
        return GithubIntegration
    if type_ == IntegrationType.linkedin:
        return LinkedinIntegration
    if type_ == IntegrationType.bitbucket:
        return BitBucketIntegration
    if type_ == IntegrationType.vsts:
        return VSTSIntegration
    if type_ == IntegrationType.jira:
        return JiraIntegration
    raise ValueError("Invalid integration")


def init_integrations(settings: GitentialSettings, kvstore: KeyValueStore):
    ret = {}
    for name, int_settings in settings.integrations.items():
        int_cls = integration_type_to_class(int_settings.type_)
        ret[name] = int_cls(name, settings=int_settings, kvstore=kvstore)
    return ret


# from enum import Enum
# from gitential2.settings import GitentialSettings
# from .common import RepositorySource
# from .gitlab import GitLabSource


# class RepositorySourceType(str, Enum):
#     gitlab = "gitlab"


# def get_source_for(type_: RepositorySource):
#     if type_ == RepositorySourceType.gitlab:
#         return GitLabSource


# def construct_login_configuration(settings: GitentialSettings, frontend_url):
#     logins = {}
#     for source_name, source_settings in settings.repository_sources.items():
#         if source_settings.use_as_login:
#             source_cls = get_source_for(source_settings.source_type)
#             source = source_cls(source_name, settings)
#             logins[source_name] = {
#                 "login_text": source_settings.login_text,
#                 "signup_text": source_settings.signup_text,
#                 "type": source_settings.source_type,
#                 "url": source.authentication_url(frontend_url),
#             }
#     return logins
