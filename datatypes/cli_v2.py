from enum import Enum


class ResetType(Enum):
    full = "full"
    sql_only = "sql_only"
    redis_only = "redis_only"


class CleanupType(Enum):
    full = "full"
    commits = "commits"
    pull_requests = "pull_requests"
    its_projects = "its_projects"
    redis = "redis"


class CacheRefreshType(Enum):
    everything = "everything"
    repos = "repos"
    its_projects = "its_projects"
