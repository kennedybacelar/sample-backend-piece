from enum import Enum


class Entity(str, Enum):
    user = "user"
    workspace = "workspace"
    project = "project"
    team = "team"
    dashboard = "dashboard"
    chart = "chart"
    thumbnail = "thumbnail"
    membership = "membership"
    invitation = "invitation"
    credential = "credential"
    repository = "repository"
    author = "author"


class Action(str, Enum):
    create = "create"
    update = "update"
    delete = "delete"
    read = "read"
