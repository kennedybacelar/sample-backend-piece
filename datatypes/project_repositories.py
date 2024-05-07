from typing import Tuple, List
from gitential2.datatypes.export import ExportableModel

from .common import CoreModel, IDModelMixin


class ProjectRepositoryBase(CoreModel):
    project_id: int
    repo_id: int


class ProjectRepositoryCreate(ProjectRepositoryBase):
    pass


class ProjectRepositoryUpdate(ProjectRepositoryBase):
    pass


class ProjectRepositoryInDB(IDModelMixin, ProjectRepositoryBase, ExportableModel):
    def export_names(self) -> Tuple[str, str]:
        return ("project_repository", "project_repositories")

    def export_fields(self) -> List[str]:
        return ["id", "project_id", "repo_id"]
