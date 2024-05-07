from typing import Tuple, List
from gitential2.datatypes.export import ExportableModel

from .common import CoreModel, IDModelMixin


class ProjectITSProjectBase(CoreModel):
    project_id: int
    itsp_id: int


class ProjectITSProjectCreate(ProjectITSProjectBase):
    pass


class ProjectITSProjectUpdate(ProjectITSProjectBase):
    pass


class ProjectITSProjectInDB(IDModelMixin, ProjectITSProjectBase, ExportableModel):
    def export_names(self) -> Tuple[str, str]:
        return ("project_its_project", "project_its_projects")

    def export_fields(self) -> List[str]:
        return ["id", "project_id", "itsp_id"]
