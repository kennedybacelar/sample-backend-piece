from enum import Enum
from datetime import timedelta, datetime
from typing import Optional, Dict, List, Tuple, Union

from pydantic import BaseModel
from gitential2.datatypes.export import ExportableModel
from .common import IDModelMixin, DateTimeModelMixin, CoreModel, ExtraFieldMixin


class GitProtocol(str, Enum):
    ssh = "ssh"
    https = "https"


class RepositoryBase(ExtraFieldMixin, CoreModel):
    clone_url: str
    protocol: GitProtocol
    name: str = ""
    namespace: str = ""
    private: bool = False
    integration_type: Optional[str] = None
    integration_name: Optional[str] = None
    credential_id: Optional[int] = None


class RepositoryCreate(RepositoryBase):
    pass


class RepositoryUpdate(RepositoryBase):
    pass


class RepositoryInDB(IDModelMixin, DateTimeModelMixin, RepositoryBase, ExportableModel):
    def export_fields(self) -> List[str]:
        return [
            "id",
            "created_at",
            "updated_at",
            "clone_url",
            "protocol",
            "name",
            "namespace",
            "private",
            "integration_type",
            "integration_name",
            "credential_id",
            "extra",
        ]

    def export_names(self) -> Tuple[str, str]:
        return "repository", "repositories"


class RepositoryPublic(IDModelMixin, DateTimeModelMixin, RepositoryBase):
    pass


class GitRepositoryState(BaseModel):
    branches: Dict[str, str]
    tags: Dict[str, str]

    @property
    def commit_ids(self):
        return list(self.branches.values()) + list(self.tags.values())


class GitRepositoryStateChange(BaseModel):
    old_state: GitRepositoryState
    new_state: GitRepositoryState

    @property
    def new_branches(self):
        return {b: cid for b, cid in self.new_state.branches.items() if b not in self.old_state.branches}


class RepositoryStatusPhase(str, Enum):
    pending = "pending"
    clone = "clone"
    extract = "extract"
    persist = "persist"
    done = "done"


class RepositoryStatusStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    finished = "finished"


class RepositoryStatus(CoreModel):
    id: int
    name: str
    done: bool = False
    status: RepositoryStatusStatus = RepositoryStatusStatus.pending
    error: Optional[List[Union[bool, str]]] = None
    phase: RepositoryStatusPhase = RepositoryStatusPhase.pending
    clone: float = 0.0
    extract: float = 0.0
    persist: float = 0.0
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def is_stuck(self) -> bool:
        if not self.done:
            if self.started_at:
                return (datetime.utcnow() - self.started_at) > timedelta(hours=6)
            else:
                return self.status != RepositoryStatusStatus.pending
        else:
            return False

    def reset(self):
        self.status = RepositoryStatusStatus.pending
        self.phase = RepositoryStatusPhase.pending
        self.clone = 0.0
        self.extract = 0.0
        self.persist = 0.0
        self.error = None
        self.done = False
        self.started_at = None
        self.finished_at = None
        self.updated_at = datetime.utcnow()
        return self

    def cloning_started(self):
        self.started_at = datetime.utcnow()
        self.status = RepositoryStatusStatus.in_progress
        self.phase = RepositoryStatusPhase.clone
        self.clone = 0.1
        self.done = False
        self.updated_at = datetime.utcnow()

        return self

    def cloning_finished(self):
        self.status = RepositoryStatusStatus.in_progress
        self.phase = RepositoryStatusPhase.clone
        self.clone = 1.0
        self.done = False
        self.updated_at = datetime.utcnow()
        return self

    def extract_started(self):
        self.status = RepositoryStatusStatus.in_progress
        self.phase = RepositoryStatusPhase.extract
        self.extract = 0.1
        self.done = False
        self.updated_at = datetime.utcnow()
        return self

    def extract_finished(self):
        self.status = RepositoryStatusStatus.in_progress
        self.phase = RepositoryStatusPhase.extract
        self.extract = 1.0
        self.done = False
        self.updated_at = datetime.utcnow()
        return self

    def persist_started(self):
        self.status = RepositoryStatusStatus.in_progress
        self.phase = RepositoryStatusPhase.persist
        self.persist = 0.1
        self.done = False
        self.updated_at = datetime.utcnow()
        return self

    def persist_finished(self):
        self.status = RepositoryStatusStatus.finished
        self.phase = RepositoryStatusPhase.done
        self.persist = 1.0
        self.done = True
        self.finished_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        return self

    def finished_with_error(self, error_msg: str):
        self.status = RepositoryStatusStatus.finished
        self.phase = RepositoryStatusPhase.done
        self.done = True
        self.error = [True, error_msg]
        self.finished_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        return self
