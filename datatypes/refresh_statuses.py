from enum import Enum
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Union
from xmlrpc.client import Boolean
from .common import CoreModel


class RefreshStatus(str, Enum):
    up_to_date = "up_to_date"
    in_progress = "in_progress"
    error = "error"


class RefreshCommitsPhase(str, Enum):
    pending = "pending"
    cloning = "clone"
    extract = "extract"
    persist = "persist"
    done = "done"


class LegacyRepositoryStatusStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    finished = "finished"


class LegacyRepositoryRefreshStatus(CoreModel):
    id: int
    name: str
    done: bool = False
    status: LegacyRepositoryStatusStatus = LegacyRepositoryStatusStatus.pending
    error: Optional[List[Union[bool, str]]] = None
    phase: RefreshCommitsPhase = RefreshCommitsPhase.pending
    clone: float = 0.0
    extract: float = 0.0
    persist: float = 0.0
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RepositoryRefreshStatus(CoreModel):
    workspace_id: int
    repository_id: int
    repository_name: str
    commits_refresh_scheduled: bool = False

    commits_last_successful_run: Optional[datetime] = None
    commits_started: Optional[datetime] = None
    commits_last_run: Optional[datetime] = None
    commits_error: bool = False
    commits_error_msg: str = ""
    commits_in_progress: bool = False
    commits_phase: RefreshCommitsPhase = RefreshCommitsPhase.pending

    prs_refresh_scheduled: bool = False

    prs_last_successful_run: Optional[datetime] = None
    prs_started: Optional[datetime] = None
    prs_last_run: Optional[datetime] = None
    prs_in_progress: bool = False
    prs_error: bool = False
    prs_error_msg: str = ""

    def summary(self) -> Tuple[RefreshStatus, str]:
        reason = ""
        if self.prs_error or self.commits_error:
            ret_status = RefreshStatus.error
            error_msg = ",".join(
                [msg for msg in [self.commits_error_msg, self.prs_error_msg] if msg is not None]
            ).strip(",")
            reason = f"Failed to process repository: {error_msg}"

        elif (
            self.prs_in_progress
            or self.commits_in_progress
            or self.prs_refresh_scheduled
            or self.commits_refresh_scheduled
        ):
            ret_status = RefreshStatus.in_progress
        else:
            ret_status = RefreshStatus.up_to_date

        return ret_status, reason

    def to_legacy(self) -> LegacyRepositoryRefreshStatus:

        clone, extract, persist = self._calc_float_values()

        phase = self._calc_phase()
        legacy_dict = {
            "id": self.repository_id,
            "name": self.repository_name,
            "started_at": self.commits_started,
            "finished_at": self.commits_last_run,
            "status": self._calc_legacy_status(),
            "phase": phase,
            "clone": clone,
            "extract": extract,
            "persist": persist,
            "error": [self.commits_error_msg or self.prs_error_msg]
            if self.commits_error_msg or self.prs_error_msg
            else None,
            "done": (
                not self.commits_in_progress
                and not self.commits_refresh_scheduled
                and not self.prs_in_progress
                and not self.prs_refresh_scheduled
            ),
        }

        return LegacyRepositoryRefreshStatus(**legacy_dict)

    def _calc_legacy_status(self):
        if self.commits_in_progress or self.prs_in_progress:
            return LegacyRepositoryStatusStatus.in_progress
        elif self.commits_refresh_scheduled or self.prs_refresh_scheduled:
            return LegacyRepositoryStatusStatus.pending
        else:
            return LegacyRepositoryStatusStatus.finished

    def _calc_float_values(self):
        if self.commits_phase == RefreshCommitsPhase.pending or (
            not self.commits_in_progress and self.commits_refresh_scheduled
        ):
            return 0, 0, 0
        elif self.commits_phase == RefreshCommitsPhase.cloning:
            return 0.1, 0, 0
        elif self.commits_phase == RefreshCommitsPhase.extract:
            return 1.0, 0.1, 0
        elif self.commits_phase == RefreshCommitsPhase.persist:
            return 1.0, 1.0, 0.1
        elif self.commits_phase == RefreshCommitsPhase.done:
            return 1.0, 1.0, 1.0
        else:
            return 0, 0, 0

    def _calc_phase(self):
        if (not self.commits_in_progress) and self.commits_refresh_scheduled:
            return "pending"
        else:
            return self.commits_phase.value


class ITSProjectRefreshPhase(str, Enum):
    scheduled = "scheduled"
    running = "running"
    done = "done"
    unknown = "unknown"


class ITSProjectRefreshStatus(CoreModel):
    workspace_id: int
    id: int
    name: str
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_successful_at: Optional[datetime] = None
    phase: ITSProjectRefreshPhase = ITSProjectRefreshPhase.unknown
    count_recently_updated_items: int = 0
    count_processed_items: int = 0
    is_error: Boolean = False
    error_msg: Optional[str] = None

    def is_done(self) -> bool:
        return self.phase == ITSProjectRefreshPhase.done

    def summary(self) -> Tuple[RefreshStatus, str]:
        if self.phase in [ITSProjectRefreshPhase.scheduled, ITSProjectRefreshPhase.running]:
            return RefreshStatus.in_progress, ""
        elif self.phase == ITSProjectRefreshPhase.done and self.is_error:
            return RefreshStatus.error, self.error_msg or ""
        else:
            return RefreshStatus.up_to_date, ""


class ProjectRefreshStatus(CoreModel):
    workspace_id: int
    id: int
    name: str
    done: bool
    repos: List[LegacyRepositoryRefreshStatus]
    repositories: List[RepositoryRefreshStatus]
    its_projects: List[ITSProjectRefreshStatus]
    status: RefreshStatus
    reason: Optional[Dict[str, str]]
    last_refreshed_at: Optional[datetime] = None
