from datetime import date
from .common import CoreModel


class SprintBase(CoreModel):
    date: date
    weeks: int
    pattern: str


class Sprint(SprintBase):
    pass
