from __future__ import annotations
from enum import Enum
from typing import Dict, Union, List, Optional, Literal
from pydantic import BaseModel, Field


# basic types

DQStaticValueExpr = Union[bool, int, str, List[int], List[str]]


class DQSourceName(str, Enum):
    # its
    its_issues = "its_issues"
    its_issue_comments = "its_issue_comments"
    its_issue_linked_issue = "its_issue_linked_issue"
    # pull requests
    pull_requests = "pull_requests"
    pull_request_comments = "pull_request_comments"
    pull_request_commits = "pull_request_commits"
    # commits and patches
    calculated_commits = "calculated_commits"
    calculated_patches = "calculated_patches"
    # deploy_commits
    deploy_commits = "deploy_commits"


DQ_ITS_SOURCE_NAMES = [DQSourceName.its_issues, DQSourceName.its_issue_comments, DQSourceName.its_issue_linked_issue]


class DQType(str, Enum):
    select = "select"
    aggregate = "aggregate"


class DQColumnAttrName(str, Enum):
    # date rounding
    ROUND_TO_HOUR = "round_to_hour"
    ROUND_TO_DAY = "round_to_day"
    ROUND_TO_WEEK = "round_to_week"
    ROUND_TO_MONTH = "round_to_month"
    ROUND_TO_YEAR = "round_to_year"
    EPOCH_SECONDS = "epoch_seconds"

    # date categorization
    TO_DAY_OF_WEEK = "to_day_of_week"
    TO_HOUR_OF_DAY = "to_hour_of_day"


class DQFunctionName(str, Enum):
    # aggregations
    MEAN = "mean"
    SUM = "sum"
    COUNT = "count"

    # filtering
    EQ = "eq"
    NEQ = "neq"
    LT = "lt"
    LTE = "lte"
    GT = "gt"
    GTE = "gte"
    IN = "in"
    NIN = "nin"
    BETWEEN = "between"

    # mathematical operations
    MUL = "mul"
    DIV = "div"
    ADD = "add"
    SUB = "sub"

    # null check
    ISNULL = "isnull"
    NOTNULL = "notnull"


# Column definitions


class DQSingleColumnExpr(BaseModel):
    col: str
    as_: Optional[str] = Field(None, alias="as")
    attr: Optional[DQColumnAttrName] = None


class DQFnColumnExpr(BaseModel):
    fn: DQFunctionName
    args: List["DQColumnExpr"] = Field(default_factory=list)
    as_: Optional[str] = Field(None, alias="as")
    attr: Optional[DQColumnAttrName] = None


DQColumnExpr = Union[DQSingleColumnExpr, DQFnColumnExpr, DQStaticValueExpr]

DQFnColumnExpr.update_forward_refs()


class DQSortyByColumnExpr(DQSingleColumnExpr):
    desc: bool = False


# Aliases for Expr definitions

DQFilterExpr = DQFnColumnExpr
DQDimensionExpr = DQColumnExpr
DQSelectionExpr = DQColumnExpr
DQSortByExpr = DQSortyByColumnExpr


# Base DataQuery definition


class DataQuery(BaseModel):
    query_type: DQType
    source_name: DQSourceName
    filters: List[DQFilterExpr] = Field(default_factory=list)
    dimensions: List[DQDimensionExpr] = Field(default_factory=list)
    selections: List[DQSelectionExpr]
    sort_by: List[DQSortByExpr] = Field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None


class DQResultOrientation(str, Enum):
    DICT = "dict"
    LIST = "list"
    SERIES = "series"
    SPLIT = "split"
    TIGHT = "tight"
    RECORDS = "records"

    @property
    def as_literal(self) -> Literal["dict", "list", "series", "split", "records", "index"]:
        if self.value == "dict":
            return "dict"
        if self.value == "list":
            return "list"
        if self.value == "series":
            return "series"
        if self.value == "split":
            return "split"
        if self.value == "records":
            return "records"
        else:
            return "index"


class DQResult(BaseModel):
    total: Optional[int] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    orientation: DQResultOrientation = DQResultOrientation.LIST
    results: Union[list, dict]


MultiQuery = Dict[str, DataQuery]
