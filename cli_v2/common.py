from enum import Enum
from typing import Union, Optional, List, Any
from pathlib import Path
from pprint import pprint
import json
from structlog import get_logger
import pandas as pd
import numpy as np
import typer
from tabulate import tabulate
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder

from gitential2.settings import load_settings
from gitential2.core.context import init_context_from_settings, GitentialContext


logger = get_logger(__name__)


class OutputFormat(str, Enum):
    json = "json"
    tabulate = "tabulate"
    csv = "csv"
    pprint = "pprint"


def get_context() -> GitentialContext:
    return init_context_from_settings(load_settings())


def validate_directory_exists(d: Path):
    if not d.exists():
        raise typer.Exit(1)
    if not d.is_dir():
        raise typer.Exit(2)


def print_results(
    results: Union[list, pd.DataFrame],
    format_: OutputFormat = OutputFormat.tabulate,
    fields: Optional[Union[List[str], str]] = None,
):
    if isinstance(fields, str):
        fields = fields.split(",")

    if format_ == OutputFormat.tabulate:
        return _print_tabulate(results, fields)
    elif format_ == OutputFormat.json:
        return _print_json(results, fields)
    elif format_ == OutputFormat.csv:
        return _print_csv(results, fields)
    else:
        return pprint(results)


def _print_tabulate(
    results: Optional[Union[list, pd.DataFrame]],
    fields: Optional[List[str]] = None,
):
    if isinstance(results, list) and results:
        head, *_ = results
        if isinstance(head, dict):
            header = list(head.keys())  # {h: h.replace("_", "\n") for h in list(head.keys())}
        elif isinstance(head, BaseModel):
            header = list(jsonable_encoder(head, include=set(fields) if fields else None).keys())
        else:
            raise ValueError("Don't know how to print ")
        header_dict = {h: h.replace("_", "\n") for h in header}
        jsonable_results = _fix_fields_ordering(
            [jsonable_encoder(e, include=set(fields) if fields else None) for e in results], fields
        )
        print(tabulate(jsonable_results, headers=header_dict, tablefmt="psql"))
    elif isinstance(results, pd.DataFrame):
        print(tabulate(results, headers="keys", tablefmt="psql"))


def _print_json(
    results: Optional[Union[list, pd.DataFrame]],
    fields: Optional[List[str]] = None,
):
    if isinstance(results, list) and results:
        jsonable_results = _fix_fields_ordering(
            [jsonable_encoder(e, include=set(fields) if fields else None) for e in results], fields
        )
        print(json.dumps(jsonable_results, indent=2))
    elif isinstance(results, pd.DataFrame):
        if results.empty:
            print({})
        ret = results.replace([np.inf, -np.inf], np.nan)
        ret = ret.where(pd.notnull(ret), None)
        jsonable = ret.to_dict(orient="list")
        print(json.dumps(jsonable, indent=2))


def _print_csv(
    results: Optional[Union[list, pd.DataFrame]],
    fields: Optional[List[str]] = None,
):
    if isinstance(results, list) and results:
        jsonable_results = _fix_fields_ordering(
            [jsonable_encoder(e, include=set(fields) if fields else None) for e in results], fields
        )
        headers = jsonable_results[0].keys()
        print(",".join(headers))
        for r in jsonable_results:
            print(",".join(_as_string(v) for v in r.values()))


def _as_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str) and "," in value:
        return f'"{value}"'
    else:
        return value


def _fix_fields_ordering(results: list, fields: Optional[List[str]]) -> list:
    if not fields:
        return results
    else:
        ret = []
        for result in results:
            ret.append({f: result[f] for f in fields})
        return ret
