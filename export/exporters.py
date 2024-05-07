from datetime import datetime
from abc import abstractmethod
import csv
import os
import json
from collections import defaultdict

from typing import Optional, List, Dict, Any

from pathlib import Path

import sqlalchemy as sa
import xlsxwriter

from gitential2.datatypes.export import ExportableModel
from gitential2.datatypes.extraction import Langtype
from gitential2.backends.sql import json_dumps
from gitential2.backends.sql.tables import get_workspace_metadata


class Exporter:
    def export_object(self, obj: ExportableModel, fields: Optional[List[str]] = None):
        fields = fields or obj.export_fields()
        name_singular, name_plural = obj.export_names()
        exportable_dict = obj.to_exportable(fields=fields)
        self._export_low_level(
            name_singular=name_singular, name_plural=name_plural, fields=fields, exportable_dict=exportable_dict
        )

    @abstractmethod
    def _export_low_level(self, name_singular: str, name_plural: str, fields: List[str], exportable_dict: dict):
        pass

    @abstractmethod
    def get_files(self) -> List[str]:
        pass

    def close(self):
        pass


class CSVExporter(Exporter):
    def __init__(self, destination_directory: Path, prefix: str = ""):
        self.destination_directory = destination_directory
        self.prefix = prefix
        self._files: dict = {}
        self._writers: dict = {}

    def _get_filename(self, name_plural: str):
        return os.path.join(self.destination_directory, f"{self.prefix}{name_plural}.csv")

    def _get_writer(self, name_singular: str, name_plural: str, fields: List[str]):
        if name_singular not in self._writers:
            # pylint: disable=consider-using-with
            self._files[name_singular] = open(self._get_filename(name_plural), "w", encoding="utf-8")
            self._writers[name_singular] = csv.DictWriter(self._files[name_singular], fieldnames=fields)
            self._writers[name_singular].writeheader()
        return self._writers[name_singular]

    def _export_low_level(self, name_singular: str, name_plural: str, fields: List[str], exportable_dict: dict):
        writer = self._get_writer(name_singular, name_plural, fields)
        writer.writerow(exportable_dict)

    def get_files(self) -> List[str]:
        list_of_file_names: List[str] = []
        for file in self._files.values():
            list_of_file_names.append(file.name)
        return list_of_file_names

    def close(self):
        for f in self._files.values():
            f.close()


class JSONExporter(Exporter):
    def __init__(self, destination_directory: Path, prefix: str = ""):
        self.destination_directory = destination_directory
        self.prefix = prefix
        self._files: dict = {}
        self._counter: Dict[str, int] = defaultdict(int)

    def _get_filename(self, name_plural: str):
        return os.path.join(self.destination_directory, f"{self.prefix}{name_plural}.json")

    def _get_json_file(self, name_singular, name_plural):
        if name_singular not in self._files:
            # pylint: disable=consider-using-with
            self._files[name_singular] = open(self._get_filename(name_plural), "w", encoding="utf-8")
            self._files[name_singular].write("[\n")
        return self._files[name_singular]

    def _export_low_level(self, name_singular: str, name_plural: str, fields: List[str], exportable_dict: dict):
        json_str = json.dumps(exportable_dict, sort_keys=False, indent=2)
        json_file = self._get_json_file(name_singular, name_plural)
        self._counter[name_singular] += 1
        if self._counter[name_singular] > 1:
            json_file.write(",\n")
        json_file.write(json_str)

    def get_files(self) -> List[str]:
        list_of_file_names: List[str] = []
        for file in self._files.values():
            list_of_file_names.append(file.name)
        return list_of_file_names

    def close(self):
        for f in self._files.values():
            f.write("]")
            f.close()


class SQLiteExporter(Exporter):
    def __init__(self, destination_directory: Path, prefix: str = ""):
        self.destination_directory = destination_directory
        self.prefix = prefix
        self.sqlite_file = os.path.join(destination_directory, prefix + "export.sqlite")

        self._engine = sa.create_engine(
            f"sqlite:///{self.sqlite_file}",
            json_serializer=json_dumps,
        )
        self._workspace_tables, _ = get_workspace_metadata(schema=None)
        self._workspace_tables.create_all(self._engine)
        self._cache: List[tuple] = []
        self._counter = 0

    def _export_low_level(self, name_singular: str, name_plural: str, fields: List[str], exportable_dict: dict):
        self._cache.append((name_singular, name_plural, exportable_dict))
        self._counter += 1
        if self._counter == 1000:
            self._flush()

    def _flush(self):
        sp_: dict = {}
        se_: dict = {}
        for s, p, e in self._cache:
            sp_[s] = p
            if s not in se_:
                se_[s] = [e]
            else:
                se_[s].append(e)

        for s, p in sp_.items():
            self._insert_values(s, p, se_[s])

        self._cache = []
        self._counter = 0

    def _insert_values(self, name_singular: str, name_plural: str, exportables: List[dict]):
        table = self._workspace_tables.tables[name_plural]
        query = table.insert()
        values = [_convert_fields(name_singular, exportable_dict) for exportable_dict in exportables]
        try:
            with self._engine.connect() as connection:
                connection.execute(query, values)
        except sa.exc.IntegrityError:
            pass

    def get_files(self) -> List[str]:
        return [self.sqlite_file]

    def close(self):
        self._flush()


def _convert_fields(name_singular: str, exportable_dict: dict) -> dict:
    ret: dict = {}

    def _is_dt_field(name_singular, k):
        return (
            (k in ["created_at", "updated_at", "published_at"])
            or (
                name_singular
                in [
                    "calculated_commit",
                    "calculated_patch",
                    "extracted_commit",
                    "extracted_patch",
                    "extracted_patch_rewrite",
                ]
                and k in ["atime", "ctime", "date", "rewritten_atime"]
            )
            or (
                name_singular == "pull_request"
                and k
                in [
                    "closed_at",
                    "merged_at",
                    "first_reaction_at",
                    "first_commit_authored_at",
                ]
            )
            or (name_singular == "pull_request_commit" and k in ["committer_date", "author_date"])
        )

    for k, v in exportable_dict.items():
        if _is_dt_field(name_singular, k):
            if v:
                ret[k] = datetime.fromisoformat(v)
            else:
                ret[k] = None
        elif name_singular in ["calculated_patch", "extracted_patch"] and k == "langtype":
            ret[k] = Langtype(v)
        else:
            ret[k] = v
    return ret


class XlsxExporter(Exporter):
    def __init__(self, destination_directory: Path, prefix: str = ""):
        self.destination_directory = destination_directory
        self.prefix = prefix
        self.xlsx_file = os.path.join(destination_directory, prefix + "export.xlsx")
        self.workbook = xlsxwriter.Workbook(self.xlsx_file, {"nan_inf_to_errors": True, "remove_timezone": True})
        self.date_format = self.workbook.add_format({"num_format": "dd/mm/yy hh:mm:ss"})
        self.current_row: Dict[str, int] = {}

    def _get_worksheet(self, name_plural: str, fields: List[str]):
        existing_worksheet = self.workbook.get_worksheet_by_name(name_plural)
        if existing_worksheet:
            return existing_worksheet
        else:
            ws = self.workbook.add_worksheet(name_plural)
            for col, field in enumerate(fields):
                ws.write(0, col, field)
            self.current_row[name_plural] = 1

            return ws

    def _export_low_level(self, name_singular: str, name_plural: str, fields: List[str], exportable_dict: dict):
        ws = self._get_worksheet(name_plural, fields)
        col = 0
        row = self.current_row[name_plural]
        for col, f in enumerate(fields):
            value = self._prepare_value(name_singular, f, exportable_dict.get(f))
            if isinstance(value, datetime):
                ws.write_datetime(row, col, value, self.date_format)
            else:
                ws.write(row, col, value)
        self.current_row[name_plural] += 1

    def _prepare_value(self, name_singular: str, field: str, value: Any):
        def _it_was_a_datetime_field(name_singular, field):
            return (
                name_singular in ["calculated_commit", "calculated_patch", "extracted_commit", "extracted_patch"]
                and field in ["atime", "ctime", "date"]
            ) or (
                field
                in ["created_at", "updated_at", "closed_at", "merged_at", "published_at", "committer_date"]
                + ["resolution_date", "started_at", "last_comment_at", "last_change_at", "ended_at"]
            )

        def _convert_to_excel_compatible_datetime(value):
            if value:
                return datetime.fromisoformat(value)
            else:
                return None

        if _it_was_a_datetime_field(name_singular, field):
            value = _convert_to_excel_compatible_datetime(value)
        if field == "extra":
            return ""  # Ignore 'extra' fields in xlsx exports
        if isinstance(value, (dict, list)):
            return json_dumps(value)
        return value

    def get_files(self) -> List[str]:
        return [self.xlsx_file]

    def close(self):
        self.workbook.close()
