from enum import Enum
from typing import List, Any

from fastapi import Response


class PaginatedHeaderKeys(str, Enum):
    x_total_count = "X-Total-Count"
    x_current_limit = "X-Current-Limit"
    x_current_offset = "X-Current-Offset"
    access_control_expose_headers = "Access-Control-Expose-Headers"


def get_paginated_response(response: Response, items: List[Any], total_count: int, limit: int = 100, offset: int = 0):
    response.headers[PaginatedHeaderKeys.x_total_count] = str(total_count)
    response.headers[PaginatedHeaderKeys.x_current_limit] = str(limit)
    response.headers[PaginatedHeaderKeys.x_current_offset] = str(offset)
    response.headers[PaginatedHeaderKeys.access_control_expose_headers] = ", ".join(
        [
            PaginatedHeaderKeys.x_total_count,
            PaginatedHeaderKeys.x_current_limit,
            PaginatedHeaderKeys.x_current_offset,
        ]
    )
    return items
