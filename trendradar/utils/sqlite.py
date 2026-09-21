"""
SQLite helpers for safely executing queries with dynamic IN clauses.
"""

from collections.abc import Iterable, Iterator, Sequence
from typing import Any

IN_CLAUSE_CHUNK_SIZE = 500


def iter_chunks(
    values: Sequence[Any], size: int = IN_CLAUSE_CHUNK_SIZE
) -> Iterator[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def execute_chunked_in(
    cursor: Any,
    sql_template: str,
    values: Iterable[Any],
    extra_params: Sequence[Any] = (),
    chunk_size: int = IN_CLAUSE_CHUNK_SIZE,
) -> list[Any]:
    """
    Execute a query containing ``IN ({placeholders})`` in bounded chunks.

    Values are de-duplicated while preserving their first-seen order. Extra
    parameters are bound after the IN values for each query execution.
    """
    unique_values = list(dict.fromkeys(values))
    rows: list[Any] = []
    for chunk in iter_chunks(unique_values, chunk_size):
        placeholders = ",".join("?" * len(chunk))
        cursor.execute(
            sql_template.format(placeholders=placeholders), (*chunk, *extra_params)
        )
        rows.extend(cursor.fetchall())
    return rows
