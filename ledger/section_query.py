import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass

from ledger.history import LedgerHistory, Observation
from ledger.sections import normalize_scope


class SourcePositionRejected(ValueError):
    pass


@dataclass(frozen=True)
class SectionSnapshot:
    position: int
    scope: tuple[str, ...]
    rows: tuple[Observation, ...]


def query_section_at(source: sqlite3.Connection, position: int,
                     sections: Iterable[str]) -> SectionSnapshot:
    history = LedgerHistory(source)
    if type(position) is not int or not 0 <= position <= history.head_position():
        raise SourcePositionRejected(f'no committed source position: {position!r}')
    scope = normalize_scope(sections)
    rows = tuple(row for row in history.rows_at(position) if row.section in scope)
    return SectionSnapshot(position, scope, rows)
