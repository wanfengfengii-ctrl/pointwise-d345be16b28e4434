import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from os import PathLike


_DECIMAL = re.compile(r'0|-?[1-9][0-9]*')


def open_database(path: str | PathLike[str]) -> sqlite3.Connection:
    db = sqlite3.connect(path, isolation_level=None)
    try:
        db.execute('PRAGMA journal_mode = WAL').fetchone()
        db.execute('PRAGMA foreign_keys = ON')
    except BaseException:
        db.close()
        raise
    return db


@contextmanager
def transaction(db: sqlite3.Connection, *, write: bool):
    db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
    try:
        yield
        db.commit()
    except BaseException:
        if db.in_transaction:
            db.rollback()
        raise


def reading_text(value: int | None) -> str | None:
    return None if value is None else str(Decimal(value))


def reading_from_text(value: str | None) -> int | None:
    if value is None:
        return None
    if not isinstance(value, str) or _DECIMAL.fullmatch(value) is None:
        raise ValueError('reading must be canonical decimal text or null')
    return int(Decimal(value))


@dataclass(frozen=True)
class Observation:
    observation_id: str
    section: str
    reading: int | None
    source_position: int

    def to_payload(self) -> dict:
        return {
            'observation_id': self.observation_id,
            'section': self.section,
            'reading': reading_text(self.reading),
            'source_position': self.source_position,
        }


@dataclass(frozen=True)
class FactEvent:
    position: int
    operation: str
    observation_id: str
    section: str | None
    reading: int | None

    def observation(self) -> Observation | None:
        if self.section is None:
            return None
        return Observation(self.observation_id, self.section, self.reading, self.position)


class LedgerHistory:
    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def initialize(self) -> None:
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS facts (
                position INTEGER PRIMARY KEY,
                operation TEXT NOT NULL CHECK (operation IN ('register', 'correct', 'withdraw')),
                observation_id TEXT NOT NULL,
                section TEXT,
                reading TEXT,
                CHECK ((operation = 'withdraw' AND section IS NULL AND reading IS NULL)
                    OR (operation <> 'withdraw' AND section IS NOT NULL))
            )
        ''')
        self.db.execute('''
            CREATE INDEX IF NOT EXISTS facts_by_observation ON facts(observation_id, position)
        ''')

    def head_position(self) -> int:
        return self.db.execute('SELECT coalesce(max(position), 0) FROM facts').fetchone()[0]

    def append(self, operation: str, observation_id: str, section: str | None,
               reading: int | None) -> int:
        cursor = self.db.execute('''
            INSERT INTO facts(operation, observation_id, section, reading) VALUES (?, ?, ?, ?)
        ''', (operation, observation_id, section, reading_text(reading)))
        return cursor.lastrowid

    @staticmethod
    def _event(row) -> FactEvent:
        position, operation, observation_id, section, reading = row
        return FactEvent(position, operation, observation_id, section, reading_from_text(reading))

    def events_between(self, after: int, through: int) -> tuple[FactEvent, ...]:
        rows = self.db.execute('''
            SELECT position, operation, observation_id, section, reading FROM facts
            WHERE position > ? AND position <= ? ORDER BY position
        ''', (after, through))
        return tuple(self._event(row) for row in rows)

    def has_identity(self, observation_id: str) -> bool:
        return self.db.execute('''
            SELECT 1 FROM facts WHERE observation_id = ? LIMIT 1
        ''', (observation_id,)).fetchone() is not None

    def observation_at(self, observation_id: str, position: int) -> Observation | None:
        row = self.db.execute('''
            SELECT position, operation, observation_id, section, reading FROM facts
            WHERE observation_id = ? AND position <= ? ORDER BY position DESC LIMIT 1
        ''', (observation_id, position)).fetchone()
        return None if row is None else self._event(row).observation()

    def rows_at(self, position: int) -> tuple[Observation, ...]:
        visible = {}
        for event in self.events_between(0, position):
            observation = event.observation()
            if observation is None:
                visible.pop(event.observation_id, None)
            else:
                visible[event.observation_id] = observation
        return tuple(sorted(visible.values(), key=lambda row: (row.section, row.observation_id)))
