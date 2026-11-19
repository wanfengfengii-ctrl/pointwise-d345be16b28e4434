import json
import sqlite3
from collections.abc import Iterable
from contextlib import nullcontext
from dataclasses import dataclass

from delivery.envelope import EnvelopeRejected
from ledger.history import Observation, reading_from_text, reading_text, transaction
from ledger.sections import require_identifier, require_section


@dataclass(frozen=True)
class ReplicaHead:
    replica_id: str
    subscription_id: str
    scope: tuple[str, ...]
    scope_version: int
    position: int


@dataclass(frozen=True)
class ReplicaView:
    head: ReplicaHead
    rows: tuple[Observation, ...]


def payload_observation(payload: dict) -> Observation:
    if not isinstance(payload, dict) or payload.keys() != {
        'observation_id', 'section', 'reading', 'source_position',
    }:
        raise EnvelopeRejected('invalid observation fields')
    position = payload['source_position']
    if type(position) is not int or not 1 <= position < 2**63:
        raise EnvelopeRejected('invalid observation source position')
    try:
        observation_id = require_identifier(payload['observation_id'], 'observation_id')
        section = require_section(payload['section'])
        reading = reading_from_text(payload['reading'])
    except ValueError as error:
        raise EnvelopeRejected(str(error)) from error
    return Observation(observation_id, section, reading, position)


class ReplicaStore:
    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def initialize(self) -> None:
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS replica_heads (
                replica_id TEXT PRIMARY KEY,
                subscription_id TEXT NOT NULL UNIQUE,
                scope TEXT NOT NULL,
                scope_version INTEGER NOT NULL CHECK (scope_version > 0),
                position INTEGER NOT NULL CHECK (position >= 0)
            )
        ''')
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS replica_rows (
                replica_id TEXT NOT NULL REFERENCES replica_heads(replica_id),
                observation_id TEXT NOT NULL,
                section TEXT NOT NULL,
                reading TEXT,
                source_position INTEGER NOT NULL,
                PRIMARY KEY (replica_id, observation_id)
            )
        ''')

    @staticmethod
    def _head(row) -> ReplicaHead | None:
        if row is None:
            return None
        replica_id, subscription_id, scope, version, position = row
        return ReplicaHead(replica_id, subscription_id, tuple(json.loads(scope)), version, position)

    def head(self, replica_id: str) -> ReplicaHead | None:
        return self._head(self.db.execute('''
            SELECT replica_id, subscription_id, scope, scope_version, position
            FROM replica_heads WHERE replica_id = ?
        ''', (replica_id,)).fetchone())

    def head_for_subscription(self, subscription_id: str) -> ReplicaHead | None:
        return self._head(self.db.execute('''
            SELECT replica_id, subscription_id, scope, scope_version, position
            FROM replica_heads WHERE subscription_id = ?
        ''', (subscription_id,)).fetchone())

    def load_view(self, replica_id: str) -> ReplicaView | None:
        with nullcontext() if self.db.in_transaction else transaction(self.db, write=False):
            head = self.head(replica_id)
            if head is None:
                return None
            result = self.db.execute('''
                SELECT observation_id, section, reading, source_position FROM replica_rows
                WHERE replica_id = ? ORDER BY section, observation_id
            ''', (replica_id,))
            rows = tuple(Observation(identifier, section, reading_from_text(reading), position)
                         for identifier, section, reading, position in result)
        return ReplicaView(head, rows)

    def write_head(self, head: ReplicaHead) -> None:
        self.db.execute('''
            INSERT INTO replica_heads(replica_id, subscription_id, scope, scope_version, position)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(replica_id) DO UPDATE SET scope = excluded.scope,
            scope_version = excluded.scope_version, position = excluded.position
        ''', (head.replica_id, head.subscription_id, json.dumps(head.scope),
              head.scope_version, head.position))

    def replace_rows(self, replica_id: str, rows: Iterable[Observation]) -> None:
        self.db.execute('DELETE FROM replica_rows WHERE replica_id = ?', (replica_id,))
        self.db.executemany('''
            INSERT INTO replica_rows(replica_id, observation_id, section, reading, source_position)
            VALUES (?, ?, ?, ?, ?)
        ''', ((replica_id, row.observation_id, row.section, reading_text(row.reading),
               row.source_position) for row in rows))

    def apply_delta(self, replica_id: str, operation: str,
                    value: Observation | str | None) -> None:
        if operation == 'upsert' and isinstance(value, Observation):
            self.db.execute('''
                INSERT INTO replica_rows(replica_id, observation_id, section, reading, source_position)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(replica_id, observation_id) DO UPDATE SET section = excluded.section,
                reading = excluded.reading, source_position = excluded.source_position
            ''', (replica_id, value.observation_id, value.section, reading_text(value.reading),
                  value.source_position))
        elif operation == 'delete' and isinstance(value, str):
            self.db.execute('''
                DELETE FROM replica_rows WHERE replica_id = ? AND observation_id = ?
            ''', (replica_id, value))
        elif operation != 'progress' or value is not None:
            raise EnvelopeRejected('invalid materialized row operation')
