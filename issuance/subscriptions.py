import json
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from os import PathLike

from delivery.envelope import Envelope
from delivery.outbox import Outbox
from issuance.snapshot import build_initial_envelope
from ledger.history import LedgerHistory, open_database, transaction
from ledger.sections import normalize_scope, require_identifier


class SubscriptionRejected(ValueError):
    pass


@dataclass(frozen=True)
class SubscriptionState:
    subscription_id: str
    replica_id: str
    scope: tuple[str, ...]
    scope_version: int
    cursor: int


class SubscriptionStore:
    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def initialize(self) -> None:
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS subscriptions (
                subscription_id TEXT PRIMARY KEY,
                replica_id TEXT NOT NULL UNIQUE,
                scope TEXT NOT NULL,
                scope_version INTEGER NOT NULL CHECK (scope_version > 0),
                cursor INTEGER NOT NULL CHECK (cursor >= 0)
            )
        ''')
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS scope_issuances (
                subscription_id TEXT NOT NULL REFERENCES subscriptions(subscription_id),
                request_id TEXT NOT NULL,
                envelope_id TEXT NOT NULL UNIQUE REFERENCES outbox(envelope_id),
                PRIMARY KEY (subscription_id, request_id)
            )
        ''')

    def insert(self, state: SubscriptionState) -> None:
        conflict = self.db.execute('''
            SELECT 1 FROM subscriptions WHERE subscription_id = ? OR replica_id = ?
        ''', (state.subscription_id, state.replica_id)).fetchone()
        if conflict is not None:
            raise SubscriptionRejected('subscription identifier or replica already in use')
        self.db.execute('''
            INSERT INTO subscriptions(subscription_id, replica_id, scope, scope_version, cursor)
            VALUES (?, ?, ?, ?, ?)
        ''', (state.subscription_id, state.replica_id, json.dumps(state.scope),
              state.scope_version, state.cursor))

    def get(self, subscription_id: str) -> SubscriptionState:
        row = self.db.execute('''
            SELECT subscription_id, replica_id, scope, scope_version, cursor
            FROM subscriptions WHERE subscription_id = ?
        ''', (subscription_id,)).fetchone()
        if row is None:
            raise SubscriptionRejected(f'unknown subscription: {subscription_id}')
        identifier, replica_id, scope, version, cursor = row
        return SubscriptionState(identifier, replica_id, tuple(json.loads(scope)), version, cursor)

    def set_cursor(self, subscription_id: str, scope_version: int, position: int) -> None:
        result = self.db.execute('''
            UPDATE subscriptions SET cursor = ? WHERE subscription_id = ?
            AND scope_version = ? AND cursor = ?
        ''', (position, subscription_id, scope_version, position - 1))
        if result.rowcount != 1:
            raise SubscriptionRejected('issuance cursor changed or is not consecutive')

    def set_scope(self, subscription_id: str, prior_version: int, scope: tuple[str, ...],
                  cut_position: int) -> None:
        result = self.db.execute('''
            UPDATE subscriptions SET scope = ?, scope_version = scope_version + 1, cursor = ?
            WHERE subscription_id = ? AND scope_version = ? AND cursor >= ?
        ''', (json.dumps(scope), cut_position, subscription_id, prior_version, cut_position))
        if result.rowcount != 1:
            raise SubscriptionRejected('current issuance cannot hand off this scope and position')


def open_source(path: str | PathLike[str]) -> sqlite3.Connection:
    db = open_database(path)
    try:
        with transaction(db, write=True):
            LedgerHistory(db).initialize()
            Outbox(db).initialize()
            SubscriptionStore(db).initialize()
    except BaseException:
        db.close()
        raise
    return db


def subscribe(source: sqlite3.Connection, subscription_id: str, replica_id: str,
              sections: Iterable[str]) -> Envelope:
    try:
        require_identifier(subscription_id, 'subscription_id')
        require_identifier(replica_id, 'replica_id')
        scope = normalize_scope(sections)
    except ValueError as error:
        raise SubscriptionRejected(str(error)) from error
    with transaction(source, write=True):
        position = LedgerHistory(source).head_position()
        state = SubscriptionState(subscription_id, replica_id, scope, 1, position)
        SubscriptionStore(source).insert(state)
        envelope = build_initial_envelope(source, subscription_id, replica_id, position, scope)
        Outbox(source).put(envelope)
    return envelope
