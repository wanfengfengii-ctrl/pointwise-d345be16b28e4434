import json
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass

from ledger.history import transaction
from ledger.sections import normalize_scope, require_identifier
from replica.store import ReplicaStore


class ScopeRequestRejected(ValueError):
    pass


@dataclass(frozen=True)
class ScopeReceipt:
    subscription_id: str
    request_id: str
    position: int


@dataclass(frozen=True)
class ScopeRequest:
    subscription_id: str
    request_id: str
    scope: tuple[str, ...]
    cut_position: int
    prior_version: int
    completed_position: int | None = None

    @property
    def receipt(self) -> ScopeReceipt | None:
        if self.completed_position is None:
            return None
        return ScopeReceipt(self.subscription_id, self.request_id, self.completed_position)


class ScopeRequestStore:
    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def initialize(self) -> None:
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS scope_requests (
                subscription_id TEXT NOT NULL REFERENCES replica_heads(subscription_id),
                request_id TEXT NOT NULL,
                scope TEXT NOT NULL,
                cut_position INTEGER NOT NULL CHECK (cut_position >= 0),
                prior_version INTEGER NOT NULL CHECK (prior_version > 0),
                completed_position INTEGER,
                PRIMARY KEY (subscription_id, request_id)
            )
        ''')
        self.db.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS one_pending_scope_per_subscription
            ON scope_requests(subscription_id) WHERE completed_position IS NULL
        ''')

    @staticmethod
    def _request(row) -> ScopeRequest | None:
        if row is None:
            return None
        subscription_id, request_id, scope, cut, prior, completed = row
        return ScopeRequest(subscription_id, request_id, tuple(json.loads(scope)), cut, prior, completed)

    def get(self, subscription_id: str, request_id: str) -> ScopeRequest | None:
        return self._request(self.db.execute('''
            SELECT subscription_id, request_id, scope, cut_position, prior_version, completed_position
            FROM scope_requests WHERE subscription_id = ? AND request_id = ?
        ''', (subscription_id, request_id)).fetchone())

    def pending(self, subscription_id: str) -> ScopeRequest | None:
        return self._request(self.db.execute('''
            SELECT subscription_id, request_id, scope, cut_position, prior_version, completed_position
            FROM scope_requests WHERE subscription_id = ? AND completed_position IS NULL
        ''', (subscription_id,)).fetchone())

    def insert(self, request: ScopeRequest) -> None:
        self.db.execute('''
            INSERT INTO scope_requests(subscription_id, request_id, scope, cut_position, prior_version)
            VALUES (?, ?, ?, ?, ?)
        ''', (request.subscription_id, request.request_id, json.dumps(request.scope),
              request.cut_position, request.prior_version))

    def complete(self, request: ScopeRequest, position: int) -> ScopeReceipt:
        updated = self.db.execute('''
            UPDATE scope_requests SET completed_position = ? WHERE subscription_id = ?
            AND request_id = ? AND completed_position IS NULL AND cut_position = ?
        ''', (position, request.subscription_id, request.request_id, position))
        if updated.rowcount != 1:
            raise ScopeRequestRejected('pending request does not match activation')
        return ScopeReceipt(request.subscription_id, request.request_id, position)


def change_scope(receiver: sqlite3.Connection, subscription_id: str, request_id: str,
                 sections: Iterable[str]) -> ScopeRequest | ScopeReceipt:
    try:
        require_identifier(subscription_id, 'subscription_id')
        require_identifier(request_id, 'request_id')
        scope = normalize_scope(sections)
    except ValueError as error:
        raise ScopeRequestRejected(str(error)) from error
    requests = ScopeRequestStore(receiver)
    with transaction(receiver, write=True):
        prior = requests.get(subscription_id, request_id)
        if prior is not None:
            if prior.scope != scope:
                raise ScopeRequestRejected('request identifier already has a different scope')
            return prior.receipt or prior
        head = ReplicaStore(receiver).head_for_subscription(subscription_id)
        if head is None:
            raise ScopeRequestRejected('subscription has no installed replica')
        if requests.pending(subscription_id) is not None:
            raise ScopeRequestRejected('another scope request is pending')
        request = ScopeRequest(subscription_id, request_id, scope, head.position, head.scope_version)
        requests.insert(request)
    return request
