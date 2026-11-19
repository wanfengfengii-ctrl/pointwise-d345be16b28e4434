import sqlite3

from delivery.envelope import (
    DeliveryResult, Envelope, EnvelopeRejected, decode_envelope, encode_envelope,
    envelope_fingerprint,
)


class Inbox:
    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def initialize(self) -> None:
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS inbox (
                envelope_id TEXT PRIMARY KEY,
                subscription_id TEXT NOT NULL,
                replica_id TEXT NOT NULL,
                scope_version INTEGER NOT NULL,
                position INTEGER NOT NULL,
                kind TEXT NOT NULL,
                body TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('waiting', 'applied', 'retired'))
            )
        ''')
        self.db.execute('''
            CREATE INDEX IF NOT EXISTS inbox_waiting_delta
            ON inbox(replica_id, subscription_id, scope_version, position)
            WHERE kind = 'delta' AND status = 'waiting'
        ''')

    def stage(self, envelope: Envelope) -> str:
        body = encode_envelope(envelope)
        row = self.db.execute('''
            SELECT body, status FROM inbox WHERE envelope_id = ?
        ''', (envelope.envelope_id,)).fetchone()
        if row is not None:
            if row[0] != body:
                raise EnvelopeRejected('same envelope identifier has different content')
            return row[1]
        self.db.execute('''
            INSERT INTO inbox(envelope_id, subscription_id, replica_id, scope_version,
                              position, kind, body, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'waiting')
        ''', (envelope.envelope_id, envelope.subscription_id, envelope.replica_id,
              envelope.scope_version, envelope.position, envelope.kind, body))
        return 'waiting'

    def next_at(self, replica_id: str, subscription_id: str, scope_version: int,
                position: int) -> Envelope | None:
        row = self.db.execute('''
            SELECT body FROM inbox WHERE replica_id = ? AND subscription_id = ?
            AND scope_version = ? AND position = ? AND kind = 'delta' AND status = 'waiting'
        ''', (replica_id, subscription_id, scope_version, position)).fetchone()
        return None if row is None else decode_envelope(row[0])

    def outcome(self, envelope: Envelope) -> DeliveryResult:
        row = self.db.execute('''
            SELECT body, status FROM inbox WHERE envelope_id = ?
        ''', (envelope.envelope_id,)).fetchone()
        if row is None or row[0] != encode_envelope(envelope):
            raise EnvelopeRejected('outcome requested for an unstaged envelope')
        return DeliveryResult(envelope.envelope_id, envelope.subscription_id, envelope.replica_id,
                              envelope.position, row[1], envelope_fingerprint(envelope))

    def finish(self, envelope_id: str, status: str) -> None:
        if status not in ('applied', 'retired'):
            raise ValueError('a terminal status is required')
        updated = self.db.execute('''
            UPDATE inbox SET status = ? WHERE envelope_id = ? AND status = 'waiting'
        ''', (status, envelope_id))
        if updated.rowcount != 1:
            raise EnvelopeRejected('only a staged waiting envelope can be finished')

    def retire_scope(self, replica_id: str, subscription_id: str, active_version: int) -> None:
        self.db.execute('''
            UPDATE inbox SET status = 'retired' WHERE replica_id = ? AND subscription_id = ?
            AND scope_version < ? AND kind = 'delta' AND status = 'waiting'
        ''', (replica_id, subscription_id, active_version))
