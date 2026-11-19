import sqlite3

from delivery.envelope import (
    Envelope, EnvelopeRejected, decode_envelope, encode_envelope, same_envelope_content,
)


class Outbox:
    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def initialize(self) -> None:
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS outbox (
                sequence INTEGER PRIMARY KEY,
                envelope_id TEXT NOT NULL UNIQUE,
                subscription_id TEXT NOT NULL,
                body TEXT NOT NULL,
                acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0, 1))
            )
        ''')
        self.db.execute('''
            CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(subscription_id, sequence)
            WHERE acknowledged = 0
        ''')

    def put(self, envelope: Envelope) -> None:
        existing = self.get(envelope.envelope_id)
        if existing is not None:
            if not same_envelope_content(existing, envelope):
                raise EnvelopeRejected('outbox already has different content for this identifier')
            return
        self.db.execute('''
            INSERT INTO outbox(envelope_id, subscription_id, body) VALUES (?, ?, ?)
        ''', (envelope.envelope_id, envelope.subscription_id, encode_envelope(envelope)))

    def get(self, envelope_id: str) -> Envelope | None:
        row = self.db.execute('SELECT body FROM outbox WHERE envelope_id = ?', (envelope_id,)).fetchone()
        return None if row is None else decode_envelope(row[0])

    def pending_for(self, subscription_id: str) -> tuple[Envelope, ...]:
        rows = self.db.execute('''
            SELECT body FROM outbox WHERE subscription_id = ? AND acknowledged = 0
            ORDER BY sequence
        ''', (subscription_id,))
        return tuple(decode_envelope(row[0]) for row in rows)

    def mark_acknowledged(self, envelope: Envelope) -> None:
        updated = self.db.execute('''
            UPDATE outbox SET acknowledged = 1 WHERE envelope_id = ? AND body = ?
        ''', (envelope.envelope_id, encode_envelope(envelope)))
        if updated.rowcount != 1:
            raise EnvelopeRejected('confirmation does not identify an original outbox envelope')
