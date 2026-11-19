import sqlite3
from collections.abc import Iterable

from delivery.envelope import Envelope, make_envelope
from ledger.section_query import query_section_at


def snapshot_payload_at(source: sqlite3.Connection, position: int,
                        sections: Iterable[str]) -> dict:
    snapshot = query_section_at(source, position, sections)
    return {
        'scope': list(snapshot.scope),
        'rows': [row.to_payload() for row in snapshot.rows],
    }


def build_initial_envelope(source: sqlite3.Connection, subscription_id: str, replica_id: str,
                           position: int, sections: Iterable[str]) -> Envelope:
    return make_envelope(subscription_id, replica_id, 1, position, 'initial',
                         snapshot_payload_at(source, position, sections))
