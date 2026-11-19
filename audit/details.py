import sqlite3
from dataclasses import dataclass

from audit.totals import SectionTotals, summarize_sections
from ledger.history import Observation
from replica.store import ReplicaStore


class ReplicaNotReady(LookupError):
    pass


@dataclass(frozen=True)
class AuditReport:
    replica_id: str
    subscription_id: str
    position: int
    scope: tuple[str, ...]
    rows: tuple[Observation, ...]
    totals: tuple[SectionTotals, ...]


def read(receiver: sqlite3.Connection, replica_id: str) -> AuditReport:
    view = ReplicaStore(receiver).load_view(replica_id)
    if view is None:
        raise ReplicaNotReady(f'replica is not installed: {replica_id}')
    head = view.head
    return AuditReport(head.replica_id, head.subscription_id, head.position, head.scope, view.rows,
                       summarize_sections(head.scope, view.rows))
