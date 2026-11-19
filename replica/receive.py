import sqlite3
from os import PathLike

from delivery.envelope import DeliveryResult, Envelope, EnvelopeRejected
from delivery.inbox import Inbox
from ledger.history import open_database, transaction
from replica.activation import activate_scope
from replica.increment_apply import apply_increment
from replica.scope_request import ScopeRequestStore
from replica.snapshot_apply import apply_initial_snapshot
from replica.store import ReplicaStore


def open_replica(path: str | PathLike[str]) -> sqlite3.Connection:
    db = open_database(path)
    try:
        with transaction(db, write=True):
            ReplicaStore(db).initialize()
            Inbox(db).initialize()
            ScopeRequestStore(db).initialize()
    except BaseException:
        db.close()
        raise
    return db


def accept_envelope(receiver: sqlite3.Connection, replica_id: str,
                    envelope: Envelope) -> DeliveryResult:
    if envelope.replica_id != replica_id:
        raise EnvelopeRejected('incorrect destination replica')
    inbox = Inbox(receiver)
    store = ReplicaStore(receiver)
    with transaction(receiver, write=True):
        status = inbox.stage(envelope)
        if status == 'waiting':
            if envelope.kind == 'initial':
                apply_initial_snapshot(store, envelope)
                inbox.finish(envelope.envelope_id, 'applied')
            elif envelope.kind == 'delta':
                apply_increment(store, inbox, envelope)
            else:
                activate_scope(store, inbox, envelope)
        result = inbox.outcome(envelope)
    return result
