from dataclasses import replace

from delivery.envelope import Envelope, EnvelopeRejected
from delivery.inbox import Inbox
from ledger.sections import require_identifier
from replica.scope_request import ScopeRequestStore
from replica.store import ReplicaHead, ReplicaStore, payload_observation


def _delta(envelope: Envelope, scope: tuple[str, ...] | None):
    payload = envelope.payload
    operation = payload.get('operation')
    if operation == 'progress' and payload.keys() == {'operation'}:
        return operation, None
    if operation == 'upsert' and payload.keys() == {'operation', 'row'}:
        row = payload_observation(payload['row'])
        if row.source_position != envelope.position or (scope is not None and row.section not in scope):
            raise EnvelopeRejected('delta row does not belong to its position and scope')
        return operation, row
    if operation == 'delete' and payload.keys() == {'operation', 'observation_id'}:
        try:
            return operation, require_identifier(payload['observation_id'], 'observation_id')
        except ValueError as error:
            raise EnvelopeRejected(str(error)) from error
    raise EnvelopeRejected('invalid delta fields')


def drain_contiguous_increments(store: ReplicaStore, inbox: Inbox, head: ReplicaHead) -> None:
    if ScopeRequestStore(store.db).pending(head.subscription_id) is not None:
        return
    while True:
        piece = inbox.next_at(head.replica_id, head.subscription_id, head.scope_version,
                              head.position + 1)
        if piece is None:
            return
        operation, value = _delta(piece, head.scope)
        store.apply_delta(head.replica_id, operation, value)
        head = replace(head, position=piece.position)
        store.write_head(head)
        inbox.finish(piece.envelope_id, 'applied')


def apply_increment(store: ReplicaStore, inbox: Inbox, envelope: Envelope) -> None:
    head = store.head(envelope.replica_id)
    if head is None:
        if envelope.scope_version != 1 or envelope.position == 0:
            raise EnvelopeRejected('invalid delta before initial snapshot')
        _delta(envelope, None)
        return
    if head.subscription_id != envelope.subscription_id:
        raise EnvelopeRejected('delta belongs to another subscription')
    if envelope.scope_version < head.scope_version:
        inbox.finish(envelope.envelope_id, 'retired')
        return
    if envelope.scope_version > head.scope_version:
        request = ScopeRequestStore(store.db).pending(head.subscription_id)
        if (request is None or envelope.scope_version != request.prior_version + 1
                or envelope.position <= request.cut_position):
            raise EnvelopeRejected('delta has no corresponding pending scope')
        _delta(envelope, request.scope)
        return
    if envelope.position <= head.position:
        inbox.finish(envelope.envelope_id, 'retired')
        return
    _delta(envelope, head.scope)
    drain_contiguous_increments(store, inbox, head)
