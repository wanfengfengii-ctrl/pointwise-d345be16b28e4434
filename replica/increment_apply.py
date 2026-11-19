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


def _work_order_step(envelope: Envelope, payload: dict, bounds: tuple[int, int],
                     scope: tuple[str, ...] | None):
    operation = payload.get('operation')
    allowed = {'source_position', 'operation'}
    if operation == 'upsert':
        allowed = allowed | {'row'}
    elif operation == 'delete':
        allowed = allowed | {'observation_id'}
    if payload.keys() != allowed:
        raise EnvelopeRejected('invalid work order step fields')
    position = payload['source_position']
    if type(position) is not int or not bounds[0] <= position <= bounds[1]:
        raise EnvelopeRejected('work order step is outside its unit')
    if operation == 'progress':
        return operation, None
    if operation == 'upsert':
        row = payload_observation(payload['row'])
        if row.source_position != position:
            raise EnvelopeRejected('work order row does not carry its own source position')
        if scope is not None and row.section not in scope:
            raise EnvelopeRejected('work order row is outside the subscription scope')
        return operation, row
    if operation == 'delete':
        try:
            return operation, require_identifier(payload['observation_id'], 'observation_id')
        except ValueError as error:
            raise EnvelopeRejected(str(error)) from error
    raise EnvelopeRejected('invalid work order step operation')


def _work_order_steps(envelope: Envelope, scope: tuple[str, ...] | None):
    payload = envelope.payload
    if payload.keys() != {'first_position', 'last_position', 'steps'}:
        raise EnvelopeRejected('invalid work order fields')
    first, last, steps = payload['first_position'], payload['last_position'], payload['steps']
    if (type(first) is not int or type(last) is not int or first != envelope.position
            or last < first or not isinstance(steps, list)
            or not all(isinstance(step, dict) for step in steps)
            or [step.get('source_position') for step in steps] != list(range(first, last + 1))):
        raise EnvelopeRejected('work order does not cover a consecutive position range')
    bounds = (first, last)
    return last, tuple(_work_order_step(envelope, step, bounds, scope) for step in steps)


def drain_contiguous_increments(store: ReplicaStore, inbox: Inbox, head: ReplicaHead) -> None:
    if ScopeRequestStore(store.db).pending(head.subscription_id) is not None:
        return
    while True:
        piece = inbox.next_at(head.replica_id, head.subscription_id, head.scope_version,
                              head.position + 1)
        if piece is None:
            return
        # Validate the whole next unit before touching any of its rows.
        if piece.kind == 'work_order':
            last, operations = _work_order_steps(piece, head.scope)
        else:
            last = piece.position
            operations = (_delta(piece, head.scope),)
        for operation, value in operations:
            store.apply_delta(head.replica_id, operation, value)
        head = replace(head, position=last)
        store.write_head(head)
        inbox.finish(piece.envelope_id, 'applied')


def apply_increment(store: ReplicaStore, inbox: Inbox, envelope: Envelope) -> None:
    head = store.head(envelope.replica_id)
    if head is None:
        if envelope.scope_version != 1 or envelope.position == 0:
            raise EnvelopeRejected('invalid delta before initial snapshot')
        if envelope.kind == 'work_order':
            _work_order_steps(envelope, None)
        else:
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
        if envelope.kind == 'work_order':
            _work_order_steps(envelope, request.scope)
        else:
            _delta(envelope, request.scope)
        return
    if envelope.position <= head.position:
        inbox.finish(envelope.envelope_id, 'retired')
        return
    if envelope.kind == 'work_order':
        _work_order_steps(envelope, head.scope)
    else:
        _delta(envelope, head.scope)
    drain_contiguous_increments(store, inbox, head)
