import sqlite3
from collections.abc import Iterable

from delivery.ack import acknowledge_delivery, is_terminal_delivery
from delivery.envelope import DeliveryResult, EnvelopeRejected
from delivery.outbox import Outbox
from replica.receive import accept_envelope


class ReplyLost(ConnectionError):
    pass


def deliver(source: sqlite3.Connection, receiver: sqlite3.Connection, envelope_id: str,
            replica_id: str, *, lose_reply: bool = False) -> DeliveryResult:
    if source is receiver:
        raise ValueError('source and receiver require independent connections')
    envelope = Outbox(source).get(envelope_id)
    if envelope is None:
        raise EnvelopeRejected(f'unknown original envelope: {envelope_id}')
    if envelope.replica_id != replica_id:
        raise EnvelopeRejected('envelope was issued to a different replica')
    result = accept_envelope(receiver, replica_id, envelope)
    if lose_reply:
        raise ReplyLost(envelope_id)
    acknowledge_delivery(source, envelope, result)
    return result


def resend_pending(source: sqlite3.Connection, receiver: sqlite3.Connection,
                   subscription_id: str, replica_id: str, *,
                   hold: Iterable[str] = ()) -> tuple[DeliveryResult, ...]:
    held = frozenset(hold)
    outbox = Outbox(source)
    # A later piece may unblock an earlier waiting piece that still needs confirmation.
    candidates = tuple(piece for piece in outbox.pending_for(subscription_id)
                       if piece.envelope_id not in held)
    results = []
    while candidates:
        waiting = []
        for piece in candidates:
            result = deliver(source, receiver, piece.envelope_id, replica_id)
            results.append(result)
            if not is_terminal_delivery(result):
                waiting.append(piece)
        if len(waiting) == len(candidates):
            break
        candidates = waiting
    return tuple(results)
