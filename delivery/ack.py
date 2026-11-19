import sqlite3

from delivery.envelope import DeliveryResult, Envelope, EnvelopeRejected, envelope_fingerprint
from delivery.outbox import Outbox
from ledger.history import transaction


def is_terminal_delivery(result: DeliveryResult) -> bool:
    return result.status in ('applied', 'retired')


def acknowledge_delivery(source: sqlite3.Connection, envelope: Envelope,
                         result: DeliveryResult) -> bool:
    if (result.envelope_id != envelope.envelope_id
            or result.subscription_id != envelope.subscription_id
            or result.replica_id != envelope.replica_id
            or result.position != envelope.position
            or result.fingerprint != envelope_fingerprint(envelope)):
        raise EnvelopeRejected('delivery result does not match original envelope')
    if result.status == 'waiting':
        return False
    if not is_terminal_delivery(result):
        raise EnvelopeRejected(f'unknown delivery status: {result.status}')
    with transaction(source, write=True):
        Outbox(source).mark_acknowledged(envelope)
    return True
