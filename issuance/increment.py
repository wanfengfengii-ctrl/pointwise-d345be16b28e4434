import sqlite3

from delivery.envelope import Envelope, make_envelope
from delivery.outbox import Outbox
from issuance.projection import project_fact_change
from issuance.subscriptions import SubscriptionStore
from ledger.history import LedgerHistory, transaction


def _issue_through(source: sqlite3.Connection, subscription_id: str, *,
                   one: bool) -> tuple[Envelope, ...]:
    history = LedgerHistory(source)
    subscriptions = SubscriptionStore(source)
    outbox = Outbox(source)
    with transaction(source, write=True):
        state = subscriptions.get(subscription_id)
        through = history.head_position()
        if one:
            through = min(through, state.cursor + 1)
        envelopes = []
        for event in history.events_between(state.cursor, through):
            before = history.observation_at(event.observation_id, event.position - 1)
            delta = project_fact_change(state.scope, before, event)
            envelope = make_envelope(state.subscription_id, state.replica_id, state.scope_version,
                                     event.position, 'delta', delta.to_payload())
            outbox.put(envelope)
            subscriptions.set_cursor(subscription_id, state.scope_version, event.position)
            envelopes.append(envelope)
    return tuple(envelopes)


def issue_next_increment(source: sqlite3.Connection, subscription_id: str) -> Envelope | None:
    pieces = _issue_through(source, subscription_id, one=True)
    return pieces[0] if pieces else None


def issue_available_increments(source: sqlite3.Connection,
                               subscription_id: str) -> tuple[Envelope, ...]:
    return _issue_through(source, subscription_id, one=False)
