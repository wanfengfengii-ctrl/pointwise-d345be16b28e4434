import sqlite3

from delivery.envelope import Envelope, make_envelope
from delivery.outbox import Outbox
from issuance.projection import project_fact_change, project_unit_steps
from issuance.subscriptions import SubscriptionRejected, SubscriptionStore
from ledger.history import LedgerHistory, transaction


def _issue_units(source: sqlite3.Connection, subscription_id: str, *,
                 one: bool) -> tuple[Envelope, ...]:
    history = LedgerHistory(source)
    subscriptions = SubscriptionStore(source)
    outbox = Outbox(source)
    envelopes = []
    with transaction(source, write=True):
        state = subscriptions.get(subscription_id)
        while True:
            cursor = subscriptions.get(subscription_id).cursor
            through = history.head_position()
            if through <= cursor:
                break
            first = cursor + 1
            bounds = history.work_order_bounds(first)
            if bounds is not None and bounds[0] != first:
                raise SubscriptionRejected(
                    f'issuance cannot resume inside a work order: {first}')
            if bounds is not None:
                last = bounds[1]
                events = history.events_between(cursor, last)
                payload = {
                    'first_position': first,
                    'last_position': last,
                    'steps': project_unit_steps(state.scope, events, history),
                }
                envelope = make_envelope(state.subscription_id, state.replica_id,
                                         state.scope_version, first, 'work_order', payload)
            else:
                last = first
                (event,) = history.events_between(cursor, first)
                before = history.observation_at(event.observation_id, event.position - 1)
                payload = project_fact_change(state.scope, before, event)
                envelope = make_envelope(state.subscription_id, state.replica_id,
                                         state.scope_version, first, 'delta', payload)
            outbox.put(envelope)
            subscriptions.advance_cursor(subscription_id, state.scope_version, cursor, last)
            envelopes.append(envelope)
            if one:
                break
    return tuple(envelopes)


def issue_next_increment(source: sqlite3.Connection, subscription_id: str) -> Envelope | None:
    pieces = _issue_units(source, subscription_id, one=True)
    return pieces[0] if pieces else None


def issue_available_increments(source: sqlite3.Connection,
                               subscription_id: str) -> tuple[Envelope, ...]:
    return _issue_units(source, subscription_id, one=False)
