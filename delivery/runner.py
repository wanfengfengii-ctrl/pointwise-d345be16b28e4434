import sqlite3
from collections.abc import Iterable

from delivery.envelope import DeliveryResult, Envelope, EnvelopeRejected
from delivery.resend import resend_pending
from issuance.increment import issue_available_increments
from issuance.scope_change import issue_scope_change
from issuance.subscriptions import SubscriptionStore
from replica.scope_request import ScopeRequestStore


def forward_pending_scope_request(source: sqlite3.Connection, receiver: sqlite3.Connection,
                                  subscription_id: str) -> Envelope | None:
    request = ScopeRequestStore(receiver).pending(subscription_id)
    if request is None:
        return None
    return issue_scope_change(source, request.subscription_id, request.request_id, request.scope,
                              cut_position=request.cut_position, prior_version=request.prior_version)


def advance_replication(source: sqlite3.Connection, receiver: sqlite3.Connection,
                        subscription_id: str, replica_id: str, *,
                        hold: Iterable[str] = ()) -> tuple[DeliveryResult, ...]:
    if source is receiver:
        raise ValueError('source and receiver require independent connections')
    state = SubscriptionStore(source).get(subscription_id)
    if state.replica_id != replica_id:
        raise EnvelopeRejected('subscription was issued to a different replica')
    forward_pending_scope_request(source, receiver, subscription_id)
    issue_available_increments(source, subscription_id)
    return resend_pending(source, receiver, subscription_id, replica_id, hold=hold)
