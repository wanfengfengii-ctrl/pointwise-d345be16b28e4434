import sqlite3
from collections.abc import Iterable

from delivery.envelope import Envelope, make_envelope
from delivery.outbox import Outbox
from issuance.snapshot import snapshot_payload_at
from issuance.subscriptions import SubscriptionRejected, SubscriptionStore
from ledger.history import transaction
from ledger.section_query import SourcePositionRejected
from ledger.sections import normalize_scope, require_identifier


class ScopeIssuanceRejected(ValueError):
    pass


def find_scope_issuance(source: sqlite3.Connection, subscription_id: str,
                        request_id: str) -> Envelope | None:
    row = source.execute('''
        SELECT envelope_id FROM scope_issuances WHERE subscription_id = ? AND request_id = ?
    ''', (subscription_id, request_id)).fetchone()
    return None if row is None else Outbox(source).get(row[0])


def issue_scope_change(source: sqlite3.Connection, subscription_id: str, request_id: str,
                       sections: Iterable[str], *, cut_position: int,
                       prior_version: int) -> Envelope:
    try:
        require_identifier(subscription_id, 'subscription_id')
        require_identifier(request_id, 'request_id')
        scope = normalize_scope(sections)
    except ValueError as error:
        raise ScopeIssuanceRejected(str(error)) from error
    if type(prior_version) is not int or prior_version < 1:
        raise ScopeIssuanceRejected('invalid prior scope version')
    if type(cut_position) is not int or cut_position < 0:
        raise ScopeIssuanceRejected('invalid cut position')

    subscriptions = SubscriptionStore(source)
    try:
        with transaction(source, write=True):
            previous = find_scope_issuance(source, subscription_id, request_id)
            if previous is not None:
                if (previous.position != cut_position or previous.scope_version != prior_version + 1
                        or previous.payload['scope'] != list(scope)):
                    raise ScopeIssuanceRejected('request already issued with different parameters')
                return previous

            state = subscriptions.get(subscription_id)
            if state.scope_version != prior_version or cut_position > state.cursor:
                raise ScopeIssuanceRejected('request does not match the prior issued prefix')
            payload = snapshot_payload_at(source, cut_position, scope)
            payload['request_id'] = request_id
            payload['prior_version'] = prior_version
            envelope = make_envelope(subscription_id, state.replica_id, prior_version + 1,
                                     cut_position, 'cutover', payload)
            Outbox(source).put(envelope)
            subscriptions.set_scope(subscription_id, prior_version, scope, cut_position)
            source.execute('''
                INSERT INTO scope_issuances(subscription_id, request_id, envelope_id)
                VALUES (?, ?, ?)
            ''', (subscription_id, request_id, envelope.envelope_id))
    except (SubscriptionRejected, SourcePositionRejected) as error:
        raise ScopeIssuanceRejected(str(error)) from error
    return envelope
