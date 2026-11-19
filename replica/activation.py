from delivery.envelope import Envelope, EnvelopeRejected
from delivery.inbox import Inbox
from replica.scope_request import ScopeReceipt, ScopeRequestStore
from replica.snapshot_apply import snapshot_rows, snapshot_scope
from replica.store import ReplicaHead, ReplicaStore


class ScopeActivationRejected(EnvelopeRejected):
    pass


def activate_scope(store: ReplicaStore, inbox: Inbox, envelope: Envelope) -> ScopeReceipt:
    payload = envelope.payload
    if envelope.kind != 'cutover' or payload.keys() != {'request_id', 'prior_version', 'scope', 'rows'}:
        raise ScopeActivationRejected('invalid scope handoff fields')
    request_id = payload['request_id']
    prior_version = payload['prior_version']
    if not isinstance(request_id, str) or not request_id or type(prior_version) is not int:
        raise ScopeActivationRejected('invalid scope handoff identity')

    requests = ScopeRequestStore(store.db)
    request = requests.get(envelope.subscription_id, request_id)
    head = store.head(envelope.replica_id)
    if request is None or request.receipt is not None or head is None:
        raise ScopeActivationRejected('handoff has no corresponding pending request and replica')
    if (head.subscription_id != envelope.subscription_id or head.scope_version != prior_version
            or request.prior_version != prior_version or envelope.scope_version != prior_version + 1
            or head.position != request.cut_position or envelope.position != request.cut_position):
        raise ScopeActivationRejected('handoff position or scope qualification disagrees with request')
    try:
        scope = snapshot_scope(payload)
        if scope != request.scope:
            raise ScopeActivationRejected('handoff scope disagrees with request')
        rows = snapshot_rows(payload, scope, envelope.position)
    except ScopeActivationRejected:
        raise
    except EnvelopeRejected as error:
        raise ScopeActivationRejected(str(error)) from error

    store.replace_rows(envelope.replica_id, rows)
    store.write_head(ReplicaHead(envelope.replica_id, envelope.subscription_id, scope,
                                 envelope.scope_version, envelope.position))
    result = requests.complete(request, envelope.position)
    inbox.retire_scope(envelope.replica_id, envelope.subscription_id, envelope.scope_version)
    inbox.finish(envelope.envelope_id, 'applied')
    return result
