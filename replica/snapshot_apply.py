from delivery.envelope import Envelope, EnvelopeRejected
from ledger.history import Observation
from ledger.sections import normalize_scope
from replica.store import ReplicaHead, ReplicaStore, payload_observation


class InitialSnapshotRejected(EnvelopeRejected):
    pass


def snapshot_scope(payload: dict) -> tuple[str, ...]:
    scope = payload.get('scope')
    if not isinstance(scope, list):
        raise EnvelopeRejected('snapshot scope must be a list')
    try:
        normalized = normalize_scope(scope)
    except ValueError as error:
        raise EnvelopeRejected(str(error)) from error
    if list(normalized) != scope:
        raise EnvelopeRejected('snapshot scope is not canonical')
    return normalized


def snapshot_rows(payload: dict, scope: tuple[str, ...],
                  position: int) -> tuple[Observation, ...]:
    if not isinstance(payload.get('rows'), list):
        raise EnvelopeRejected('snapshot rows must be a list')
    rows = []
    identities = set()
    for item in payload['rows']:
        row = payload_observation(item)
        if row.section not in scope or row.source_position > position:
            raise EnvelopeRejected('observation is outside snapshot range or position')
        if row.observation_id in identities:
            raise EnvelopeRejected('duplicate observation in snapshot')
        identities.add(row.observation_id)
        rows.append(row)
    return tuple(rows)


def apply_initial_snapshot(store: ReplicaStore, envelope: Envelope) -> None:
    if envelope.kind != 'initial' or envelope.scope_version != 1:
        raise InitialSnapshotRejected('invalid initial snapshot qualification')
    if (store.head(envelope.replica_id) is not None
            or store.head_for_subscription(envelope.subscription_id) is not None):
        raise InitialSnapshotRejected('initial snapshot cannot replace an installed replica')
    payload = envelope.payload
    if payload.keys() != {'scope', 'rows'}:
        raise InitialSnapshotRejected('invalid initial snapshot fields')
    try:
        scope = snapshot_scope(payload)
        rows = snapshot_rows(payload, scope, envelope.position)
    except EnvelopeRejected as error:
        raise InitialSnapshotRejected(str(error)) from error
    store.write_head(ReplicaHead(envelope.replica_id, envelope.subscription_id, scope,
                                 envelope.scope_version, envelope.position))
    store.replace_rows(envelope.replica_id, rows)
