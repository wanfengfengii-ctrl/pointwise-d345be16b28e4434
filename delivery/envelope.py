import hashlib
import json
from dataclasses import dataclass


class EnvelopeRejected(ValueError):
    pass


def _canonical(value) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    except (TypeError, ValueError) as error:
        raise EnvelopeRejected('envelope is not JSON encodable') from error


@dataclass(frozen=True)
class Envelope:
    envelope_id: str
    subscription_id: str
    replica_id: str
    scope_version: int
    position: int
    kind: str
    payload_json: str

    @property
    def payload(self) -> dict:
        try:
            value = json.loads(self.payload_json)
        except (ValueError, TypeError) as error:
            raise EnvelopeRejected('invalid envelope payload') from error
        if not isinstance(value, dict):
            raise EnvelopeRejected('envelope payload must be an object')
        return value


@dataclass(frozen=True)
class DeliveryResult:
    envelope_id: str
    subscription_id: str
    replica_id: str
    position: int
    status: str
    fingerprint: str


def make_envelope(subscription_id: str, replica_id: str, scope_version: int,
                  position: int, kind: str, payload: dict) -> Envelope:
    if any(not isinstance(value, str) or not value for value in (subscription_id, replica_id)):
        raise EnvelopeRejected('envelope requires subscription and replica identifiers')
    if type(scope_version) is not int or not 1 <= scope_version < 2**63:
        raise EnvelopeRejected('invalid scope version')
    if type(position) is not int or not 0 <= position < 2**63:
        raise EnvelopeRejected('invalid envelope position')
    if kind not in ('initial', 'delta', 'cutover') or not isinstance(payload, dict):
        raise EnvelopeRejected('invalid envelope kind or payload')
    identity = _canonical([subscription_id, scope_version, kind, position])
    number = hashlib.sha256(identity.encode('ascii')).hexdigest()
    return Envelope(number, subscription_id, replica_id, scope_version, position, kind,
                    _canonical(payload))


def encode_envelope(envelope: Envelope) -> str:
    payload = envelope.payload
    canonical = make_envelope(envelope.subscription_id, envelope.replica_id,
                              envelope.scope_version, envelope.position, envelope.kind, payload)
    if envelope != canonical:
        raise EnvelopeRejected('envelope identity or canonical payload is inconsistent')
    return _canonical({
        'envelope_id': envelope.envelope_id,
        'subscription_id': envelope.subscription_id,
        'replica_id': envelope.replica_id,
        'scope_version': envelope.scope_version,
        'position': envelope.position,
        'kind': envelope.kind,
        'payload': payload,
    })


def decode_envelope(encoded: str) -> Envelope:
    try:
        fields = json.loads(encoded)
        number = fields.pop('envelope_id')
        envelope = make_envelope(**fields)
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise EnvelopeRejected('invalid encoded envelope') from error
    if number != envelope.envelope_id:
        raise EnvelopeRejected('encoded envelope has the wrong identifier')
    return envelope


def same_envelope_content(left: Envelope, right: Envelope) -> bool:
    return encode_envelope(left) == encode_envelope(right)


def envelope_fingerprint(envelope: Envelope) -> str:
    return hashlib.sha256(encode_envelope(envelope).encode('ascii')).hexdigest()
