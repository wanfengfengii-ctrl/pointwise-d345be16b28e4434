from ledger.history import LedgerHistory
from ledger.sections import require_section


class RevisionTargetRejected(ValueError):
    pass


def _require_active(history: LedgerHistory, observation_id: str) -> None:
    if history.observation_at(observation_id, history.head_position()) is None:
        raise RevisionTargetRejected(f'no active observation: {observation_id}')


def append_correction(history: LedgerHistory, observation_id: str, section: str,
                      reading: int | None) -> int:
    _require_active(history, observation_id)
    return history.append('correct', observation_id, require_section(section), reading)


def append_withdrawal(history: LedgerHistory, observation_id: str) -> int:
    _require_active(history, observation_id)
    return history.append('withdraw', observation_id, None, None)
