import sqlite3

from ledger.history import LedgerHistory, transaction
from ledger.revisions import append_correction, append_withdrawal
from ledger.sections import require_identifier, require_section


class RecordRejected(ValueError):
    pass


def validate_reading(reading: int | None) -> None:
    if reading is not None and type(reading) is not int:
        raise RecordRejected('reading must be integer micrometres or None')


def record(source: sqlite3.Connection, operation: str, observation_id: str,
           section: str | None = None, reading: int | None = None) -> int:
    history = LedgerHistory(source)
    try:
        require_identifier(observation_id, 'observation_id')
        validate_reading(reading)
        with transaction(source, write=True):
            if operation == 'register':
                if history.has_identity(observation_id):
                    raise RecordRejected(f'observation already registered: {observation_id}')
                return history.append(operation, observation_id, require_section(section), reading)
            if operation == 'correct':
                return append_correction(history, observation_id, section, reading)
            if operation == 'withdraw':
                if section is not None or reading is not None:
                    raise RecordRejected('withdrawal only accepts an observation identifier')
                return append_withdrawal(history, observation_id)
            raise RecordRejected(f'unknown record operation: {operation!r}')
    except RecordRejected:
        raise
    except ValueError as error:
        raise RecordRejected(str(error)) from error
