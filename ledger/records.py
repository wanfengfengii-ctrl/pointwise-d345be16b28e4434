import sqlite3

from ledger.history import LedgerHistory, transaction
from ledger.revisions import append_correction, append_withdrawal
from ledger.sections import require_identifier, require_section


class RecordRejected(ValueError):
    pass


def validate_reading(reading: int | None) -> None:
    if reading is not None and type(reading) is not int:
        raise RecordRejected('reading must be integer micrometres or None')


def _apply(history: LedgerHistory, operation: str, observation_id: str,
           section: str | None, reading: int | None) -> int:
    require_identifier(observation_id, 'observation_id')
    validate_reading(reading)
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


def record(source: sqlite3.Connection, operation: str, observation_id: str,
           section: str | None = None, reading: int | None = None) -> int:
    history = LedgerHistory(source)
    try:
        with transaction(source, write=True):
            return _apply(history, operation, observation_id, section, reading)
    except RecordRejected:
        raise
    except ValueError as error:
        raise RecordRejected(str(error)) from error


def _operation_fields(item) -> tuple[str, str, str | None, int | None]:
    if not isinstance(item, (tuple, list)):
        raise RecordRejected('each work order operation must be a tuple or list')
    if not 2 <= len(item) <= 4:
        raise RecordRejected('a work order operation needs 2 to 4 positional fields')
    operation, observation_id = item[0], item[1]
    section = item[2] if len(item) >= 3 else None
    reading = item[3] if len(item) >= 4 else None
    if not isinstance(operation, str):
        raise RecordRejected('work order operation name must be text')
    return operation, observation_id, section, reading


def record_work_order(source: sqlite3.Connection, operations) -> tuple[int, ...]:
    if isinstance(operations, (str, bytes)):
        raise RecordRejected('a work order is a nonempty list of operations')
    try:
        items = list(operations)
    except TypeError as error:
        raise RecordRejected('a work order is a nonempty list of operations') from error
    if not items:
        raise RecordRejected('a work order is a nonempty list of operations')
    fields = tuple(_operation_fields(item) for item in items)

    history = LedgerHistory(source)
    positions = []
    try:
        with transaction(source, write=True):
            for operation, observation_id, section, reading in fields:
                positions.append(_apply(history, operation, observation_id, section, reading))
            history.record_work_order(positions[0], positions[-1])
    except RecordRejected:
        raise
    except ValueError as error:
        raise RecordRejected(str(error)) from error
    return tuple(positions)
