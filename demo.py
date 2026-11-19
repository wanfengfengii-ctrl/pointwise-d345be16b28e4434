import json
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from audit.details import read
from audit.export import export_details, export_totals
from audit.receipts import receipt
from delivery.resend import ReplyLost, deliver
from delivery.runner import forward_pending_scope_request
from issuance.increment import issue_available_increments
from issuance.subscriptions import open_source, subscribe
from ledger.records import record
from replica.receive import open_replica
from replica.scope_request import change_scope


SUBSCRIPTION = 'night'
REPLICA = 'D'


def _send(source, receiver, piece, **options):
    return deliver(source, receiver, piece.envelope_id, REPLICA, **options)


def _values(report):
    return [[row.observation_id, row.reading] for row in report.rows]


def _require(actual, expected):
    if actual != expected:
        raise AssertionError(f'expected {expected!r}, observed {actual!r}')


def _scene(case_id, inputs, expected, actual, downstream_expected, downstream_actual, paths, oracle):
    _require(actual, expected)
    _require(downstream_actual, downstream_expected)
    print('POINTWISE_SCENARIO ' + json.dumps({
        'case_id': case_id,
        'input': inputs,
        'expected': expected,
        'actual': actual,
        'downstream_expected': downstream_expected,
        'downstream_actual': downstream_actual,
        'code_paths': paths,
        'oracle': oracle,
    }, ensure_ascii=False, sort_keys=True))


def run_workday(source, receiver):
    record(source, 'register', 'E', 'east', 5)
    record(source, 'register', 'F', 'east', None)
    record(source, 'register', 'W', 'west', 17)
    initial = subscribe(source, SUBSCRIPTION, REPLICA, ['east'])
    _send(source, receiver, initial)
    report = read(receiver, REPLICA)
    observed_totals = [[item.section, item.records, item.missing, item.reading_sum]
                       for item in report.totals]
    _scene('v1', {'east': {'E': 5, 'F': None}, 'west': {'W': 17}},
           {'position': 3, 'rows': [['E', 5], ['F', None]]},
           {'position': report.position, 'rows': _values(report)},
           [['east', 2, 1, 5]], observed_totals,
           ['ledger/records.py', 'issuance/snapshot.py', 'replica/snapshot_apply.py',
            'audit/details.py', 'audit/totals.py'],
           'The two hand-listed east rows include one missing value and one value of 5 micrometres.')
    print(export_details(report), end='')
    print(export_totals(report), end='')

    record(source, 'correct', 'E', 'east', 7)
    record(source, 'withdraw', 'F')
    record(source, 'correct', 'E', 'east', 6)
    record(source, 'correct', 'E', 'east', 5)
    pieces = issue_available_increments(source, SUBSCRIPTION)
    for piece in pieces[:2]:
        _send(source, receiver, piece)
    _require(_send(source, receiver, pieces[3]).status, 'waiting')
    _require(read(receiver, REPLICA).position, 5)
    _send(source, receiver, pieces[2])
    _send(source, receiver, pieces[3])
    old = read(receiver, REPLICA)
    _require((old.position, _values(old)), (7, [['E', 5]]))

    change_scope(receiver, SUBSCRIPTION, 'R', ['west'])
    record(source, 'correct', 'W', 'west', 19)
    record(source, 'correct', 'E', 'east', 11)
    _, late_east = issue_available_increments(source, SUBSCRIPTION)
    _require(read(receiver, REPLICA), old)
    handoff = forward_pending_scope_request(source, receiver, SUBSCRIPTION)
    if handoff is None:
        raise AssertionError('pending scope has no issued handoff')
    try:
        _send(source, receiver, handoff, lose_reply=True)
    except ReplyLost:
        pass
    else:
        raise AssertionError('expected a lost activation reply')
    activated = read(receiver, REPLICA)
    first_receipt = receipt(receiver, SUBSCRIPTION, 'R')
    piece8, progress9 = issue_available_increments(source, SUBSCRIPTION)
    _send(source, receiver, piece8)
    status = _send(source, receiver, late_east).status
    after_late = read(receiver, REPLICA)
    _scene('v5', {'before': _values(old), 'target': ['west'], 'later_west_reading': 19},
           {'receipt_position': 7, 'position': 7, 'rows': [['W', 17]]},
           {'receipt_position': first_receipt.position, 'position': activated.position,
            'rows': _values(activated)},
           {'position': 8, 'rows': [['W', 19]], 'late_east': 'retired'},
           {'position': after_late.position, 'rows': _values(after_late), 'late_east': status},
           ['issuance/scope_change.py', 'replica/activation.py', 'replica/increment_apply.py',
            'audit/receipts.py'],
           'W was registered at 3, first corrected at 8, and E is outside the west scope.')
    _send(source, receiver, progress9)
    return handoff, first_receipt


def main():
    with TemporaryDirectory() as directory:
        root = Path(directory)
        with closing(open_source(root / 'source.sqlite')) as source:
            replica_path = root / 'replica.sqlite'
            with closing(open_replica(replica_path)) as receiver:
                piece, first_receipt = run_workday(source, receiver)
            with closing(open_replica(replica_path)) as reopened:
                replay = change_scope(reopened, SUBSCRIPTION, 'R', ['west'])
                _require(replay, first_receipt)
                _require(_send(source, reopened, piece).status, 'applied')
                report = read(reopened, REPLICA)
                _require((report.position, _values(report)), (9, [['W', 19]]))
                print(f'Receipt {replay.request_id} at {replay.position}; current audit at {report.position}')
                print(export_details(report), end='')


if __name__ == '__main__':
    main()
