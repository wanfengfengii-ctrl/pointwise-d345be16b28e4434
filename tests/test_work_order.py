import sqlite3

from audit.details import read
from delivery.envelope import EnvelopeRejected, make_envelope
from delivery.outbox import Outbox
from delivery.resend import ReplyLost, resend_pending
from delivery.runner import advance_replication
from issuance.increment import issue_available_increments, issue_next_increment
from issuance.subscriptions import SubscriptionStore
from ledger.history import LedgerHistory
from ledger.records import RecordRejected, record, record_work_order
from ledger.section_query import SourcePositionRejected, query_section_at
from replica.receive import accept_envelope
from tests.support import (
    EAST, REPLICA, SUBSCRIPTION, WEST, WorkdayCase, reject_receiver_commit, totals, values,
)

SEED = [('register', 'E', 'east', 5), ('register', 'F', 'east', None)]
WORK_ORDER = [('withdraw', 'F'), ('register', 'W', 'west', 17), ('register', 'G', 'east', 7)]


class WorkOrderLedgerTests(WorkdayCase):
    def seed_two(self):
        self.assertEqual(tuple(record(self.source, *op) for op in SEED), (1, 2))

    def test_record_work_order_occupies_one_consecutive_range(self):
        self.seed_two()
        positions = record_work_order(self.source, WORK_ORDER)
        self.assertEqual(positions, (3, 4, 5))
        self.assertEqual(LedgerHistory(self.source).head_position(), 5)
        self.assertEqual(self.source.execute('SELECT count(*) FROM work_orders').fetchone()[0], 1)
        events = LedgerHistory(self.source).events_between(0, 5)
        self.assertEqual([(event.position, event.operation) for event in events],
                         [(1, 'register'), (2, 'register'), (3, 'withdraw'),
                          (4, 'register'), (5, 'register')])
        self.assertEqual(values(query_section_at(self.source, 0, EAST).rows), ())
        self.assertEqual(values(query_section_at(self.source, 5, EAST).rows), (('E', 'east', 5, 1),
                                                                                ('G', 'east', 7, 5)))
        for position in (3, 4):
            with self.subTest(position=position), self.assertRaises(SourcePositionRejected):
                query_section_at(self.source, position, EAST)

    def test_work_order_validation_leaves_no_fact_or_boundary(self):
        self.seed_two()
        bad_orders = [
            [],
            [('correct', 'E', 'east', 8), ('correct', 'UNKNOWN', 'east', 4)],
            [('register', 'G', 'east', 7), ('register', 'G', 'east', 8)],
            [('bogus', 'G', 'east', 7)],
            [('withdraw', 'F', 'east')],
            [('register', 'G', 'east', True)],
            [('register',)],
            [('register', 'G', 'east', 7, 'extra')],
            'not-a-list',
            None,
            [('register', 'G', 'east', 7), 3],
        ]
        for order in bad_orders:
            with self.subTest(order=order), self.assertRaises(RecordRejected):
                record_work_order(self.source, order)
        history = LedgerHistory(self.source)
        self.assertEqual(history.head_position(), 2)
        self.assertIsNone(history.work_order_bounds(3))
        self.assertEqual(self.source.execute('SELECT count(*) FROM work_orders').fetchone()[0], 0)
        self.assertEqual(record(self.source, 'register', 'G', 'east', 7), 3)

    def test_later_operations_see_earlier_results_in_same_order(self):
        self.seed_two()
        self.assertEqual(record_work_order(self.source, [
            ('register', 'G', 'east', 7),
            ('correct', 'G', 'east', 8),
            ('withdraw', 'G'),
        ]), (3, 4, 5))
        self.assertEqual(values(query_section_at(self.source, 5, EAST).rows), (('E', 'east', 5, 1),
                                                                                ('F', 'east', None, 2)))
        with self.assertRaises(SourcePositionRejected):
            query_section_at(self.source, 4, EAST)

    def test_database_failure_rolls_back_the_whole_order(self):
        self.seed_two()
        before = LedgerHistory(self.source).head_position()
        with reject_receiver_commit(self.source) as denied:
            with self.assertRaises(sqlite3.DatabaseError):
                record_work_order(self.source, WORK_ORDER)
        self.assertEqual(denied, ['COMMIT'])
        self.assertEqual(LedgerHistory(self.source).head_position(), before)
        self.assertEqual(self.source.execute('SELECT count(*) FROM work_orders').fetchone()[0], 0)
        self.assertEqual(record_work_order(self.source, WORK_ORDER), (3, 4, 5))


class WorkOrderDeliveryTests(WorkdayCase):
    def seed_east_at_two(self):
        self.assertEqual(tuple(record(self.source, *op) for op in SEED), (1, 2))
        return self.subscribe()

    def install_east_at_two(self):
        piece = self.seed_east_at_two()
        self.assertEqual(self.send(piece).status, 'applied')
        self.assertEqual(read(self.receiver, REPLICA).position, 2)
        return piece

    def test_whole_order_held_until_all_scope_ready_then_advances(self):
        initial = self.seed_east_at_two()
        self.assertEqual(self.send(initial).status, 'applied')
        self.assertEqual(read(self.receiver, REPLICA).position, 2)
        record_work_order(self.source, WORK_ORDER)
        (unit,) = self.issue()
        self.assertEqual(unit.kind, 'work_order')
        self.assertEqual((unit.position, unit.payload['last_position']), (3, 5))
        self.assertEqual([step['source_position'] for step in unit.payload['steps']], [3, 4, 5])
        for step in unit.payload['steps']:
            if step['operation'] == 'upsert':
                self.assertNotEqual(step['row']['section'], 'west')
        self.assertNotIn('W', unit.payload_json)
        before = read(self.receiver, REPLICA)
        self.assertEqual((before.position, values(before.rows), totals(before)),
                         (2, (('E', 'east', 5, 1), ('F', 'east', None, 2)), (('east', 2, 1, 5),)))
        self.assertEqual(self.send(unit).status, 'applied')
        after = read(self.receiver, REPLICA)
        self.assertEqual((after.position, values(after.rows), totals(after)),
                         (5, (('E', 'east', 5, 1), ('G', 'east', 7, 5)), (('east', 2, 0, 12),)))
        self.assertEqual(after.rows, query_section_at(self.source, 5, EAST).rows)

    def test_later_unit_keeps_east_rows_and_advances_positions(self):
        self.install_east_at_two()
        record_work_order(self.source, WORK_ORDER)
        (first,) = self.issue()
        self.assertEqual(self.send(first).status, 'applied')
        record_work_order(self.source, [('correct', 'W', 'west', 19), ('register', 'X', 'west', 0)])
        (second,) = self.issue()
        self.assertEqual(second.kind, 'work_order')
        self.assertEqual(second.position, 6)
        self.assertEqual([step['operation'] for step in second.payload['steps']],
                         ['progress', 'progress'])
        self.assertEqual(self.send(second).status, 'applied')
        report = read(self.receiver, REPLICA)
        self.assertEqual((report.position, values(report.rows)),
                         (7, (('E', 'east', 5, 1), ('G', 'east', 7, 5))))

    def test_same_value_correction_advances_row_source_position(self):
        self.install_east_at_two()
        record_work_order(self.source, [('correct', 'E', 'east', 5), ('correct', 'E', 'east', 5)])
        (unit,) = self.issue()
        self.assertEqual([step['row']['source_position'] for step in unit.payload['steps']], [3, 4])
        self.assertEqual(self.send(unit).status, 'applied')
        report = read(self.receiver, REPLICA)
        self.assertEqual((report.position, values(report.rows)),
                         (4, (('E', 'east', 5, 4), ('F', 'east', None, 2))))
        self.assertEqual(report.rows, query_section_at(self.source, 4, EAST).rows)

    def test_issue_next_increment_returns_whole_order_once(self):
        self.install_east_at_two()
        record_work_order(self.source, WORK_ORDER)
        first = issue_next_increment(self.source, SUBSCRIPTION)
        self.assertEqual((first.kind, first.position), ('work_order', 3))
        self.assertIsNone(issue_next_increment(self.source, SUBSCRIPTION))
        self.assertEqual(self.send(first).status, 'applied')
        self.assertEqual(read(self.receiver, REPLICA).position, 5)

    def test_rejected_source_keeps_positions_and_detail(self):
        self.install_east_at_two()
        record_work_order(self.source, WORK_ORDER)
        with self.assertRaises(RecordRejected):
            record_work_order(self.source, [('correct', 'E', 'east', 8),
                                            ('correct', 'UNKNOWN', 'east', 4)])
        self.assertEqual(LedgerHistory(self.source).head_position(), 5)
        self.assertEqual(values(query_section_at(self.source, 5, EAST).rows),
                         (('E', 'east', 5, 1), ('G', 'east', 7, 5)))
        self.assertEqual(record(self.source, 'register', 'Y', 'east', 1), 6)

    def test_observation_moves_out_and_back_inside_one_order(self):
        self.install_east_at_two()
        record_work_order(self.source, [('correct', 'E', 'west', 99), ('correct', 'E', 'east', 5)])
        (unit,) = self.issue()
        self.assertEqual([step['operation'] for step in unit.payload['steps']],
                         ['delete', 'upsert'])
        self.assertEqual(self.send(unit).status, 'applied')
        report = read(self.receiver, REPLICA)
        self.assertEqual((report.position, values(report.rows)),
                         (4, (('E', 'east', 5, 4), ('F', 'east', None, 2))))
        with self.assertRaises(SourcePositionRejected):
            query_section_at(self.source, 3, EAST)
        self.assertEqual(values(query_section_at(self.source, 2, EAST).rows),
                         (('E', 'east', 5, 1), ('F', 'east', None, 2)))

    def test_receiver_commit_rejection_keeps_old_book_and_allows_retry(self):
        self.install_east_at_two()
        record_work_order(self.source, WORK_ORDER)
        (unit,) = self.issue()
        before = read(self.receiver, REPLICA)
        with reject_receiver_commit(self.receiver) as denied:
            with self.assertRaises(sqlite3.DatabaseError):
                self.send(unit)
        self.assertEqual(denied, ['COMMIT'])
        self.reopen()
        self.assertEqual(read(self.receiver, REPLICA), before)
        self.assertIn(unit.envelope_id, self.pending_ids())
        self.assertEqual(self.send(unit).status, 'applied')
        self.assertEqual((read(self.receiver, REPLICA).position, values(read(self.receiver, REPLICA).rows)),
                         (5, (('E', 'east', 5, 1), ('G', 'east', 7, 5))))

    def test_lost_reply_then_replay_does_not_reapply(self):
        self.install_east_at_two()
        record_work_order(self.source, WORK_ORDER)
        (unit,) = self.issue()
        with self.assertRaises(ReplyLost):
            self.send(unit, lose_reply=True)
        committed = read(self.receiver, REPLICA)
        self.assertEqual((committed.position, values(committed.rows)),
                         (5, (('E', 'east', 5, 1), ('G', 'east', 7, 5))))
        self.reopen()
        self.assertEqual(read(self.receiver, REPLICA), committed)
        self.assertEqual(self.send(unit).status, 'applied')
        self.assertEqual(read(self.receiver, REPLICA), committed)
        self.assertEqual(self.send(unit), self.send(unit))
        self.assertEqual(self.pending_ids(), ())

    def test_out_of_order_order_and_single_delta_interleave(self):
        self.install_east_at_two()
        record(self.source, 'correct', 'E', 'east', 9)
        record_work_order(self.source, WORK_ORDER)
        pieces = self.issue()
        self.assertEqual([(piece.position, piece.kind) for piece in pieces],
                         [(3, 'delta'), (4, 'work_order')])
        self.assertEqual(self.send(pieces[1]).status, 'waiting')
        self.assertEqual(read(self.receiver, REPLICA).position, 2)
        results = resend_pending(self.source, self.receiver, SUBSCRIPTION, REPLICA)
        self.assertEqual([(item.position, item.status) for item in results],
                         [(3, 'applied'), (4, 'applied')])
        report = read(self.receiver, REPLICA)
        self.assertEqual((report.position, values(report.rows)),
                         (6, (('E', 'east', 9, 3), ('G', 'east', 7, 6))))

    def test_conflicting_and_wrong_target_order_pieces_are_rejected(self):
        self.install_east_at_two()
        record_work_order(self.source, WORK_ORDER)
        (unit,) = self.issue()
        with self.assertRaises(EnvelopeRejected):
            accept_envelope(self.receiver, 'other', unit)
        conflicts = [
            {**unit.payload, 'last_position': 6},
            {**unit.payload, 'steps': unit.payload['steps'][:2]},
            {**unit.payload, 'first_position': 4},
        ]
        for payload in conflicts:
            tampered = make_envelope(unit.subscription_id, unit.replica_id, unit.scope_version,
                                     unit.position, unit.kind, payload)
            self.assertEqual(tampered.envelope_id, unit.envelope_id)
            with self.subTest(payload=payload), self.assertRaises(EnvelopeRejected):
                accept_envelope(self.receiver, REPLICA, tampered)
        self.assertEqual(read(self.receiver, REPLICA).position, 2)
        self.assertEqual(self.send(unit).status, 'applied')

    def test_scope_change_after_order_reads_complete_cut(self):
        self.install_east_at_two()
        record_work_order(self.source, WORK_ORDER)
        (unit,) = self.issue()
        self.assertEqual(self.send(unit).status, 'applied')
        self.assertEqual(read(self.receiver, REPLICA).position, 5)
        record_work_order(self.source, [('correct', 'W', 'west', 19)])
        next_piece = issue_next_increment(self.source, SUBSCRIPTION)
        self.assertIsNotNone(next_piece)
        self.assertEqual(self.send(next_piece).status, 'applied')
        report = read(self.receiver, REPLICA)
        self.assertEqual((report.position, values(report.rows)),
                         (6, (('E', 'east', 5, 1), ('G', 'east', 7, 5))))


class WorkOrderScopeTests(WorkdayCase):
    def seed_two(self):
        self.assertEqual(tuple(record(self.source, *op) for op in SEED), (1, 2))

    def test_new_install_gets_full_order_boundary(self):
        self.seed_two()
        record_work_order(self.source, WORK_ORDER)
        initial = self.subscribe()
        self.assertEqual(initial.position, 5)
        self.assertEqual(self.send(initial).status, 'applied')
        report = read(self.receiver, REPLICA)
        self.assertEqual((report.position, values(report.rows)),
                         (5, (('E', 'east', 5, 1), ('G', 'east', 7, 5))))

    def test_switch_while_replica_at_two_and_late_east_order(self):
        from audit.receipts import receipt
        from replica.scope_request import change_scope
        self.seed_two()
        self.install()
        self.assertEqual(read(self.receiver, REPLICA).position, 2)
        change_scope(self.receiver, SUBSCRIPTION, 'R', WEST)
        record_work_order(self.source, WORK_ORDER)
        old_east = self.issue()
        from delivery.runner import forward_pending_scope_request
        handoff = forward_pending_scope_request(self.source, self.receiver, SUBSCRIPTION)
        self.assertEqual(handoff.position, 2)
        self.assertEqual(self.send(handoff).status, 'applied')
        activated = read(self.receiver, REPLICA)
        self.assertEqual((activated.position, activated.scope, values(activated.rows)), (2, WEST, ()))
        self.assertEqual(receipt(self.receiver, SUBSCRIPTION, 'R').position, 2)
        new_pieces = self.issue()
        self.assertEqual(new_pieces[0].position, 3)
        self.assertIn('17', new_pieces[0].payload_json)
        self.assertEqual(self.send(new_pieces[0]).status, 'applied')
        report = read(self.receiver, REPLICA)
        self.assertEqual((report.position, values(report.rows)), (5, (('W', 'west', 17, 4),)))
        for piece in old_east:
            self.assertEqual(self.send(piece).status, 'retired')
        after_late = read(self.receiver, REPLICA)
        self.assertEqual((after_late.position, values(after_late.rows)), (5, (('W', 'west', 17, 4),)))
        self.assertEqual(receipt(self.receiver, SUBSCRIPTION, 'R').position, 2)


class WorkOrderLegacyTests(WorkdayCase):
    LEGACY = [('correct', 'E', 'east', 6), ('register', 'Z', 'east', 0)]

    def seed_two(self):
        self.assertEqual(tuple(record(self.source, *op) for op in SEED), (1, 2))

    def test_legacy_v1_pieces_interleave_with_work_order(self):
        self.seed_two()
        self.install()
        self.assertEqual(tuple(record(self.source, *op) for op in self.LEGACY), (3, 4))
        earlier, later = self.issue()
        self.assertEqual(self.send(later).status, 'waiting')
        record_work_order(self.source, WORK_ORDER)
        (unit,) = self.issue()
        self.assertEqual((unit.kind, unit.position, unit.payload['last_position']),
                         ('work_order', 5, 7))
        self.assertEqual(self.send(unit).status, 'waiting')
        self.assertEqual(read(self.receiver, REPLICA).position, 2)
        self.assertEqual(self.send(earlier).status, 'applied')
        report = read(self.receiver, REPLICA)
        self.assertEqual((report.position, values(report.rows)),
                         (7, (('E', 'east', 6, 3), ('G', 'east', 7, 7),
                              ('Z', 'east', 0, 4))))
        self.assertEqual(totals(report), (('east', 3, 0, 13),))
        for piece in (later, unit):
            self.assertEqual(self.send(piece).status, 'applied')
        self.assertEqual(self.pending_ids(), ())
