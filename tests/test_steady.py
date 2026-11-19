import csv
import io
import sqlite3
from contextlib import closing

from audit.details import read
from audit.export import export_details, export_totals
from delivery.envelope import EnvelopeRejected, make_envelope
from delivery.resend import deliver, resend_pending
from ledger.history import transaction
from ledger.records import record
from ledger.section_query import query_section_at
from replica.receive import accept_envelope, open_replica
from replica.store import ReplicaStore
from tests.support import EAST, REPLICA, SUBSCRIPTION, WorkdayCase, reject_receiver_commit, totals, values


class SteadyTests(WorkdayCase):
    def test_correction_and_withdrawal_preserve_historical_rows(self):
        self.seed_initial()
        self.install()
        record(self.source, 'correct', 'E', 'east', 7)
        record(self.source, 'withdraw', 'F')
        correction, withdrawal = self.issue()
        self.assertEqual(self.send(correction).status, 'applied')
        self.assertEqual(values(self.report().rows), (('E', 'east', 7, 4), ('F', 'east', None, 2)))
        self.assertEqual(self.send(withdrawal).status, 'applied')
        self.assertEqual((self.report().position, values(self.report().rows)), (5, (('E', 'east', 7, 4),)))
        self.assertEqual(values(query_section_at(self.source, 3, EAST).rows), (
            ('E', 'east', 5, 1), ('F', 'east', None, 2),
        ))

    def test_pinned_read_exports_one_committed_view(self):
        self.seed_initial()
        self.install()
        with closing(open_replica(self.replica_path)) as reader:
            with transaction(reader, write=False):
                self.assertEqual(ReplicaStore(reader).head(REPLICA).position, 3)
                record(self.source, 'correct', 'E', 'east', 7)
                (piece,) = self.issue()
                self.assertEqual(self.send(piece).status, 'applied')
                pinned = read(reader, REPLICA)
                self.assertEqual((pinned.position, values(pinned.rows)), (3, (
                    ('E', 'east', 5, 1), ('F', 'east', None, 2),
                )))
                self.assertEqual(totals(pinned), (('east', 2, 1, 5),))
                details = list(csv.reader(io.StringIO(export_details(pinned))))
                self.assertEqual((details[0], details[2]), (['position', '3'], ['east', 'E', '5', '1']))
                exported_totals = list(csv.reader(io.StringIO(export_totals(pinned))))
                self.assertEqual((exported_totals[0], exported_totals[2]),
                                 (['position', '3'], ['east', '2', '1', '5']))
            current = read(reader, REPLICA)
        self.assertEqual((current.position, totals(current)), (4, (('east', 2, 1, 7),)))

    def _make_gap(self):
        self.seed_position_six()
        self.install()
        self.assertEqual(record(self.source, 'correct', 'E', 'east', 10), 7)
        self.assertEqual(record(self.source, 'withdraw', 'F'), 8)
        return self.issue()

    def test_out_of_order_then_duplicate(self):
        piece7, piece8 = self._make_gap()
        before = self.report()
        self.assertEqual((before.position, values(before.rows)), (6, (
            ('E', 'east', 6, 6), ('F', 'east', None, 5),
        )))
        self.assertEqual(self.send(piece8).status, 'waiting')
        self.assertEqual(self.report(), before)
        first = self.send(piece7)
        self.assertEqual(first.status, 'applied')
        self.assertEqual(self.send(piece7), first)
        self.assertEqual(self.send(piece8).status, 'applied')
        self.assertEqual((self.report().position, values(self.report().rows)), (8, (('E', 'east', 10, 7),)))
        self.assertEqual(self.pending_ids(), ())

    def test_out_of_scope_progress_completes_contiguous_prefix(self):
        self.seed_position_six()
        self.install()
        self.assertEqual(record(self.source, 'correct', 'W', 'west', 21), 7)
        self.assertEqual(record(self.source, 'correct', 'E', 'east', 12), 8)
        piece7, piece8 = self.issue()
        self.assertEqual(piece7.payload, {'operation': 'progress'})
        self.assertEqual(self.send(piece8).status, 'waiting')
        self.assertEqual(self.report().position, 6)
        self.assertEqual(self.send(piece7).status, 'applied')
        self.assertEqual((self.report().position, values(self.report().rows)), (8, (
            ('E', 'east', 12, 8), ('F', 'east', None, 5),
        )))

    def test_wrong_target_and_conflicting_content_leave_visible_book_intact(self):
        piece7, piece8 = self._make_gap()
        before = self.report()
        with self.assertRaises(EnvelopeRejected):
            deliver(self.source, self.receiver, piece7.envelope_id, 'another-replica')
        with self.assertRaises(EnvelopeRejected):
            accept_envelope(self.receiver, 'another-replica', piece7)
        self.assertEqual(self.send(piece8).status, 'waiting')
        conflicting = make_envelope(piece8.subscription_id, piece8.replica_id, piece8.scope_version,
                                    piece8.position, piece8.kind, {'operation': 'progress'})
        self.assertEqual(conflicting.envelope_id, piece8.envelope_id)
        with self.assertRaises(EnvelopeRejected):
            accept_envelope(self.receiver, REPLICA, conflicting)
        self.assertEqual(self.report(), before)
        self.assertEqual(self.pending_ids(), (piece7.envelope_id, piece8.envelope_id))
        self.assertEqual(self.send(piece7).status, 'applied')
        self.assertEqual(values(self.report().rows), (('E', 'east', 10, 7),))

    def test_receiver_commit_rejection_keeps_gap_piece_available(self):
        piece7, piece8 = self._make_gap()
        self.assertEqual(self.send(piece8).status, 'waiting')
        before = self.report()
        with reject_receiver_commit(self.receiver) as denied:
            with self.assertRaises(sqlite3.DatabaseError):
                self.send(piece7)
        self.assertEqual(denied, ['COMMIT'])
        self.reopen()
        self.assertEqual(self.report(), before)
        self.assertEqual(self.pending_ids(), (piece7.envelope_id, piece8.envelope_id))
        results = resend_pending(self.source, self.receiver, SUBSCRIPTION, REPLICA)
        self.assertEqual([(item.position, item.status) for item in results], [(7, 'applied'), (8, 'applied')])
        self.assertEqual((self.report().position, values(self.report().rows)), (8, (('E', 'east', 10, 7),)))
        self.assertEqual(self.pending_ids(), ())

    def test_scope_projection_follows_observation_moving_across_segments(self):
        self.seed_initial()
        self.install()
        record(self.source, 'correct', 'E', 'west', 9)
        record(self.source, 'correct', 'W', 'east', 13)
        leave, enter = self.issue()
        self.assertEqual(leave.payload, {'operation': 'delete', 'observation_id': 'E'})
        self.assertEqual(self.send(leave).status, 'applied')
        self.assertEqual(values(self.report().rows), (('F', 'east', None, 2),))
        self.assertEqual(self.send(enter).status, 'applied')
        self.assertEqual(values(self.report().rows), (('F', 'east', None, 2), ('W', 'east', 13, 5)))

    def test_integer_precision_survives_delivery_and_aggregation(self):
        value = -(10**80 + 19)
        record(self.source, 'register', 'E', 'east', value)
        record(self.source, 'register', 'F', 'east', 10**80)
        self.install()
        self.assertEqual(self.report().rows[0].reading, value)
        self.assertEqual(totals(self.report()), (('east', 2, 0, -19),))
        self.assertIn('east,2,0,-19', export_totals(self.report()))
