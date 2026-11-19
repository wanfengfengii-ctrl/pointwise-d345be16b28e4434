from audit.receipts import ReceiptPending, receipt
from delivery.envelope import make_envelope
from delivery.runner import advance_replication, forward_pending_scope_request
from issuance.scope_change import ScopeIssuanceRejected, find_scope_issuance, issue_scope_change
from ledger.records import record
from ledger.section_query import query_section_at
from replica.activation import ScopeActivationRejected
from replica.receive import accept_envelope
from replica.scope_request import ScopeRequest, ScopeRequestRejected, change_scope
from tests.support import EAST, REPLICA, SUBSCRIPTION, WEST, WorkdayCase, totals, values


class SwitchTests(WorkdayCase):
    def _forward(self, request_id='R'):
        piece = forward_pending_scope_request(self.source, self.receiver, SUBSCRIPTION)
        self.assertIsNotNone(piece, 'SCOPE_CUTOVER_MISMATCH')
        self.assertEqual(find_scope_issuance(self.source, SUBSCRIPTION, request_id), piece)
        return piece

    def test_waiting_then_activate(self):
        self.install_through_seven()
        before = self.report()
        accepted = change_scope(self.receiver, SUBSCRIPTION, 'R', WEST)
        self.assertIsInstance(accepted, ScopeRequest)
        with self.assertRaises(ReceiptPending):
            receipt(self.receiver, SUBSCRIPTION, 'R')
        self.assertEqual(self.report(), before)
        cutover = self._forward()
        self.assertEqual(self._forward(), cutover)
        self.assertEqual((cutover.position, cutover.payload['rows']), (7, [
            {'observation_id': 'W', 'section': 'west', 'reading': '17', 'source_position': 3},
        ]), 'SCOPE_CUTOVER_MISMATCH')
        self.assertEqual(record(self.source, 'correct', 'W', 'west', 19), 8)
        self.assertEqual(self.report(), before)
        self.assertEqual(self.send(cutover).status, 'applied', 'SCOPE_CUTOVER_MISMATCH')
        original = receipt(self.receiver, SUBSCRIPTION, 'R')
        report = self.report()
        self.assertEqual((original.request_id, original.position, report.position, report.scope,
                          values(report.rows), totals(report)),
                         ('R', 7, 7, WEST, (('W', 'west', 17, 3),), (('west', 1, 0, 17),)),
                         'SCOPE_CUTOVER_MISMATCH')
        (piece8,) = self.issue()
        self.assertEqual(self.send(piece8).status, 'applied')
        self.assertEqual((self.report().position, values(self.report().rows)), (8, (('W', 'west', 19, 8),)))
        self.assertEqual(record(self.source, 'withdraw', 'W'), 9)
        (piece9,) = self.issue()
        self.assertEqual(self.send(piece9).status, 'applied')
        self.assertEqual((self.report().position, self.report().rows, totals(self.report())),
                         (9, (), (('west', 0, 0, 0),)))

    def test_receiver_behind_and_future_scope_piece_arrives_before_activation(self):
        record(self.source, 'register', 'E', 'east', 5)
        for reading in (6, 7, 8, 9, 5):
            record(self.source, 'correct', 'E', 'east', reading)
        initial = self.install()
        self.assertEqual(initial.position, 6)
        self.assertEqual(record(self.source, 'register', 'W', 'west', 17), 7)
        (old7,) = self.issue()
        change_scope(self.receiver, SUBSCRIPTION, 'R', WEST)
        cutover = self._forward()
        self.assertIn(cutover.position, (6, 7))
        self.send(old7)
        self.assertEqual(record(self.source, 'correct', 'W', 'west', 19), 8)
        new = self.issue()
        self.assertEqual([piece.position for piece in new], list(range(cutover.position + 1, 9)))
        self.assertEqual(self.send(new[-1]).status, 'waiting')
        waiting = self.report()
        self.assertEqual((waiting.scope, values(waiting.rows)), (EAST, (('E', 'east', 5, 6),)))
        self.assertLessEqual(waiting.position, cutover.position)
        self.assertEqual(self.send(cutover).status, 'applied')
        original = receipt(self.receiver, SUBSCRIPTION, 'R')
        installed = self.report()
        self.assertEqual((installed.position, installed.scope), (original.position, WEST))
        expected = () if original.position == 6 else (('W', 'west', 17, 7),)
        self.assertEqual(values(installed.rows), expected)
        self.assertEqual(installed.rows, query_section_at(self.source, original.position, WEST).rows)
        advance_replication(self.source, self.receiver, SUBSCRIPTION, REPLICA)
        self.assertEqual((self.report().position, values(self.report().rows)), (8, (('W', 'west', 19, 8),)))
        self.assertEqual(self.pending_ids(), ())

    def test_retired_after_reopen(self):
        self.install_through_seven()
        change_scope(self.receiver, SUBSCRIPTION, 'R', WEST)
        self.assertEqual(record(self.source, 'correct', 'W', 'west', 19), 8)
        self.assertEqual(record(self.source, 'correct', 'E', 'east', 11), 9)
        old8, old9 = self.issue()
        self.assertEqual(self.send(old8).status, 'waiting')
        cutover = self._forward()
        self.assertEqual(self.send(cutover).status, 'applied', 'SCOPE_CUTOVER_MISMATCH')
        self.assertEqual(values(self.report().rows), (('W', 'west', 17, 3),), 'SCOPE_CUTOVER_MISMATCH')
        self.reopen()
        new8, new9 = self.issue()
        self.assertEqual(self.send(new8).status, 'applied')
        old = self.report()
        self.assertEqual((old.position, old.scope, values(old.rows)), (8, WEST, (('W', 'west', 19, 8),)))
        self.assertEqual(self.send(old9).status, 'retired', 'SCOPE_CUTOVER_MISMATCH')
        self.assertEqual(self.report(), old, 'SCOPE_CUTOVER_MISMATCH')
        self.assertEqual(self.send(old8).status, 'retired')
        self.assertEqual(self.send(new9).status, 'applied')
        self.assertEqual((self.report().position, values(self.report().rows)), (9, (('W', 'west', 19, 8),)))
        self.assertEqual(receipt(self.receiver, SUBSCRIPTION, 'R').position, 7)

    def test_prior_control_replay_cannot_reverse_a_later_cutover(self):
        self.install_through_seven()
        change_scope(self.receiver, SUBSCRIPTION, 'R', WEST)
        first = self._forward()
        outcome = self.send(first)
        change_scope(self.receiver, SUBSCRIPTION, 'S', EAST)
        second = self._forward('S')
        self.assertEqual(self.send(second).status, 'applied')
        self.reopen()
        self.assertEqual(self.send(first), outcome)
        self.assertEqual((self.report().scope, values(self.report().rows)), (EAST, (('E', 'east', 5, 7),)))
        self.assertEqual(change_scope(self.receiver, SUBSCRIPTION, 'R', ['west', 'west']),
                         receipt(self.receiver, SUBSCRIPTION, 'R'))

    def test_pending_request_and_issuance_reject_conflicts(self):
        self.install_through_seven()
        pending = change_scope(self.receiver, SUBSCRIPTION, 'R', WEST)
        self.assertEqual(change_scope(self.receiver, SUBSCRIPTION, 'R', ['west', 'west']), pending)
        with self.assertRaises(ScopeRequestRejected):
            change_scope(self.receiver, SUBSCRIPTION, 'R', EAST)
        with self.assertRaises(ScopeRequestRejected):
            change_scope(self.receiver, SUBSCRIPTION, 'S', WEST)
        with self.assertRaises(ScopeIssuanceRejected):
            issue_scope_change(self.source, SUBSCRIPTION, 'R', WEST, cut_position=8, prior_version=1)
        cutover = self._forward()
        with self.assertRaises(ScopeIssuanceRejected):
            issue_scope_change(self.source, SUBSCRIPTION, 'R', EAST, cut_position=7, prior_version=1)
        payload = cutover.payload
        payload['scope'] = ['east']
        bad = make_envelope(cutover.subscription_id, cutover.replica_id, cutover.scope_version,
                            cutover.position, cutover.kind, payload)
        before = self.report()
        with self.assertRaises(ScopeActivationRejected):
            accept_envelope(self.receiver, REPLICA, bad)
        self.assertEqual(self.report(), before)
        with self.assertRaises(ReceiptPending):
            receipt(self.receiver, SUBSCRIPTION, 'R')
        self.assertEqual(self.send(cutover).status, 'applied')
