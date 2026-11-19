"""Hand-listed work order examples and a runnable V1 comparison."""

import argparse
import sqlite3
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from audit.details import read
from delivery.outbox import Outbox
from delivery.resend import ReplyLost, deliver
from issuance.increment import issue_available_increments
from issuance.subscriptions import open_source, subscribe
from ledger.records import record
from ledger.section_query import query_section_at
from replica.receive import open_replica
from tests.support import reject_receiver_commit, totals, values

EAST = ("east",)
SUBSCRIPTION = "night"
REPLICA = "D"
SEED = [("register", "E", "east", 5), ("register", "F", "east", None)]
WORK_ORDER = [("withdraw", "F"), ("register", "W", "west", 17), ("register", "G", "east", 7)]
WEST_ONLY = [("correct", "W", "west", 19), ("register", "X", "west", 0)]
RETURN_TO_EAST = [("correct", "E", "west", 99), ("correct", "E", "east", 5)]
INVALID_ORDER = [("correct", "E", "east", 8), ("correct", "UNKNOWN", "east", 4)]
EAST_AT_2 = (("E", "east", 5, 1), ("F", "east", None, 2))
EAST_AT_5 = (("E", "east", 5, 1), ("G", "east", 7, 5))
LEGACY_OPERATIONS = [("correct", "E", "east", 6), ("register", "Z", "east", 0)]
LEGACY_AT_4 = (("E", "east", 6, 3), ("F", "east", None, 2), ("Z", "east", 0, 4))


def require(actual, expected):
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def check_view(receiver, position, rows):
    report = read(receiver, REPLICA)
    require((report.position, values(report.rows)), (position, rows))
    return report


def send(source, receiver, piece, **options):
    return deliver(source, receiver, piece.envelope_id, REPLICA, **options)


def install_v1(source, receiver):
    require(tuple(record(source, *operation) for operation in SEED), (1, 2))
    initial = subscribe(source, SUBSCRIPTION, REPLICA, EAST)
    require(send(source, receiver, initial).status, "applied")
    check_view(receiver, 2, EAST_AT_2)


def observe_v1(source, receiver):
    install_v1(source, receiver)
    require(tuple(record(source, *operation) for operation in WORK_ORDER), (3, 4, 5))
    withdrawal, progress, replacement = issue_available_increments(source, SUBSCRIPTION)
    require(progress.payload, {"operation": "progress"})
    require(send(source, receiver, withdrawal).status, "applied")
    after_withdrawal = check_view(receiver, 3, (EAST_AT_2[0],))
    require(values(query_section_at(source, 3, EAST).rows), values(after_withdrawal.rows))
    require(send(source, receiver, replacement).status, "waiting")
    require(read(receiver, REPLICA), after_withdrawal)
    require(send(source, receiver, progress).status, "applied")
    completed = check_view(receiver, 5, EAST_AT_5)
    require(totals(completed), (("east", 2, 0, 12),))
    print(f"V1 separate entries: {after_withdrawal.position} {values(after_withdrawal.rows)}"
          f" -> {completed.position} {values(completed.rows)}")


def seed_legacy(source, receiver):
    install_v1(source, receiver)
    require(tuple(record(source, *operation) for operation in LEGACY_OPERATIONS), (3, 4))
    earlier, later = issue_available_increments(source, SUBSCRIPTION)
    require(send(source, receiver, later).status, "waiting")
    check_view(receiver, 2, EAST_AT_2)
    return earlier, later


def pending(source):
    return Outbox(source).pending_for(SUBSCRIPTION)


def exercise_legacy(root):
    source_path, receiver_path = root / "source.sqlite", root / "replica.sqlite"
    with closing(open_source(source_path)) as source, closing(open_replica(receiver_path)) as receiver:
        earlier, later = seed_legacy(source, receiver)
    with closing(open_source(source_path)) as source, closing(open_replica(receiver_path)) as receiver:
        require(pending(source), (earlier, later))
        check_view(receiver, 2, EAST_AT_2)
        with reject_receiver_commit(receiver) as denied:
            try:
                send(source, receiver, earlier)
            except sqlite3.DatabaseError:
                require(denied, ["COMMIT"])
            else:
                raise AssertionError("expected receiver COMMIT rejection")
        check_view(receiver, 2, EAST_AT_2)
        require(pending(source), (earlier, later))
        try:
            send(source, receiver, earlier, lose_reply=True)
        except ReplyLost:
            pass
        else:
            raise AssertionError("expected lost reply after receiver commit")
        completed = check_view(receiver, 4, LEGACY_AT_4)
        require(pending(source), (earlier, later))
    with closing(open_source(source_path)) as source, closing(open_replica(receiver_path)) as receiver:
        require(read(receiver, REPLICA), completed)
        for piece in (earlier, later):
            applied = send(source, receiver, piece)
            require((applied.position, applied.status), (piece.position, "applied"))
            require(send(source, receiver, piece), applied)
        require(pending(source), ())
        require(read(receiver, REPLICA), completed)
    print(f"V1 in-flight ledger: {completed.position} {values(completed.rows)}")


def main():
    parser = argparse.ArgumentParser(
        description="Run the V1 comparison or save two V1 databases with old pieces in flight."
    )
    parser.add_argument("--legacy-dir", type=Path, help="new directory for a V1 source and replica pair")
    args = parser.parse_args()
    if args.legacy_dir is not None:
        root = args.legacy_dir
        root.mkdir()
        with (closing(open_source(root / "source.sqlite")) as source,
              closing(open_replica(root / "replica.sqlite")) as receiver):
            earlier, later = seed_legacy(source, receiver)
        print(f"V1 pending {earlier.position} {earlier.envelope_id};"
              f" staged {later.position} {later.envelope_id}")
        return
    with closing(open_source(":memory:")) as source, closing(open_replica(":memory:")) as receiver:
        observe_v1(source, receiver)
    with TemporaryDirectory() as directory:
        exercise_legacy(Path(directory))


if __name__ == "__main__":
    main()
