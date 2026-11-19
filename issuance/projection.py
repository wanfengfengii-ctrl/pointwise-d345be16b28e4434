from dataclasses import dataclass

from ledger.history import FactEvent, Observation


@dataclass(frozen=True)
class ScopeDelta:
    operation: str
    observation_id: str | None = None
    row: Observation | None = None

    def to_payload(self) -> dict:
        if self.row is not None:
            return {'operation': self.operation, 'row': self.row.to_payload()}
        if self.observation_id is not None:
            return {'operation': self.operation, 'observation_id': self.observation_id}
        return {'operation': self.operation}


def project_fact_change(scope: tuple[str, ...], before: Observation | None,
                        event: FactEvent) -> ScopeDelta:
    after = event.observation()
    if after is not None and after.section in scope:
        return ScopeDelta('upsert', row=after)
    if before is not None and before.section in scope:
        return ScopeDelta('delete', observation_id=event.observation_id)
    return ScopeDelta('progress')
