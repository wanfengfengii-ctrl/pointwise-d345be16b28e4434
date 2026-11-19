from ledger.history import FactEvent, Observation


def project_fact_change(scope: tuple[str, ...], before: Observation | None,
                        event: FactEvent) -> dict:
    """Render one source fact as a scope-local delta payload object."""
    after = event.observation()
    if after is not None and after.section in scope:
        return {'operation': 'upsert', 'row': after.to_payload()}
    if before is not None and before.section in scope:
        return {'operation': 'delete', 'observation_id': event.observation_id}
    return {'operation': 'progress'}


def project_unit_steps(scope: tuple[str, ...], events: tuple[FactEvent, ...],
                       history) -> list[dict]:
    """Project every fact of one unit, each tagged with its original source position.

    A work order is one audit boundary but still keeps each fact's own position and
    scope-local projection, so nothing in the unit's interior is ever a readable
    cross-section and out-of-scope text never leaves the source.
    """
    steps = []
    for event in events:
        before = history.observation_at(event.observation_id, event.position - 1)
        delta = project_fact_change(scope, before, event)
        steps.append({'source_position': event.position, **delta})
    return steps
