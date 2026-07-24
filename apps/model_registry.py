"""Pure, append-only registry rules for reviewed read-only prediction models."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone


CANDIDATE = "candidate"
APPROVED = "approved"
RETIRED = "retired"
VALID_STATUSES = {CANDIDATE, APPROVED, RETIRED}


@dataclass(frozen=True)
class ModelRecord:
    """A serializable model record; artifact paths are immutable after registration."""

    model_id: str
    model_path: str
    feature_version: str
    horizon_minutes: int
    status: str
    registered_at: str
    approved_at: str | None = None
    retired_at: str | None = None
    validation_accuracy: float | None = None
    holdout_accuracy: float | None = None
    expected_return_pct: float | None = None


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def validate_record(record):
    """Validate model metadata without loading an artifact or contacting a service."""
    if not record.model_id.strip():
        raise ValueError("model_id must not be empty")
    if not record.model_path.strip():
        raise ValueError("model_path must not be empty")
    if not record.feature_version.strip():
        raise ValueError("feature_version must not be empty")
    if int(record.horizon_minutes) not in {15, 60}:
        raise ValueError("horizon_minutes must be 15 or 60")
    if record.status not in VALID_STATUSES:
        raise ValueError("model status is invalid")
    if record.status == APPROVED and not record.approved_at:
        raise ValueError("approved model requires approved_at")
    if record.status == RETIRED and not record.retired_at:
        raise ValueError("retired model requires retired_at")
    for metric in (record.validation_accuracy, record.holdout_accuracy):
        if metric is not None and not 0 <= float(metric) <= 1:
            raise ValueError("accuracy metrics must be between zero and one")
    if record.expected_return_pct is not None and float(record.expected_return_pct) < 0:
        raise ValueError("expected_return_pct must not be negative")
    return record


def register_candidate(*, model_id, model_path, feature_version, horizon_minutes, validation_accuracy, holdout_accuracy, expected_return_pct, registered_at=None):
    """Create an unapproved record. Registration alone can never make a model live."""
    return validate_record(ModelRecord(
        model_id=model_id,
        model_path=model_path,
        feature_version=feature_version,
        horizon_minutes=int(horizon_minutes),
        status=CANDIDATE,
        registered_at=registered_at or utc_now_iso(),
        validation_accuracy=float(validation_accuracy),
        holdout_accuracy=float(holdout_accuracy),
        expected_return_pct=float(expected_return_pct),
    ))


def current_records(records):
    """Resolve current state from the last append-only event for each model identifier."""
    current = {}
    for record in records:
        validate_record(record)
        current[record.model_id] = record
    return tuple(current.values())


def approved_for(records, feature_version, horizon_minutes):
    """Return the single current champion for a feature/horizon contract, or None."""
    matches = [
        record for record in current_records(records)
        if record.status == APPROVED
        and record.feature_version == feature_version
        and int(record.horizon_minutes) == int(horizon_minutes)
    ]
    if len(matches) > 1:
        raise ValueError("registry has more than one approved model for a feature and horizon")
    return matches[0] if matches else None


def promote(records, model_id, approved_at=None):
    """Append retirement and approval events while retaining all prior registry history."""
    states = {record.model_id: record for record in current_records(records)}
    source = states.get(model_id)
    if source is None:
        raise ValueError("model_id is not registered")
    if source.status not in {CANDIDATE, RETIRED}:
        raise ValueError("only candidate or retired models can be promoted")
    approval_time = approved_at or utc_now_iso()
    result = list(records)
    current = approved_for(result, source.feature_version, source.horizon_minutes)
    if current:
        result.append(ModelRecord(**{**asdict(current), "status": RETIRED, "retired_at": approval_time}))
    result.append(ModelRecord(**{**asdict(source), "status": APPROVED, "approved_at": approval_time, "retired_at": None}))
    return tuple(validate_record(record) for record in result)


def rollback(records, model_id, approved_at=None):
    """Rollback is a promotion of a previously registered candidate, never a mutation."""
    return promote(records, model_id, approved_at=approved_at)
