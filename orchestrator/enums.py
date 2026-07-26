"""Vocabularies for the event log.

These mirror the CHECK constraints in ``migrations/001_event_log.sql``. Adding a
value here without widening the constraint (or vice versa) is a bug the tests
will catch.
"""

from enum import StrEnum


class RunStatus(StrEnum):
    """Lifecycle of one unit of work."""

    QUEUED = "QUEUED"
    LEASED = "LEASED"
    RUNNING = "RUNNING"
    AWAITING_HUMAN = "AWAITING_HUMAN"
    DONE = "DONE"
    FAILED = "FAILED"
    ABANDONED = "ABANDONED"


#: Statuses that release the ``(kind, subject)`` uniqueness slot. Kept in sync
#: with the partial index ``agent_runs_open_subject_key``.
TERMINAL_STATUSES = frozenset({RunStatus.DONE, RunStatus.FAILED, RunStatus.ABANDONED})


class Outcome(StrEnum):
    """How a sandbox ended, as distinct from why a run ended.

    ``FAILED`` and ``GONE`` are both "the container is not running any more", but
    they mean opposite things: FAILED is a verdict from the work, GONE is the
    infrastructure losing the work. Only the second one deserves a retry.
    """

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    GONE = "GONE"


class EventType(StrEnum):
    """Everything that can be appended to a run's history.

    Only some of these move the run's status; see ``log.apply_event``. The rest
    are narrative, which is the point of keeping a log rather than a state field.
    """

    RUN_QUEUED = "run_queued"
    RUN_LEASED = "run_leased"
    SANDBOX_STARTED = "sandbox_started"
    CLAUDE_EVENT = "claude_event"
    STAGE_COMPLETED = "stage_completed"
    ARTIFACT = "artifact"
    HUMAN_GATE = "human_gate"
    RUN_ABANDONED = "run_abandoned"
    RUN_FAILED = "run_failed"
    RUN_DONE = "run_done"
    #: Narrative marker (#9): this run's outcome has been emailed (or the
    #: payload says why it deliberately was not). One per run, ever — it is
    #: what makes the notifier idempotent.
    EMAIL_SENT = "email_sent"
