"""The baseline report (#79): a read-only fold of the event log into Markdown.

Most of this is pure: synthetic rows in, facts and Markdown out, because event
timestamps inside one test transaction are all the same ``now()`` and durations
could not be exercised against the real database. One test runs the command
end to end against Postgres to prove the SQL and the CLI wiring.
"""

from collections import Counter
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta

from orchestrator import auth, driver, report
from orchestrator.enums import EventType
from orchestrator.log import append, create_run

T0 = datetime(2026, 9, 20, 9, 0, tzinfo=UTC)


def at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


class Log:
    """Builds the raw rows ``report.fold`` consumes."""

    def __init__(self) -> None:
        self.runs: list[dict] = []
        self.events: list[dict] = []

    def run(self, kind, *, created=0.0, status="DONE", attempts=1):
        run_id = len(self.runs) + 1
        self.runs.append(
            {
                "id": run_id,
                "kind": kind,
                "status": status,
                "attempts": attempts,
                "created_at": at(created),
            }
        )
        self.event(run_id, EventType.RUN_QUEUED, created)
        return run_id

    def event(self, run_id, type_, minutes, payload=None):
        seq = sum(1 for e in self.events if e["run_id"] == run_id) + 1
        self.events.append(
            {
                "run_id": run_id,
                "seq": seq,
                "type": type_.value,
                "payload": payload or {},
                "created_at": at(minutes),
            }
        )

    def sandbox(
        self, kind, start, minutes, *, result=None, cost=1.0, exit_code=0, failed=None
    ):
        run_id = self.run(kind, created=start)
        self.event(run_id, EventType.SANDBOX_STARTED, start)
        self.event(
            run_id,
            EventType.CLAUDE_EVENT,
            start + minutes,
            {
                "type": "result",
                "total_cost_usd": cost,
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 2000,
                    "cache_read_input_tokens": 3_000_000,
                    "cache_creation_input_tokens": 1500,
                },
            },
        )
        self.event(
            run_id,
            EventType.STAGE_COMPLETED,
            start + minutes,
            {"exit_code": exit_code, "result": result},
        )
        if failed is None:
            self.event(run_id, EventType.RUN_DONE, start + minutes)
        else:
            self.event(run_id, EventType.RUN_FAILED, start + minutes, failed)
        return run_id

    def facts(self):
        return report.fold(self.runs, self.events)


def drive_session(log, start, minutes, tasks, outcome="COMPLETE", *, same_ts=False):
    run_id = log.run(driver.RUN_KIND, created=start)
    log.event(run_id, EventType.RUN_LEASED, start)
    end = start if same_ts else start + minutes
    for kind, reported, attempt, error in tasks:
        log.event(
            run_id,
            EventType.ARTIFACT,
            end,
            {
                "task_id": 1,
                "kind": kind,
                "subject": "series/secret-subject",
                "attempt": attempt,
                "reported": reported,
                "error": error,
            },
        )
    log.event(run_id, EventType.STAGE_COMPLETED, end, {"result": {"outcome": outcome}})
    log.event(run_id, EventType.RUN_DONE, end)
    return run_id


def test_buckets_hide_raw_text():
    assert report.abandon_bucket("lease expired (worker w1)") == "lease expired"
    assert report.abandon_bucket("sandbox vanished (c-12)") == "sandbox vanished"
    assert report.abandon_bucket("sandbox never started") == "sandbox never started"
    assert report.abandon_bucket("sandbox failed to start: x") == (
        "sandbox failed to start"
    )
    assert report.abandon_bucket("superseded by newer queued run 9") == "superseded"
    assert report.abandon_bucket(None) == "other"
    bucket = report.task_error_bucket
    assert bucket("job 4 still RUNNING after 900s") == "timeout"
    assert bucket("POST /x transport failure: TimeoutError()") == "timeout"
    assert bucket("POST /x transport failure: URLError()") == "transport"
    assert bucket("report answered 409") == "report rejected"
    assert bucket("job 7 failed: boom") == "job failed"
    assert bucket("POST /x answered 404: {}") == "http error"
    assert bucket("/x reported success=false") == "success=false"
    assert bucket("wait-check answered without a satisfied flag") == (
        "wait-check malformed"
    )
    assert bucket("weird") == "other"


def test_percentile_and_formatting():
    assert report.percentile([], 50) is None
    assert report.percentile([3, 1, 2], 50) == 2
    assert report.percentile([1, 2, 3, 4], 95) == 4
    assert report.fmt_duration(None) == "–"
    assert report.fmt_duration(42) == "42s"
    assert report.fmt_duration(125) == "2m 05s"
    assert report.fmt_duration(3 * 3600 + 120) == "3h 02m"
    assert report.fmt_mix(Counter()) == "–"
    assert report.fmt_mix(Counter(a=1, b=3)) == "b 3, a 1"


def test_sandbox_fold_sums_attempts_cost_and_tokens():
    log = Log()
    run_id = log.run("sentry_triage", created=0, attempts=2)
    log.event(run_id, EventType.SANDBOX_STARTED, 1)
    log.event(
        run_id,
        EventType.RUN_ABANDONED,
        4,
        {"requeued": True, "reason": "sandbox vanished (c)"},
    )
    log.event(run_id, EventType.SANDBOX_STARTED, 10)
    log.event(
        run_id,
        EventType.CLAUDE_EVENT,
        15,
        {"type": "result", "total_cost_usd": 0.5, "usage": {"output_tokens": 7}},
    )
    log.event(
        run_id,
        EventType.STAGE_COMPLETED,
        15,
        {"exit_code": 0, "result": {"outcome": "NOT_A_BUG"}},
    )
    log.event(run_id, EventType.RUN_DONE, 15)
    [facts] = log.facts()
    assert facts.sandbox_seconds == 8 * 60
    assert facts.cost_usd == 0.5
    assert facts.tokens["output_tokens"] == 7
    assert facts.outcome == "NOT_A_BUG"
    assert facts.requeues == 1
    assert facts.failure is None
    assert facts.duration == 15 * 60


def test_failures_and_outcome_shapes():
    log = Log()
    log.sandbox("adversarial_review", 0, 5, result={"verdict": "STANDS"})
    log.sandbox("rubric_verify", 0, 5, result={"passed": False})
    log.sandbox("sentry_triage", 0, 30, exit_code=124, failed={"exit_code": 124})
    log.sandbox("memory_dream", 0, 5, result="not a dict", failed={"exit_code": 1})
    cancelled = log.run("memory_dream", created=0, status="ABANDONED")
    log.event(
        cancelled,
        EventType.RUN_ABANDONED,
        1,
        {"requeued": False, "cancelled": True, "reason": "stale"},
    )
    lost = log.run("smoke", created=0, status="ABANDONED")
    log.event(lost, EventType.RUN_ABANDONED, 1, {"requeued": False, "reason": "x"})
    review, rubric, timeout, failed, cancelled, lost = log.facts()
    assert review.outcome == "STANDS"
    assert rubric.outcome == "NOT_PASSED"
    assert timeout.failure == "timeout"
    assert failed.failure == "sandbox failed"
    assert failed.outcome is None
    assert cancelled.failure == "cancelled"
    assert cancelled.outcome == "ABANDONED"
    assert lost.failure == "abandoned: other"


def test_pause_window_opens_on_auth_and_closes_on_next_run_that_ran():
    log = Log()
    log.sandbox("sentry_triage", 0, 5, result={"outcome": "FIX"})
    log.sandbox(
        "memory_dream", 10, 1, failed={auth.REASON_KEY: auth.REASON_AUTH}, cost=0
    )
    stale = log.run("memory_dream", created=20, status="ABANDONED")
    log.event(stale, EventType.RUN_ABANDONED, 30, {"requeued": False, "cancelled": 1})
    log.sandbox("memory_dream", 60 * 24 * 3, 5, result={"outcome": "FINDINGS"})
    log.sandbox(
        "smoke", 60 * 24 * 4, 1, failed={auth.REASON_KEY: auth.REASON_AUTH}, cost=0
    )
    windows = report.pauses(log.facts())
    assert len(windows) == 2
    first, second = windows
    assert first.start == at(11)
    assert first.end == at(60 * 24 * 3 + 5)
    assert first.auth_failed_runs == 1
    assert round(first.days, 1) == 3.0
    assert second.end is None
    assert second.days is None


def test_driver_durations_tasks_and_unknown_timestamps():
    log = Log()
    drive_session(
        log,
        0,
        12,
        [
            ("WRITE_ARTICLE", "DONE", 1, None),
            ("GENERATE_FEATURE_IMAGE", "FAILED", 2, "POST /x answered 500: {}"),
            ("APPROVE_ARTICLE", "WAITING", 1, None),
        ],
        outcome="INCOMPLETE",
    )
    drive_session(log, 100, 0.1, [], outcome="AT_CAPACITY")
    drive_session(log, 200, 9, [("WRITE_ARTICLE", "DONE", 1, None)], same_ts=True)
    facts = log.facts()
    assert [f.duration for f in facts] == [12 * 60, 6, None]
    text = report.render(facts, since=None, generated=T0)
    assert "| Sessions | 3 |" in text
    assert "12m 00s / 12m 00s (n=1)" in text
    assert "| Sessions with no recoverable duration | 1 |" in text
    assert "| Task retries (claimed at attempt > 1) | 1 |" in text
    assert "| `WRITE_ARTICLE` | 2 | 2 | 0 | 0 | 0 | – |" in text
    assert "| `GENERATE_FEATURE_IMAGE` | 1 | 0 | 1 | 0 | 0 | http error 1 |" in text
    assert "INCOMPLETE 1" in text
    assert "secret-subject" not in text
    assert "answered 500" not in text


def test_render_sections_and_empty_cases():
    log = Log()
    log.sandbox("sentry_triage", 0, 8, result={"outcome": "FIX"}, cost=2.5)
    log.sandbox(
        "memory_dream", 60, 1, failed={auth.REASON_KEY: auth.REASON_AUTH}, cost=0
    )
    log.sandbox("memory_dream", 60 * 24 * 8, 5, result={"outcome": "FINDINGS"})
    text = report.render(log.facts(), since=date(2026, 9, 1), generated=T0)
    assert "since 2026-09-01" in text
    assert "No driver sessions in range." in text
    assert "| `sentry_triage` | 1 | 8m 00s | 8m 00s | $2.50 | $2.50 |" in text
    assert "10 / 2.0k / 3.0M / 1.5k" in text
    assert "FIX 1" in text
    # The auth-killed run is a pause, not a memory_dream failure.
    assert "| `memory_dream` | 1 |" in text
    assert "| 2026-09-20 10:01 UTC | 2026-09-28 09:05 UTC | 8.0 | 1 |" in text
    assert "2026-W38 | 0 | 2 | $2.50 | login-expiry pause |" in text
    assert "2026-W39 | 0 | 0" not in text

    empty = report.render([], since=None, generated=T0)
    assert "(all runs)" in empty
    assert "No sandbox runs in range." in empty
    assert "No runs failed on the Claude login in range." in empty
    assert "No runs in range." in empty
    assert report._short(12) == "12"


def test_command_runs_read_only_against_postgres(conn, monkeypatch, capsys):
    drive = create_run(conn, driver.RUN_KIND, "drive:test")
    append(conn, drive, EventType.RUN_LEASED, worker_id="w", attempts=1)
    append(
        conn,
        drive,
        EventType.ARTIFACT,
        {"task_id": 1, "kind": "TRANSLATE", "reported": "DONE", "attempt": 1},
    )
    append(conn, drive, EventType.RUN_DONE, worker_id=None, lease_expires_at=None)
    triage = create_run(conn, "sentry_triage", "issue:1")
    append(conn, triage, EventType.RUN_LEASED, worker_id="w", attempts=1)
    append(conn, triage, EventType.SANDBOX_STARTED, {"container": "c"})
    append(conn, triage, EventType.CLAUDE_EVENT, {"type": "assistant", "x": 1})
    append(
        conn,
        triage,
        EventType.CLAUDE_EVENT,
        {"type": "result", "total_cost_usd": 1.25},
    )
    append(
        conn,
        triage,
        EventType.STAGE_COMPLETED,
        {"exit_code": 0, "result": {"outcome": "NEEDS_HUMAN"}},
    )
    append(conn, triage, EventType.RUN_DONE, worker_id=None, lease_expires_at=None)
    # report.load rolls back after reading; keep the test's rows visible to it.
    monkeypatch.setattr(conn, "rollback", lambda: None)

    @contextmanager
    def fake_connect(dsn=None):
        yield conn

    monkeypatch.setattr(report, "connect", fake_connect)
    assert report.main(["baseline", "--since", "2000-01-01"]) == 0
    out = capsys.readouterr().out
    assert "| `TRANSLATE` | 1 | 1 |" in out
    assert "NEEDS_HUMAN 1" in out
    assert "$1.25" in out
