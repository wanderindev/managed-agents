"""Queue hygiene while dispatch is paused on a dead Claude login (#59).

Four guarantees: the schedules stop enqueueing while paused, stale queued runs
can be cancelled through a command that writes proper events, queued dreams
collapse to the newest per repo, and the human hears about the pause every
day rather than once.
"""

import os
from contextlib import contextmanager

import pytest

from orchestrator import auth, dream, jobs, notify, queue
from orchestrator import poll as poll_module
from orchestrator.enums import EventType, RunStatus
from orchestrator.log import append, create_run, load_events, verify_replay
from orchestrator.loop import Orchestrator
from orchestrator.queue import get_run
from tests.fakes import FakeRunner

TO = "javier@example.com"


@pytest.fixture()
def credential(tmp_path):
    path = tmp_path / ".credentials.json"
    path.write_text('{"claudeAiOauth": {"expiresAt": 0}}')
    return path


def auth_failed(conn, credential, subject="smoke:auth"):
    """A run that died on the login, stamped the way the loop stamps it."""
    run_id = create_run(conn, "smoke", subject)
    append(conn, run_id, EventType.RUN_LEASED, worker_id="w", attempts=1)
    append(conn, run_id, EventType.SANDBOX_STARTED, {"container": "c"})
    append(
        conn,
        run_id,
        EventType.RUN_FAILED,
        {
            "exit_code": 1,
            auth.REASON_KEY: auth.REASON_AUTH,
            auth.FINGERPRINT_KEY: auth.fingerprint(credential),
        },
        worker_id=None,
        lease_expires_at=None,
    )
    return run_id


def relogin(credential):
    credential.write_text('{"claudeAiOauth": {"expiresAt": 1893456000000}}')
    os.utime(credential, ns=(1, 1))


def fake_connect(conn):
    @contextmanager
    def connect(dsn=None):
        yield conn

    return connect


def dream_queued(conn, repo, day):
    return create_run(conn, jobs.DREAM_KIND, f"dream:{repo}:{day}", {"repo": repo})


def backdate(conn, run_id, days):
    """agent_runs is a cache, not the log: its created_at may be moved in a test."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE agent_runs SET created_at = now() - make_interval(days => %s)"
            " WHERE id = %s",
            (days, run_id),
        )


# --- the schedules stand down while paused -------------------------------------


def test_paused_is_the_loops_condition(conn, credential):
    assert auth.paused(conn, credential) is None
    auth_failed(conn, credential)
    assert auth.paused(conn, credential) == auth.fingerprint(credential)
    relogin(credential)
    assert auth.paused(conn, credential) is None


def test_skip_enqueue_says_why_and_counts_the_queue(conn, credential, caplog):
    assert not auth.skip_enqueue(conn, "things", credential)
    auth_failed(conn, credential)
    create_run(conn, "smoke", "waiting")
    with caplog.at_level("WARNING", logger="orchestrator.auth"):
        assert auth.skip_enqueue(conn, "things", credential)
    assert "not enqueuing things" in caplog.text
    assert "1 run(s) already queued" in caplog.text
    assert "claude auth login" in caplog.text


def test_the_poll_enqueues_nothing_while_paused(conn, credential, monkeypatch):
    auth_failed(conn, credential)
    monkeypatch.setattr(poll_module.config, "SENTRY_TOKEN", "tok")
    monkeypatch.setattr(poll_module.config, "CLAUDE_CREDENTIALS", str(credential))
    monkeypatch.setattr(poll_module, "connect", fake_connect(conn))

    def must_not_poll(*args, **kwargs):
        raise AssertionError("polled while paused")

    monkeypatch.setattr(poll_module.sentry, "poll", must_not_poll)
    monkeypatch.setattr(poll_module, "_poll_prs", must_not_poll)
    assert poll_module.main([]) == 0, "a pause is not a failure for cron"


def test_the_dream_schedule_enqueues_nothing_while_paused(
    conn, credential, monkeypatch
):
    auth_failed(conn, credential)
    monkeypatch.setattr(dream.config, "CLAUDE_CREDENTIALS", str(credential))
    monkeypatch.setattr(dream, "connect", fake_connect(conn))

    def must_not_enqueue(*args, **kwargs):
        raise AssertionError("enqueued while paused")

    monkeypatch.setattr(dream, "enqueue", must_not_enqueue)
    monkeypatch.setattr(dream, "_pulls_client", must_not_enqueue)
    assert dream.main(["--repo", "feliu-dev"]) == 0


def test_the_dream_schedule_runs_normally_when_not_paused(
    conn, credential, monkeypatch
):
    monkeypatch.setattr(dream.config, "CLAUDE_CREDENTIALS", str(credential))
    monkeypatch.setattr(dream, "connect", fake_connect(conn))
    monkeypatch.setattr(dream, "_pulls_client", lambda: None)
    seen = []
    monkeypatch.setattr(dream, "enqueue", lambda conn, **kw: seen.append(kw["repo"]))
    assert dream.main(["--repo", "feliu-dev"]) == 0
    assert seen == ["feliu-dev"]


# --- cancelling stale queued runs ----------------------------------------------


def test_cancel_writes_events_and_lands_on_abandoned(conn):
    old = dream_queued(conn, "feliu-dev", "2026-09-25")
    backdate(conn, old, 3)
    fresh = dream_queued(conn, "feliu-dev", "2026-10-05")
    other = create_run(conn, "sentry_triage", "sentry:1")
    backdate(conn, other, 3)

    rows = queue.cancel_queued(
        conn, reason="stale", kind=jobs.DREAM_KIND, older_than=queue.parse_age("1d")
    )

    assert [r["id"] for r in rows] == [old]
    assert get_run(conn, old).status is RunStatus.ABANDONED
    assert verify_replay(conn, old), "a cancel must be a proper fold, not an edit"
    types = [e.type for e in load_events(conn, old)]
    assert types[-2:] == [EventType.RUN_ABANDONED, EventType.EMAIL_SENT]
    abandoned = load_events(conn, old)[-2].payload
    assert abandoned == {"requeued": False, "cancelled": True, "reason": "stale"}
    assert get_run(conn, fresh).status is RunStatus.QUEUED
    assert get_run(conn, other).status is RunStatus.QUEUED


def test_a_cancelled_run_sends_no_outcome_email(conn):
    run_id = dream_queued(conn, "feliu-dev", "2026-09-25")
    queue.cancel_queued(conn, reason="stale", run_ids=[run_id])
    sent = []
    assert notify.pass_once(conn, sent.append, to=TO, cap=10) == 0
    assert sent == []


def test_a_dry_run_cancels_nothing(conn):
    run_id = dream_queued(conn, "feliu-dev", "2026-09-25")
    rows = queue.cancel_queued(conn, reason="x", run_ids=[run_id], dry_run=True)
    assert [r["id"] for r in rows] == [run_id]
    assert get_run(conn, run_id).status is RunStatus.QUEUED
    assert len(load_events(conn, run_id)) == 1


def test_cancel_never_touches_a_leased_run(conn):
    run_id = create_run(conn, "smoke", "busy")
    append(conn, run_id, EventType.RUN_LEASED, worker_id="w", attempts=1)
    assert queue.cancel_queued(conn, reason="x", kind="smoke") == []
    assert get_run(conn, run_id).status is RunStatus.LEASED


def test_cancel_refuses_without_a_filter(conn):
    create_run(conn, "smoke", "keep")
    with pytest.raises(ValueError, match="without a filter"):
        queue.cancel_queued(conn, reason="x")


@pytest.mark.parametrize(
    ("text", "seconds"),
    [("30m", 1800), ("12h", 43200), ("1d", 86400), ("2w", 1209600), ("45s", 45)],
)
def test_parse_age(text, seconds):
    assert queue.parse_age(text).total_seconds() == seconds


def test_parse_age_rejects_nonsense():
    with pytest.raises(ValueError, match="not a duration"):
        queue.parse_age("yesterday")


def test_the_cli_dry_runs_then_cancels(conn, monkeypatch, capsys):
    run_id = dream_queued(conn, "feliu-dev", "2026-09-25")
    backdate(conn, run_id, 3)
    monkeypatch.setattr(queue, "connect", fake_connect(conn))
    argv = ["cancel", "--kind", jobs.DREAM_KIND, "--older-than", "1d"]

    assert queue.main([*argv, "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert f"run {run_id}  memory_dream  dream:feliu-dev:2026-09-25" in out
    assert "would cancel 1 queued run(s)" in out
    assert get_run(conn, run_id).status is RunStatus.QUEUED

    assert queue.main(argv) == 0
    assert "cancelled 1 queued run(s)" in capsys.readouterr().out
    assert get_run(conn, run_id).status is RunStatus.ABANDONED
    reason = load_events(conn, run_id)[-2].payload["reason"]
    assert "orchestrator.queue cancel" in reason


def test_the_cli_refuses_without_a_filter(capsys):
    with pytest.raises(SystemExit) as exc:
        queue.main(["cancel", "--dry-run"])
    assert exc.value.code == 2
    assert "at least one of" in capsys.readouterr().err


# --- coalescing queued dreams -------------------------------------------------


def test_only_the_newest_dream_per_repo_survives(conn):
    a1 = dream_queued(conn, "feliu-dev", "2026-09-25")
    b1 = dream_queued(conn, "pic", "2026-09-25")
    a2 = dream_queued(conn, "feliu-dev", "2026-09-26")
    a3 = dream_queued(conn, "feliu-dev", "2026-09-27")
    loose = create_run(conn, jobs.DREAM_KIND, "dream:?:x", {})

    assert sorted(queue.coalesce(conn, jobs.DREAM_KIND, "repo")) == [a1, a2]

    for kept in (a3, b1, loose):
        assert get_run(conn, kept).status is RunStatus.QUEUED
    for gone in (a1, a2):
        assert get_run(conn, gone).status is RunStatus.ABANDONED
        assert verify_replay(conn, gone)
        reason = load_events(conn, gone)[-2].payload["reason"]
        assert f"superseded by newer queued run {a3}" in reason
    assert queue.coalesce(conn, jobs.DREAM_KIND, "repo") == [], "idempotent"


def test_the_loop_coalesces_before_it_leases(conn):
    old = dream_queued(conn, "feliu-dev", "2026-09-25")
    new = dream_queued(conn, "feliu-dev", "2026-09-26")
    runner = FakeRunner()
    orch = Orchestrator(
        runner, worker_id="w", max_concurrent=1, coalesce=jobs.COALESCED
    )
    result = orch.tick(conn)
    assert result.cancelled == [old]
    assert result.leased == [new]
    assert not result.idle


def test_the_loop_coalesces_even_while_paused(conn, credential):
    auth_failed(conn, credential)
    old = dream_queued(conn, "feliu-dev", "2026-09-25")
    new = dream_queued(conn, "feliu-dev", "2026-09-26")
    orch = Orchestrator(
        FakeRunner(),
        worker_id="w",
        max_concurrent=1,
        coalesce=jobs.COALESCED,
        credentials=credential,
    )
    result = orch.tick(conn)
    assert result.paused is not None
    assert result.cancelled == [old]
    assert get_run(conn, new).status is RunStatus.QUEUED


# --- the daily paused-login email ---------------------------------------------


def email_yesterday(conn, run_id):
    """An email_sent dated yesterday. Test-only: production never writes
    agent_events by hand, and one transaction's now() cannot move a day."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO agent_events (run_id, seq, type, payload, created_at)"
            " SELECT %s, max(seq) + 1, %s, '{}'::jsonb, now() - interval '1 day'"
            " FROM agent_events WHERE run_id = %s",
            (run_id, EventType.EMAIL_SENT.value, run_id),
        )


def dream_failed_on_auth(conn, credential):
    run_id = create_run(
        conn, jobs.DREAM_KIND, "dream:feliu-dev:2026-09-24", {"repo": "feliu-dev"}
    )
    append(conn, run_id, EventType.RUN_LEASED, worker_id="w", attempts=1)
    append(
        conn,
        run_id,
        EventType.RUN_FAILED,
        {"exit_code": 1, "reason": "auth", "credentials": auth.fingerprint(credential)},
        worker_id=None,
        lease_expires_at=None,
    )
    return run_id


def test_the_first_login_email_carries_the_queued_count(conn, credential):
    dream_failed_on_auth(conn, credential)
    dream_queued(conn, "pic", "2026-09-25")
    create_run(conn, "sentry_triage", "sentry:9")
    sent = []
    assert notify.pass_once(conn, sent.append, to=TO, credentials=credential) == 1
    [email] = sent
    assert "Claude login EXPIRED" in email.subject
    assert "2 run(s) are queued" in email.body
    assert "memory_dream: 1" in email.body
    assert "orchestrator.queue cancel --older-than 1d --dry-run" in email.body
    assert "only email" not in email.body


def test_no_reminder_the_same_day_as_the_first_email(conn, credential):
    dream_failed_on_auth(conn, credential)
    sent = []
    notify.pass_once(conn, sent.append, to=TO, credentials=credential)
    notify.pass_once(conn, sent.append, to=TO, credentials=credential)
    assert len(sent) == 1


def test_the_reminder_goes_out_daily_while_paused(conn, credential):
    run_id = auth_failed(conn, credential)
    email_yesterday(conn, run_id)
    dream_queued(conn, "feliu-dev", "2026-09-25")
    dream_queued(conn, "pic", "2026-09-25")

    sent = []
    assert notify.pass_once(conn, sent.append, to=TO, credentials=credential) == 1
    [email] = sent
    assert "STILL EXPIRED" in email.subject
    assert "2 queued" in email.subject
    assert "memory_dream: 2" in email.body
    assert "claude auth login" in email.body
    marker = load_events(conn, run_id)[-1]
    assert marker.type is EventType.EMAIL_SENT
    assert marker.payload["auth_reminder"] is True
    assert verify_replay(conn, run_id)

    # Once a day, not once a tick.
    assert notify.pass_once(conn, sent.append, to=TO, credentials=credential) == 0
    # And it does not eat the daily cap of ordinary outcome emails.
    assert notify._sent_today(conn) == 0


def test_no_reminder_once_the_login_is_fixed(conn, credential):
    run_id = auth_failed(conn, credential)
    email_yesterday(conn, run_id)
    relogin(credential)
    sent = []
    assert notify.pass_once(conn, sent.append, to=TO, credentials=credential) == 0


def test_the_reminder_never_steals_the_first_email(conn, credential):
    auth_failed(conn, credential)
    assert notify._auth_reminder(conn, credential) is None


def test_a_failed_reminder_marks_nothing(conn, credential):
    run_id = auth_failed(conn, credential)
    email_yesterday(conn, run_id)
    before = len(load_events(conn, run_id))

    def refuse(email):
        raise OSError("relay refused")

    assert notify.pass_once(conn, refuse, to=TO, credentials=credential) == 0
    assert len(load_events(conn, run_id)) == before


def test_the_dry_run_shows_the_reminder(conn, credential, monkeypatch, capsys):
    run_id = auth_failed(conn, credential)
    email_yesterday(conn, run_id)
    monkeypatch.setattr(notify, "connect", fake_connect(conn))
    monkeypatch.setattr(notify.config, "CLAUDE_CREDENTIALS", str(credential))
    assert notify.main(["--dry-run"]) == 0
    assert "paused-login reminder:" in capsys.readouterr().out
    assert len(load_events(conn, run_id)) == 5, "dry run must mark nothing"
