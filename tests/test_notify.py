"""Outcome emails (issue #9).

The invariant under test: silence never means "nothing happened". Every
finished run in a human-relevant state either produces exactly one email or
carries an ``email_sent`` event saying why it deliberately did not — and past
the daily cap the remainder arrive as one digest instead of vanishing.
"""

from orchestrator import jobs, notify
from orchestrator.enums import EventType, RunStatus
from orchestrator.log import append, create_run, load_events, verify_replay
from orchestrator.loop import Orchestrator
from orchestrator.queue import get_run
from orchestrator.sources import sentry
from tests.fakes import FakeRunner
from tests.test_adversary import CHAIN_PAYLOAD
from tests.test_triage import PAYLOAD

TO = "javier@example.com"


class FakeTransport:
    def __init__(self, fail_times: int = 0):
        self.sent: list[notify.Email] = []
        self.fail_times = fail_times

    def __call__(self, email: notify.Email) -> None:
        if self.fail_times > 0:
            self.fail_times -= 1
            raise OSError("relay refused")
        self.sent.append(email)


def send_pass(conn, transport, cap=10):
    return notify.pass_once(conn, transport, to=TO, cap=cap)


def finished(conn, kind, subject, result, payload=PAYLOAD, status=EventType.RUN_DONE):
    """A run that ran and completed, the way the loop records one."""
    run_id = create_run(conn, kind, subject, payload)
    append(conn, run_id, EventType.RUN_LEASED, worker_id="w", attempts=1)
    append(conn, run_id, EventType.SANDBOX_STARTED, {"container": "c"})
    append(
        conn,
        run_id,
        EventType.STAGE_COMPLETED,
        {"exit_code": 0, "outcome": "SUCCEEDED", "result": result},
    )
    append(conn, run_id, status, worker_id=None, lease_expires_at=None)
    return run_id


def email_events(conn, run_id):
    return [e for e in load_events(conn, run_id) if e.type is EventType.EMAIL_SENT]


# --- what gets an email -------------------------------------------------------


def test_a_parked_review_emails_the_verdict_verbatim_with_the_pr_link(conn):
    run_id = finished(
        conn,
        jobs.REVIEW_KIND,
        "sentry:N-1",
        {"verdict": "STANDS", "reasoning": "all four attack lines failed"},
        payload=CHAIN_PAYLOAD,
    )
    append(
        conn,
        run_id,
        EventType.HUMAN_GATE,
        {
            "why": "adversarial review passed; PR marked ready",
            "verdict": "STANDS",
            "reasoning": "all four attack lines failed",
            "pr_url": CHAIN_PAYLOAD["pr_url"],
        },
    )
    transport = FakeTransport()

    assert send_pass(conn, transport) == 1
    (email,) = transport.sent
    assert email.to == TO
    assert "panama-in-context" in email.subject
    assert PAYLOAD["short_id"] in email.subject
    assert "STANDS" in email.subject
    assert CHAIN_PAYLOAD["pr_url"] in email.body
    assert "all four attack lines failed" in email.body
    assert PAYLOAD["permalink"] in email.body
    assert verify_replay(conn, run_id)


def test_not_a_bug_emails_without_a_pr_link(conn):
    finished(
        conn,
        sentry.RUN_KIND,
        "sentry:N-2",
        {"outcome": "NOT_A_BUG", "reason": "already fixed on main by e345fac"},
    )
    transport = FakeTransport()

    assert send_pass(conn, transport) == 1
    (email,) = transport.sent
    assert "NOT_A_BUG" in email.subject
    assert "(no pull request)" in email.body
    assert "already fixed on main" in email.body


def test_needs_human_emails_the_reason(conn):
    finished(
        conn,
        sentry.RUN_KIND,
        "sentry:N-3",
        {"outcome": "NEEDS_HUMAN", "reason": "schema change: apply migration 0023"},
    )
    transport = FakeTransport()

    send_pass(conn, transport)
    assert "migration 0023" in transport.sent[0].body


def test_a_failed_run_emails_instead_of_dying_silently(conn):
    run_id = create_run(conn, sentry.RUN_KIND, "sentry:N-4", PAYLOAD)
    append(conn, run_id, EventType.RUN_LEASED, worker_id="w", attempts=1)
    append(conn, run_id, EventType.RUN_FAILED)
    transport = FakeTransport()

    assert send_pass(conn, transport) == 1
    assert "FAILED" in transport.sent[0].subject


def test_an_abandoned_run_emails_its_attempt_count(conn):
    run_id = create_run(conn, sentry.RUN_KIND, "sentry:N-5", PAYLOAD)
    append(conn, run_id, EventType.RUN_LEASED, worker_id="w", attempts=3)
    append(conn, run_id, EventType.RUN_ABANDONED, {"requeued": False})
    transport = FakeTransport()

    send_pass(conn, transport)
    assert "ABANDONED after 3 attempts" in transport.sent[0].subject


# --- what deliberately does not ----------------------------------------------


def test_a_fix_outcome_is_suppressed_with_the_reason_recorded(conn):
    """The review run will email; emailing the fix too would double-notify."""
    run_id = finished(
        conn,
        sentry.RUN_KIND,
        "sentry:N-6",
        {"outcome": "FIX", "pr_url": "https://x/pr/1"},
    )
    transport = FakeTransport()

    assert send_pass(conn, transport) == 0
    assert transport.sent == []
    (marker,) = email_events(conn, run_id)
    assert "suppressed" in marker.payload
    assert verify_replay(conn, run_id)


def test_smoke_runs_never_email(conn):
    finished(conn, "smoke", "smoke:n", {"ok": True})
    transport = FakeTransport()
    assert send_pass(conn, transport) == 0


def test_each_run_emails_exactly_once(conn):
    finished(conn, sentry.RUN_KIND, "sentry:N-7", {"outcome": "NOT_A_BUG"})
    transport = FakeTransport()

    send_pass(conn, transport)
    send_pass(conn, transport)

    assert len(transport.sent) == 1


def test_the_email_marker_never_changes_the_status_fold(conn):
    run_id = finished(conn, sentry.RUN_KIND, "sentry:N-8", {"outcome": "NOT_A_BUG"})
    send_pass(conn, FakeTransport())
    assert get_run(conn, run_id).status is RunStatus.DONE
    assert verify_replay(conn, run_id)


def test_no_recipient_means_no_notifier(conn):
    finished(conn, sentry.RUN_KIND, "sentry:N-9", {"outcome": "NOT_A_BUG"})
    transport = FakeTransport()
    assert notify.pass_once(conn, transport, to="", cap=10) == 0
    assert transport.sent == []


# --- the cap and the digest ---------------------------------------------------


def test_past_the_cap_the_rest_arrive_as_one_digest(conn):
    for n in range(4):
        finished(conn, sentry.RUN_KIND, f"sentry:C-{n}", {"outcome": "NOT_A_BUG"})
    transport = FakeTransport()

    sent = send_pass(conn, transport, cap=2)

    assert sent == 3  # two individual + one digest
    assert len(transport.sent) == 3
    digest = transport.sent[-1]
    assert "digest" in digest.subject
    assert "2 more run(s)" in digest.subject
    assert "sentry:C-2" in digest.body and "sentry:C-3" in digest.body
    # Everyone is marked; a second pass is silent.
    assert send_pass(conn, transport, cap=2) == 0


def test_the_cap_counts_only_individual_emails_across_passes(conn):
    finished(conn, sentry.RUN_KIND, "sentry:C-a", {"outcome": "NOT_A_BUG"})
    finished(conn, sentry.RUN_KIND, "sentry:C-b", {"outcome": "NOT_A_BUG"})
    transport = FakeTransport()

    send_pass(conn, transport, cap=1)  # one individual + digest for the other
    finished(conn, sentry.RUN_KIND, "sentry:C-c", {"outcome": "NOT_A_BUG"})
    send_pass(conn, transport, cap=1)  # budget exhausted -> digest again

    subjects = [e.subject for e in transport.sent]
    assert sum("digest" not in s for s in subjects) == 1
    assert sum("digest" in s for s in subjects) == 2


# --- failure handling ---------------------------------------------------------


def test_a_transport_failure_marks_nothing_and_retries_next_pass(conn):
    run_id = finished(conn, sentry.RUN_KIND, "sentry:F-1", {"outcome": "NOT_A_BUG"})
    transport = FakeTransport(fail_times=1)

    assert send_pass(conn, transport) == 0
    assert email_events(conn, run_id) == []

    assert send_pass(conn, transport) == 1
    assert len(email_events(conn, run_id)) == 1


# --- through the loop ---------------------------------------------------------


def test_the_loop_emails_in_the_same_tick_a_run_parks(conn):
    transport = FakeTransport()
    runner = FakeRunner(result={"verdict": "STANDS", "reasoning": "held"})
    orchestrator = Orchestrator(
        runner,
        worker_id="orchestrator-test",
        max_concurrent=1,
        backoff_base_seconds=0,
        followups=jobs.followups,
        notify=lambda conn: notify.pass_once(conn, transport, to=TO, cap=10),
    )
    review_id = create_run(conn, jobs.REVIEW_KIND, "sentry:L-9", CHAIN_PAYLOAD)

    orchestrator.tick(conn)
    runner.kill_all()
    orchestrator.tick(conn)

    assert get_run(conn, review_id).status is RunStatus.AWAITING_HUMAN
    (email,) = transport.sent
    assert "STANDS" in email.subject


def test_a_notify_crash_never_costs_the_tick(conn, caplog):
    def boom(_conn):
        raise RuntimeError("relay on fire")

    runner = FakeRunner()
    orchestrator = Orchestrator(
        runner, worker_id="orchestrator-test", max_concurrent=1, notify=boom
    )
    create_run(conn, "smoke", "smoke:t-1")

    with caplog.at_level("ERROR", logger="orchestrator.loop"):
        result = orchestrator.tick(conn)

    assert result.leased, "the tick still dispatched"
    assert "notify pass failed" in caplog.text


# --- the wire format ----------------------------------------------------------


def test_smtp_send_shapes_the_message(monkeypatch):
    sent = {}

    class FakeSMTP:
        def __init__(self, host, port, timeout):
            sent["target"] = (host, port)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def ehlo(self):
            pass

        def starttls(self):
            sent["tls"] = True

        def send_message(self, msg):
            sent["subject"] = msg["Subject"]
            sent["to"] = msg["To"]

    import smtplib

    monkeypatch.setattr(smtplib, "SMTP", FakeSMTP)
    notify.smtp_send(notify.Email(to=TO, subject="s", body="b"))

    assert sent["target"] == ("smtp-relay.gmail.com", 587)
    assert sent["tls"] is True
    assert sent["to"] == TO


# --- remaining headline and body branches -------------------------------------


def test_uncertain_emails_the_doubt_prominently(conn):
    run_id = finished(
        conn,
        jobs.REVIEW_KIND,
        "sentry:U-1",
        {"verdict": "UNCERTAIN", "reasoning": "could not obtain the input"},
        payload=CHAIN_PAYLOAD,
    )
    append(
        conn,
        run_id,
        EventType.HUMAN_GATE,
        {
            "why": "adversarial review could not reach a verdict",
            "verdict": "UNCERTAIN",
            "reasoning": "could not obtain the input",
            "pr_url": CHAIN_PAYLOAD["pr_url"],
        },
    )
    transport = FakeTransport()

    send_pass(conn, transport)
    assert "UNCERTAIN" in transport.sent[0].subject
    assert "read the doubt before merging" in transport.sent[0].subject
    assert "could not obtain the input" in transport.sent[0].body


def test_a_verdictless_gate_emails_its_why(conn):
    """The exhausted-attempts and revision gates carry a why, not a verdict."""
    run_id = finished(
        conn,
        jobs.REVISION_KIND,
        "sentry:U-2",
        {"outcome": "NEEDS_HUMAN", "reason": "two agents disagree"},
        payload=CHAIN_PAYLOAD,
    )
    append(
        conn,
        run_id,
        EventType.HUMAN_GATE,
        {"why": "revision could not or should not proceed", "reason": "disagree"},
    )
    transport = FakeTransport()

    send_pass(conn, transport)
    assert "revision could not or should not proceed" in transport.sent[0].subject
    assert "Parked because:" in transport.sent[0].body


def test_a_done_run_with_no_result_still_emails(conn):
    """Exited 0 claiming success but wrote nothing: exactly the kind of
    silence a human should hear about."""
    finished(conn, sentry.RUN_KIND, "sentry:U-3", None)
    transport = FakeTransport()

    send_pass(conn, transport)
    assert "finished without a structured result" in transport.sent[0].subject


def test_a_done_review_that_chained_is_suppressed(conn):
    run_id = finished(
        conn,
        jobs.REVIEW_KIND,
        "sentry:U-4",
        {"verdict": "REFUTED", "reasoning": "wrong"},
        payload=CHAIN_PAYLOAD,
    )
    transport = FakeTransport()

    assert send_pass(conn, transport) == 0
    assert "suppressed" in email_events(conn, run_id)[0].payload


def test_a_digest_failure_marks_nothing_and_retries(conn):
    finished(conn, sentry.RUN_KIND, "sentry:D-1", {"outcome": "NOT_A_BUG"})
    finished(conn, sentry.RUN_KIND, "sentry:D-2", {"outcome": "NOT_A_BUG"})
    transport = FakeTransport()
    transport.fail_times = 0

    class DigestFails(FakeTransport):
        def __call__(self, email):
            if "digest" in email.subject:
                raise OSError("relay refused the digest")
            super().__call__(email)

    failing = DigestFails()
    assert notify.pass_once(conn, failing, to=TO, cap=1) == 1  # digest failed

    ok = FakeTransport()
    notify.pass_once(conn, ok, to=TO, cap=1)  # budget spent -> digest retried
    assert any("digest" in e.subject for e in ok.sent)


# --- the CLI ------------------------------------------------------------------


def test_dry_run_prints_without_sending_or_marking(conn, monkeypatch, capsys):
    from contextlib import contextmanager

    run_id = finished(conn, sentry.RUN_KIND, "sentry:CLI-1", {"outcome": "NOT_A_BUG"})

    @contextmanager
    def fake_connect(dsn=None):
        yield conn

    monkeypatch.setattr(notify, "connect", fake_connect)
    assert notify.main(["--dry-run"]) == 0

    out = capsys.readouterr().out
    assert "sentry:CLI-1: NOT_A_BUG" in out
    assert "(no pull request)" in out
    assert email_events(conn, run_id) == [], "dry run must mark nothing"


def test_the_cli_send_path_respects_a_disabled_notifier(conn, monkeypatch, capsys):
    from contextlib import contextmanager

    finished(conn, sentry.RUN_KIND, "sentry:CLI-2", {"outcome": "NOT_A_BUG"})

    @contextmanager
    def fake_connect(dsn=None):
        yield conn

    monkeypatch.setattr(notify, "connect", fake_connect)
    monkeypatch.setattr(notify.config, "NOTIFY_TO", "")
    assert notify.main([]) == 0
    assert "sent 0 email(s)" in capsys.readouterr().out
