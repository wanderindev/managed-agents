"""Email the human when a run needs one. Issue #9.

The rule this module exists to enforce: **silence must never mean "nothing
happened"**. Every run that reaches a state a human should know about — parked
at AWAITING_HUMAN, a NOT_A_BUG or NEEDS_HUMAN verdict, or a crash — produces
exactly one email, and every run the notifier deliberately does not email gets
an ``email_sent`` event saying why, so the log never has an unexamined run.

At most one email per run, with one exception: while dispatch is paused on a
dead Claude login, a reminder with the queued-run count goes out once a day
(#59), because the fix is a human at a terminal and one email is easy to miss.
Past the daily cap the remainder go into a single
digest instead of being suppressed. Emails link; they never dump — no stack
traces, no transcripts, no secrets.

    python -m orchestrator.notify --dry-run   # print what would be sent

The loop calls :func:`pass_once` every tick; the send happens *before* the
``email_sent`` event is appended, so a crash between the two costs a duplicate
email rather than a silent never-send.
"""

import argparse
import logging
import smtplib
import sys
from collections.abc import Callable
from dataclasses import dataclass
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import psycopg

from orchestrator import auth, config, driver, log, queue
from orchestrator.db import connect
from orchestrator.driver import RUN_KIND as DRIVE_KIND
from orchestrator.enums import EventType
from orchestrator.jobs import (
    DREAM_KIND,
    PR_REVISION_KIND,
    RESEARCH_REVISE_KIND,
    RESEARCH_WRITE_KIND,
    REVIEW_KIND,
    REVISION_KIND,
    RUBRIC_VERIFY_KIND,
)
from orchestrator.sources.sentry import RUN_KIND as TRIAGE_KIND

logger = logging.getLogger(__name__)

#: Kinds whose outcomes a human cares about. Smoke runs never email.
_KINDS = (
    TRIAGE_KIND,
    REVISION_KIND,
    REVIEW_KIND,
    PR_REVISION_KIND,
    DREAM_KIND,
    DRIVE_KIND,
    RESEARCH_WRITE_KIND,
    RUBRIC_VERIFY_KIND,
    RESEARCH_REVISE_KIND,
)

_warned_disabled = False


@dataclass(frozen=True, slots=True)
class Email:
    to: str
    subject: str
    body: str


Transport = Callable[[Email], None]


def smtp_send(email: Email) -> None:
    """The PIC transport shape: Workspace SMTP relay, STARTTLS, no auth."""
    msg = MIMEText(email.body, "plain", "utf-8")
    msg["Subject"] = email.subject
    msg["From"] = config.NOTIFY_FROM
    msg["To"] = email.to
    with smtplib.SMTP(
        config.NOTIFY_SMTP_HOST,
        config.NOTIFY_SMTP_PORT,
        timeout=config.NOTIFY_SMTP_TIMEOUT,
    ) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.send_message(msg)


# --- deciding ----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Candidate:
    run_id: int
    kind: str
    status: str
    subject: str
    attempts: int
    payload: dict[str, Any]


def _candidates(conn: psycopg.Connection) -> list[_Candidate]:
    """Runs in a human-relevant state that have never been examined."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT r.id, r.kind, r.status, r.subject, r.attempts,"
            " (SELECT e.payload FROM agent_events e"
            "   WHERE e.run_id = r.id AND e.type = 'run_queued'"
            "   ORDER BY e.seq LIMIT 1) AS payload"
            " FROM agent_runs r"
            " WHERE r.kind = ANY(%s)"
            "   AND r.status IN ('AWAITING_HUMAN', 'DONE', 'FAILED', 'ABANDONED')"
            "   AND NOT EXISTS (SELECT 1 FROM agent_events e"
            "                    WHERE e.run_id = r.id AND e.type = %s)"
            " ORDER BY r.id",
            (list(_KINDS), EventType.EMAIL_SENT.value),
        )
        return [
            _Candidate(
                run_id=row["id"],
                kind=row["kind"],
                status=row["status"],
                subject=row["subject"],
                attempts=row["attempts"],
                payload=row["payload"] or {},
            )
            for row in cur.fetchall()
        ]


def _queued_for(conn: psycopg.Connection, failed: dict) -> dict[str, int] | None:
    """The queued counts an auth-failure email reports; None for any other."""
    return queue.queued_by_kind(conn) if failed.get("reason") == "auth" else None


def _sent_today(conn: psycopg.Connection) -> int:
    """Individual emails sent today. Digests and suppressions do not count:
    the digest is the overflow mechanism and must not consume the cap."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_events"
            " WHERE type = %s AND created_at >= date_trunc('day', now())"
            "   AND NOT (payload ? 'digest') AND NOT (payload ? 'suppressed')"
            "   AND NOT (payload ? 'auth_reminder')",
            (EventType.EMAIL_SENT.value,),
        )
        return cur.fetchone()["n"]


def _latest_gate(conn: psycopg.Connection, run_id: int) -> dict[str, Any]:
    event = log.latest_event(conn, run_id, EventType.HUMAN_GATE)
    return event.payload if event else {}


def _failed_payload(conn: psycopg.Connection, cand: _Candidate) -> dict[str, Any]:
    """The ``run_failed`` payload of a FAILED run: it carries the reason when
    the loop classified the failure (a dead login, see orchestrator.auth)."""
    if cand.status != "FAILED":
        return {}
    event = log.latest_event(conn, cand.run_id, EventType.RUN_FAILED)
    return event.payload if event else {}


def _headline(
    cand: _Candidate, result: dict, gate: dict, failed: dict | None = None
) -> str | None:
    """One line saying why this email exists, or None to suppress (with the
    reason recorded). Suppression means the chain already carried the outcome
    forward, not that nothing happened."""
    failed = failed or {}
    if cand.status == "AWAITING_HUMAN":
        verdict = gate.get("verdict")
        if verdict == "STANDS":
            if gate.get("remaining"):
                # A mitigation: merging it does not close the Sentry issue, and
                # the subject line is the only part some readers get.
                return (
                    "mitigation ready for review (adversary: STANDS, issue stays open)"
                )
            return "fix ready for review (adversary: STANDS)"
        if verdict == "UNCERTAIN":
            return "adversary UNCERTAIN — read the doubt before merging"
        return gate.get("why") or "awaiting a human decision"
    if cand.status == "FAILED":
        if failed.get("reason") == "auth":
            # The one failure that is not about this run at all. Said in the
            # subject, because the fix is a human typing a login on the host
            # and every queued run waits for it (the loop pauses dispatch).
            return "Claude login EXPIRED on the agents droplet — re-run `claude auth login`"
        if failed.get("reason") == driver.REASON_NO_RESEARCH:
            # The queue has nothing to plan from. The fix is a human approving
            # (or adding) research in PIC, and every scheduled drive fails the
            # same way until then — so the subject says what to do, not "FAILED".
            return "daily drive: NO APPROVED RESEARCH in PIC — approve or add research"
        if failed.get("reason") == driver.REASON_PROTOCOL:
            return (
                f"daily drive stopped: {failed.get('what', 'queue')}"
                f" answered {failed.get('status', '?')}"
            )
        return "run FAILED (sandbox exited nonzero)"
    if cand.status == "ABANDONED":
        return f"run ABANDONED after {cand.attempts} attempts"
    # DONE:
    outcome = result.get("outcome")
    if cand.kind in (TRIAGE_KIND, REVISION_KIND, PR_REVISION_KIND):
        if outcome in ("NOT_A_BUG", "NEEDS_HUMAN"):
            return outcome
        if outcome in ("FIX", "MITIGATION"):
            return None  # the chain continues; the review run will email
        return "finished without a structured result"
    if cand.kind == DREAM_KIND:
        # A dream with findings parks AWAITING_HUMAN and is handled above;
        # DONE means it found nothing, and that is still said once — a weekly
        # audit whose silence could mean "clean" or "never ran" is worthless.
        if outcome == "CLEAN":
            return "memory audit: CLEAN"
        if outcome == "NO_CHANGE":
            # Re-verified the already-open memory PR and found nothing new.
            # Said once, like CLEAN, for the same reason; the comment the run
            # left on that PR carries the detail.
            return "memory audit: nothing new (an open memory PR already has it)"
        return "memory audit finished without a structured result"
    if cand.kind in (RESEARCH_WRITE_KIND, RESEARCH_REVISE_KIND):
        if outcome in ("WROTE", "REVISED"):
            return None  # the chain continues; the verify run will email
        return "finished without a structured result"
    if cand.kind == RUBRIC_VERIFY_KIND:
        # Passing and bound-exhausted grades park AWAITING_HUMAN above; a DONE
        # verify chained a revision, and that run (or the next grade) emails.
        if result.get("verdicts"):
            return None
        return "finished without a structured result"
    if cand.kind == DRIVE_KIND:
        # An incomplete drive parks AWAITING_HUMAN and is handled above; a
        # COMPLETE Saturday still gets its one line, same silence rule.
        if outcome == "COMPLETE":
            return "weekly drive: COMPLETE"
        return "weekly drive finished without a structured result"
    return None  # a DONE review chained a revision; that run will email


def _queued_lines(queued: dict[str, int]) -> str:
    total = sum(queued.values())
    lines = [f"{total} run(s) are queued and waiting:"]
    lines += [f"  {kind}: {n}" for kind, n in queued.items()]
    return "\n".join(lines)


#: What to do about a paused login. Shared by the first email and the daily
#: reminder, so the two can never give different instructions.
_AUTH_FIX = (
    "The poll and the dream schedule do not enqueue while paused, and queued"
    " dreams for one repo coalesce to the newest. Anything else queued"
    " dispatches all at once after the re-login, so look first and cancel what"
    " is stale (dry-run, then for real):\n"
    "  python -m orchestrator.queue cancel --older-than 1d --dry-run\n"
    "  python -m orchestrator.queue cancel --older-than 1d\n\n"
    "Fix (interactive, cannot be automated):\n"
    "  ssh -t wanderindev@<agents droplet> claude auth login\n\n"
    "Dispatch resumes on the next tick after the credential file is"
    " rewritten; no restart needed."
)


def _body(
    cand: _Candidate,
    result: dict,
    gate: dict,
    failed: dict | None = None,
    queued: dict[str, int] | None = None,
) -> str:
    payload = cand.payload
    failed = failed or {}
    if failed.get("reason") == "auth":
        return (
            "The sandbox could not authenticate: the host's Claude OAuth session"
            " expired and could not be refreshed. This is a host condition, not a"
            " problem with this run.\n\n"
            "The orchestrator has PAUSED dispatch. Every queued run waits; nothing"
            " else will be burned. A reminder follows once a day while it stays"
            " paused.\n\n"
            f"{_queued_lines(queued or {})}\n\n"
            f"{_AUTH_FIX}\n\n"
            f"(run {cand.run_id}, kind {cand.kind}, subject {cand.subject}.)"
        )
    if failed.get("reason") in (driver.REASON_NO_RESEARCH, driver.REASON_PROTOCOL):
        what = failed.get("what", "queue")
        lines = [
            (
                f"PIC's agent-task queue ({payload.get('base_url', '?')}) would not"
                f" let the drive proceed: {what} answered {failed.get('status', '?')}."
            ),
            "",
            "Response:",
            str(failed.get("detail") or "(empty)"),
            "",
        ]
        if failed.get("reason") == driver.REASON_NO_RESEARCH:
            lines += [
                (
                    "plan-week only plans from APPROVED research. Research that was"
                    " written but never approved does not count. Approve it (or add"
                    " more) in the PIC admin, and the next scheduled drive plans"
                    " that day's series on its own; nothing to restart here."
                ),
                "",
                "Until then this email repeats once per scheduled drive.",
                "",
            ]
        if result.get("summary"):
            lines += ["Session:", result["summary"], ""]
        lines += [
            (
                f"(run {cand.run_id}, kind {cand.kind}, subject {cand.subject}."
                " Full history: agent_events in the orchestrator database.)"
            )
        ]
        return "\n".join(lines)
    pr_url = gate.get("pr_url") or result.get("pr_url") or payload.get("pr_url")
    lines = [
        f"Repository:  {payload.get('repo', '?')}",
        (
            f"Sentry:      {payload.get('short_id', cand.subject)}"
            f"  ({payload.get('title', '')})"
        ),
        f"Permalink:   {payload.get('permalink', '?')}",
        f"Pull request: {pr_url or '(no pull request)'}",
        "",
    ]
    if result.get("summary"):
        lines += ["What happened:", result["summary"], ""]
    if result.get("reason"):
        lines += ["Reason:", result["reason"], ""]
    if gate.get("verdict"):
        # The adversary's words, verbatim — this is the part that lets a
        # merge/close decision happen without opening the repo.
        lines += [
            f"Adversarial verdict: {gate['verdict']}",
            gate.get("reasoning") or "",
            "",
        ]
    elif gate.get("why"):
        lines += [f"Parked because: {gate['why']}", gate.get("reason") or "", ""]
    if gate.get("remaining"):
        # A mitigation's whole point: what merging this does NOT solve.
        lines += ["Still unfixed after this merges:", gate["remaining"], ""]
    if gate.get("flagged"):
        # The dreamer's contradictions and deletion candidates. Listed in
        # full: these are precisely the edits nothing will apply for you.
        lines += ["Flagged for review (never auto-applied):"]
        for item in gate["flagged"]:
            lines += [
                f"- [{item.get('class', '?')}] {item.get('claim', '?')}",
                f"    evidence: {item.get('evidence', '?')}",
            ]
        lines += [""]
    if gate.get("parked_tasks"):
        # The drive's stuck steps; the admin queue page is the fixing tool.
        lines += ["Parked tasks (retry or skip them from the admin queue page):"]
        for item in gate["parked_tasks"]:
            lines += [
                (
                    f"- task {item.get('task_id')}  {item.get('kind', '?')} on"
                    f" {item.get('subject', '?')}:"
                    f" {item.get('error') or '(no error text)'}"
                )
            ]
        lines += [""]
    if result.get("test"):
        lines += [f"Covered by: {result['test']}", ""]
    lines += [
        (
            f"(run {cand.run_id}, kind {cand.kind}, status {cand.status}."
            " Full history: agent_events in the orchestrator database.)"
        ),
    ]
    return "\n".join(lines)


def _digest_body(remaining: list[tuple[_Candidate, str]]) -> str:
    lines = [
        (
            f"The daily email cap ({config.NOTIFY_DAILY_CAP}) is reached."
            f" {len(remaining)} more run(s) need attention:"
        ),
        "",
    ]
    for cand, headline in remaining:
        lines.append(f"- run {cand.run_id}  {cand.subject}: {headline}")
    lines += ["", "Each is marked as notified; none will email again."]
    return "\n".join(lines)


# --- the paused-login reminder ----------------------------------------------


def _auth_reminder(
    conn: psycopg.Connection, credentials: Path
) -> tuple[Email, int] | None:
    """Today's "login still expired" email (recipient unset) and the run it
    is recorded on, or None when none is due.

    Due while dispatch is paused (the loop's own condition, ``auth.paused``)
    and the newest auth-failed run has had its first email but nothing today.
    Stateless like the rest: the ``email_sent`` events are the memory. The
    first-email guard keeps the reminder from stealing that run's one outcome
    email, which ``_candidates`` would otherwise skip forever.
    """
    if auth.paused(conn, credentials) is None:
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT f.run_id, f.created_at,"
            " EXISTS (SELECT 1 FROM agent_events e"
            "          WHERE e.run_id = f.run_id AND e.type = %s) AS emailed,"
            " EXISTS (SELECT 1 FROM agent_events e"
            "          WHERE e.run_id = f.run_id AND e.type = %s"
            "            AND e.created_at >= date_trunc('day', now())) AS today"
            " FROM agent_events f"
            " WHERE f.type = %s AND f.payload->>'reason' = 'auth'"
            " ORDER BY f.id DESC LIMIT 1",
            (
                EventType.EMAIL_SENT.value,
                EventType.EMAIL_SENT.value,
                EventType.RUN_FAILED.value,
            ),
        )
        row = cur.fetchone()
    if row is None or not row["emailed"] or row["today"]:
        return None
    queued = queue.queued_by_kind(conn)
    since = row["created_at"].strftime("%Y-%m-%d %H:%M %Z").strip()
    email = Email(
        to="",
        subject=(
            "[managed-agents] Claude login STILL EXPIRED on the agents droplet"
            f" — dispatch paused since {since[:10]}, {sum(queued.values())} queued"
        ),
        body=(
            f"Dispatch has been paused since {since} (run {row['run_id']} failed"
            " to authenticate) and the credential file has not changed since.\n\n"
            f"{_queued_lines(queued)}\n\n"
            f"{_AUTH_FIX}\n\n"
            "This reminder repeats once a day while dispatch stays paused."
        ),
    )
    return email, row["run_id"]


def _remind_paused_login(
    conn: psycopg.Connection, transport: Transport, to: str, credentials: Path
) -> int:
    due = _auth_reminder(conn, credentials)
    if due is None:
        return 0
    draft, run_id = due
    email = Email(to=to, subject=draft.subject, body=draft.body)
    try:
        transport(email)
    except Exception:
        logger.exception("could not send the paused-login reminder; will retry")
        return 0
    with conn.transaction():
        log.append(
            conn,
            run_id,
            EventType.EMAIL_SENT,
            {"to": to, "subject": email.subject, "auth_reminder": True},
        )
    logger.info("emailed the paused-login reminder: %s", email.subject)
    return 1


# --- the pass ----------------------------------------------------------------


def pass_once(
    conn: psycopg.Connection,
    transport: Transport = smtp_send,
    *,
    to: str | None = None,
    cap: int | None = None,
    credentials: str | Path | None = None,
) -> int:
    """Examine every unexamined finished run; email or record why not. Then,
    while dispatch is paused on a dead login, send the daily reminder.

    Returns the number of emails sent (digest and reminder included). A transport failure
    aborts the pass with nothing marked, so the next tick retries — the
    at-least-once direction, chosen because a duplicate email is annoying and
    a silently lost one violates the whole point of #9.
    """
    global _warned_disabled
    to = config.NOTIFY_TO if to is None else to
    cap = config.NOTIFY_DAILY_CAP if cap is None else cap
    if not to:
        if not _warned_disabled:
            logger.warning("ORCHESTRATOR_NOTIFY_TO is not set; outcome emails are off")
            _warned_disabled = True
        return 0
    sent = _examine(conn, transport, to, cap)
    # After the candidates, so the day an auth failure lands, its own email
    # goes first and counts as that day's.
    return sent + _remind_paused_login(
        conn, transport, to, Path(credentials or config.CLAUDE_CREDENTIALS)
    )


def _examine(conn: psycopg.Connection, transport: Transport, to: str, cap: int) -> int:
    candidates = _candidates(conn)
    if not candidates:
        return 0

    budget = max(cap - _sent_today(conn), 0)
    sent = 0
    overflow: list[tuple[_Candidate, str]] = []
    for cand in candidates:
        stage = log.last_completed_stage(conn, cand.run_id) or {}
        result = stage.get("result") or {}
        gate = _latest_gate(conn, cand.run_id)
        failed = _failed_payload(conn, cand)
        headline = _headline(cand, result, gate, failed)

        if headline is None:
            with conn.transaction():
                log.append(
                    conn,
                    cand.run_id,
                    EventType.EMAIL_SENT,
                    {"suppressed": "the chain carried this outcome forward"},
                )
            continue

        if sent >= budget:
            overflow.append((cand, headline))
            continue

        email = Email(
            to=to,
            subject=(
                f"[managed-agents] {cand.payload.get('repo', '?')}"
                f" {cand.payload.get('short_id', cand.subject)}: {headline}"
            ),
            body=_body(cand, result, gate, failed, _queued_for(conn, failed)),
        )
        try:
            transport(email)
        except Exception:
            logger.exception(
                "could not send for run %s; pass aborted, will retry", cand.run_id
            )
            return sent
        with conn.transaction():
            log.append(
                conn,
                cand.run_id,
                EventType.EMAIL_SENT,
                {"to": to, "subject": email.subject},
            )
        sent += 1
        logger.info("emailed run %s: %s", cand.run_id, email.subject)

    if overflow:
        digest = Email(
            to=to,
            subject=(
                f"[managed-agents] digest: {len(overflow)} more run(s) need attention"
            ),
            body=_digest_body(overflow),
        )
        try:
            transport(digest)
        except Exception:
            logger.exception("could not send the digest; pass aborted, will retry")
            return sent
        for cand, headline in overflow:
            with conn.transaction():
                log.append(
                    conn,
                    cand.run_id,
                    EventType.EMAIL_SENT,
                    {"to": to, "digest": True, "headline": headline},
                )
        sent += 1
        logger.info("digest sent covering %s runs", len(overflow))
    return sent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print what would be sent without sending or marking anything",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s"
    )
    with connect() as conn:
        if args.dry_run:
            for cand in _candidates(conn):
                stage = log.last_completed_stage(conn, cand.run_id) or {}
                result = stage.get("result") or {}
                gate = _latest_gate(conn, cand.run_id)
                failed = _failed_payload(conn, cand)
                headline = _headline(cand, result, gate, failed)
                marker = headline or "(suppressed: chain carried it forward)"
                print(f"run {cand.run_id}  {cand.subject}: {marker}")
                if headline:
                    print("---")
                    print(_body(cand, result, gate, failed, _queued_for(conn, failed)))
                    print("===")
            due = _auth_reminder(conn, Path(config.CLAUDE_CREDENTIALS))
            if due is not None:
                print(f"paused-login reminder: {due[0].subject}")
                print("---")
                print(due[0].body)
                print("===")
            return 0
        sent = pass_once(conn)
        print(f"sent {sent} email(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
