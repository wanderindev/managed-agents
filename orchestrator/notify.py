"""Email the human when a run needs one. Issue #9.

The rule this module exists to enforce: **silence must never mean "nothing
happened"**. Every run that reaches a state a human should know about — parked
at AWAITING_HUMAN, a NOT_A_BUG or NEEDS_HUMAN verdict, or a crash — produces
exactly one email, and every run the notifier deliberately does not email gets
an ``email_sent`` event saying why, so the log never has an unexamined run.

At most one email per run. Past the daily cap the remainder go into a single
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
from typing import Any

import psycopg

from orchestrator import config, log
from orchestrator.db import connect
from orchestrator.enums import EventType
from orchestrator.jobs import DREAM_KIND, PR_REVISION_KIND, REVIEW_KIND, REVISION_KIND
from orchestrator.sources.sentry import RUN_KIND as TRIAGE_KIND

logger = logging.getLogger(__name__)

#: Kinds whose outcomes a human cares about. Smoke runs never email.
_KINDS = (TRIAGE_KIND, REVISION_KIND, REVIEW_KIND, PR_REVISION_KIND, DREAM_KIND)

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


def _sent_today(conn: psycopg.Connection) -> int:
    """Individual emails sent today. Digests and suppressions do not count:
    the digest is the overflow mechanism and must not consume the cap."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_events"
            " WHERE type = %s AND created_at >= date_trunc('day', now())"
            "   AND NOT (payload ? 'digest') AND NOT (payload ? 'suppressed')",
            (EventType.EMAIL_SENT.value,),
        )
        return cur.fetchone()["n"]


def _latest_gate(conn: psycopg.Connection, run_id: int) -> dict[str, Any]:
    event = log.latest_event(conn, run_id, EventType.HUMAN_GATE)
    return event.payload if event else {}


def _headline(cand: _Candidate, result: dict, gate: dict) -> str | None:
    """One line saying why this email exists, or None to suppress (with the
    reason recorded). Suppression means the chain already carried the outcome
    forward, not that nothing happened."""
    if cand.status == "AWAITING_HUMAN":
        verdict = gate.get("verdict")
        if verdict == "STANDS":
            return "fix ready for review (adversary: STANDS)"
        if verdict == "UNCERTAIN":
            return "adversary UNCERTAIN — read the doubt before merging"
        return gate.get("why") or "awaiting a human decision"
    if cand.status == "FAILED":
        return "run FAILED (sandbox exited nonzero)"
    if cand.status == "ABANDONED":
        return f"run ABANDONED after {cand.attempts} attempts"
    # DONE:
    outcome = result.get("outcome")
    if cand.kind in (TRIAGE_KIND, REVISION_KIND, PR_REVISION_KIND):
        if outcome in ("NOT_A_BUG", "NEEDS_HUMAN"):
            return outcome
        if outcome == "FIX":
            return None  # the chain continues; the review run will email
        return "finished without a structured result"
    if cand.kind == DREAM_KIND:
        # A dream with findings parks AWAITING_HUMAN and is handled above;
        # DONE means it found nothing, and that is still said once — a weekly
        # audit whose silence could mean "clean" or "never ran" is worthless.
        if outcome == "CLEAN":
            return "memory audit: CLEAN"
        return "memory audit finished without a structured result"
    return None  # a DONE review chained a revision; that run will email


def _body(cand: _Candidate, result: dict, gate: dict) -> str:
    payload = cand.payload
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


# --- the pass ----------------------------------------------------------------


def pass_once(
    conn: psycopg.Connection,
    transport: Transport = smtp_send,
    *,
    to: str | None = None,
    cap: int | None = None,
) -> int:
    """Examine every unexamined finished run; email or record why not.

    Returns the number of emails sent (digest included). A transport failure
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
        headline = _headline(cand, result, gate)

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
            body=_body(cand, result, gate),
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
                headline = _headline(cand, result, gate)
                marker = headline or "(suppressed: chain carried it forward)"
                print(f"run {cand.run_id}  {cand.subject}: {marker}")
                if headline:
                    print("---")
                    print(_body(cand, result, gate))
                    print("===")
            return 0
        sent = pass_once(conn)
        print(f"sent {sent} email(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
