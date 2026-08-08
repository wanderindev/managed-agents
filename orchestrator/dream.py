"""The dreaming job: audit a repo's memory against what the runs learned. #15.

    python -m orchestrator.dream [--repo REPO] [--days N] [--dry-run]

Scheduled off-peak (a systemd timer or cron, like the poll) and separate from
the loop for the same reason the poll is: scheduling is the operating system's
job, and running it twice is harmless — one dream run per repo per day, ever,
enforced by the subject.

The "memory corpus" here is the repo's ``CLAUDE.md``: it is the file the
sandbox agents actually load, and feliu-dev's own Agentic Protocols section
defines it as memory ("If you learn a new persistent pattern about this
codebase, update this file"). The transcripts are ``agent_events``. This
module builds the *digest* of those transcripts — mechanically, no LLM —
at enqueue time, so the run's payload is a durable, self-contained record of
the evidence the dreamer was shown, exactly like the triage prompt's Sentry
detail (#7).

One memory pull request at a time, accumulating (#45). A dream used to cut its
branch from main unconditionally, so while a memory PR sat unmerged every later
run re-derived the same edits onto a rival branch — feliu-dev's runs 21, 23 and
25 opened three PRs, two of them byte-identical. Now the enqueue looks for the
open one and points the run at its branch: the audit sees yesterday's edits as
already applied, adds only what is new, and either commits onto that branch or
just comments. The human merges one PR whenever they get to it.
"""

import argparse
import logging
import sys
import urllib.error
from typing import Any

import psycopg

from orchestrator import config, github, jobs, queue
from orchestrator.db import connect
from orchestrator.enums import EventType
from orchestrator.log import create_run, load_events
from orchestrator.queue import get_run
from orchestrator.sources import github_prs

logger = logging.getLogger(__name__)

#: Most recent runs per repo that make it into the digest. Enough for a week
#: of this system's volume; a cap because the payload must stay well under the
#: event-log truncation ceiling or the brief silently loses its own evidence.
MAX_DIGEST_RUNS = 15

#: Per-field cap inside one digest entry, same reasoning.
_MAX_FIELD_CHARS = 600


def _clip(value: Any) -> Any:
    if isinstance(value, str) and len(value) > _MAX_FIELD_CHARS:
        return value[:_MAX_FIELD_CHARS] + "…"
    return value


def _clip_dict(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    return {k: _clip(v) for k, v in payload.items()}


def open_memory_pr(
    conn: psycopg.Connection,
    client: github_prs.PullsClient,
    *,
    repo: str,
    bot_login: str | None = None,
) -> dict[str, Any] | None:
    """The still-open memory pull request an earlier dream opened, or None.

    Without this the dreamer re-derives the same edits every night: each run
    cuts a fresh branch from main, main still lacks yesterday's unmerged fix,
    so the audit finds the same gap and opens another pull request. Runs 21,
    23 and 25 on feliu-dev produced three PRs, two of which were byte-identical
    (#45). Finding the open one lets the run add to it instead.

    Identified by the branch, not by the title or body: ``agent/run-N`` names
    the run that cut it, and that run's kind is what makes it a *memory* PR
    rather than a fix PR the same bot opened. The oldest wins — with the
    accumulating branch there should only ever be one, and if history left
    several, the earliest is the one carrying the edits.
    """
    bot_login = bot_login or config.GITHUB_BOT_LOGIN
    owner = config.GITHUB_REMOTE_BASE.rstrip("/").rsplit("/", 1)[-1]
    full_repo = f"{owner}/{repo}"
    try:
        pulls = client.open_pulls(full_repo)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        # Unreachable GitHub must not stop the audit: the run still has value
        # without the branch reuse, it just risks a duplicate PR a human closes.
        logger.warning("could not list open PRs for %s: %s", full_repo, exc)
        return None

    found: list[dict[str, Any]] = []
    for pull in pulls:
        if ((pull.get("user") or {}).get("login") or "") != bot_login:
            continue
        head_ref = (pull.get("head") or {}).get("ref") or ""
        run_id = github_prs.agent_branch_run_id(head_ref)
        if run_id is None:
            continue
        try:
            origin = get_run(conn, run_id)
        except LookupError:
            continue
        if origin.kind != jobs.DREAM_KIND:
            continue
        found.append(
            {
                "number": pull.get("number"),
                "branch": head_ref,
                "url": pull.get("html_url"),
            }
        )

    if not found:
        return None
    if len(found) > 1:
        logger.warning(
            "%s has %s open memory PRs (%s); accumulating onto the oldest",
            full_repo,
            len(found),
            ", ".join(f"#{p['number']}" for p in found),
        )
    return min(found, key=lambda p: p["number"])


def recent_repos(conn: psycopg.Connection, days: int) -> list[str]:
    """Repos with any run activity inside the window, oldest name first."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT (SELECT e.payload->>'repo' FROM agent_events e"
            "   WHERE e.run_id = r.id AND e.type = 'run_queued') AS repo"
            " FROM agent_runs r"
            " WHERE r.updated_at > now() - make_interval(days => %s)",
            (days,),
        )
        return sorted(row["repo"] for row in cur.fetchall() if row["repo"])


def digest(conn: psycopg.Connection, repo: str, days: int) -> list[dict[str, Any]]:
    """What the recent runs on ``repo`` established, distilled per run.

    Structured results, stage markers, and human gates — never raw transcript.
    The dreamer's job is comparing established facts against the memory file,
    and five hundred tool events per run would bury the twenty that matter.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT r.id, r.kind, r.subject, r.status, r.updated_at"
            " FROM agent_runs r"
            " WHERE r.updated_at > now() - make_interval(days => %s)"
            "   AND (SELECT e.payload->>'repo' FROM agent_events e"
            "         WHERE e.run_id = r.id AND e.type = 'run_queued') = %s"
            " ORDER BY r.id DESC LIMIT %s",
            (days, repo, MAX_DIGEST_RUNS),
        )
        rows = cur.fetchall()

    entries = []
    for row in rows:
        events = load_events(conn, row["id"])
        claude = [e.payload for e in events if e.type is EventType.CLAUDE_EVENT]
        result = None
        gate = None
        for event in events:
            if event.type is EventType.STAGE_COMPLETED:
                result = event.payload.get("result") or result
            elif event.type is EventType.HUMAN_GATE:
                gate = event.payload
        entries.append(
            {
                "run": row["id"],
                "kind": row["kind"],
                "subject": row["subject"],
                "status": row["status"],
                "finished": row["updated_at"].isoformat(),
                "result": _clip_dict(result),
                "stages": [_clip_dict(s) for s in jobs.stage_markers(claude)],
                "gate": _clip_dict(gate),
            }
        )
    return entries


def enqueue(
    conn: psycopg.Connection,
    *,
    repo: str,
    days: int,
    dry_run: bool = False,
    pulls: github_prs.PullsClient | None = None,
) -> str | None:
    """One dream run per repo per day; returns the subject, or None if skipped.

    ``pulls`` is optional: given a client, the run is pointed at whatever memory
    pull request is still open so it accumulates onto that branch instead of
    opening a rival one (#45). Without it — no GitHub App configured, or a
    caller that does not care — the run behaves exactly as it always has.
    """
    today = queue.db_now(conn).date().isoformat()
    subject = f"dream:{repo}:{today}"
    with conn.cursor() as cur:
        # Any run with this subject, terminal or not: a dream that already ran
        # today does not run again, which is what makes rescheduling harmless.
        cur.execute(
            "SELECT 1 FROM agent_runs WHERE kind = %s AND subject = %s LIMIT 1",
            (jobs.DREAM_KIND, subject),
        )
        if cur.fetchone():
            logger.info("skipping %s: already dreamed today", subject)
            return None

    entries = digest(conn, repo, days)
    if not entries:
        logger.info("skipping %s: no run activity in the last %s day(s)", repo, days)
        return None

    payload: dict[str, Any] = {"repo": repo, "days": days, "digest": entries}
    open_pr = open_memory_pr(conn, pulls, repo=repo) if pulls else None
    if open_pr:
        # Recorded in the payload, like the digest, so the run stays a durable
        # self-contained account of what the dreamer was pointed at.
        payload["open_pr"] = open_pr
    if not dry_run:
        with conn.transaction():
            create_run(conn, jobs.DREAM_KIND, subject, payload)
    logger.info(
        "%s %s (%s recent run(s) in the digest%s)",
        "would enqueue" if dry_run else "enqueued",
        subject,
        len(entries),
        f"; accumulating onto PR #{open_pr['number']}" if open_pr else "",
    )
    return subject


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo", help="dream about one repo; default: every repo with recent runs"
    )
    parser.add_argument(
        "--days", type=int, default=7, help="transcript lookback (default 7)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be enqueued without enqueuing it",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s"
    )
    pulls = _pulls_client()
    with connect() as conn:
        repos = [args.repo] if args.repo else recent_repos(conn, args.days)
        if not repos:
            logger.info("no repos with run activity in the last %s day(s)", args.days)
        for repo in repos:
            enqueue(conn, repo=repo, days=args.days, dry_run=args.dry_run, pulls=pulls)
    return 0


def _pulls_client() -> github_prs.PullsClient | None:
    """A read-only pulls client, or None when the App is not usable.

    Degrading is correct rather than fatal — the audit itself needs no GitHub
    API, only the branch-reuse lookup does — but it is said out loud, because
    the silent version of this failure is a duplicate PR every night.
    """
    try:
        return github_prs.PullsClient(github.from_config().installation_token())
    except github.GitHubAppError as exc:
        logger.warning(
            "no open-PR lookup (GitHub App not usable: %s); a dream may open a"
            " second memory PR alongside one already open",
            exc,
        )
        return None


if __name__ == "__main__":
    sys.exit(main())
