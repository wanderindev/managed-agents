"""The dreaming job: audit a repo's memory against what the runs learned. #15.

    python -m orchestrator.dream [--repo REPO] [--days N] [--dry-run]

Scheduled weekly (cron, Mondays, like the poll a one-shot) and separate from
the loop for the same reason the poll is: scheduling is the operating system's
job, and running it twice is harmless — one dream run per repo per day, ever,
enforced by the subject.

Which repos: ``ORCHESTRATOR_DREAM_REPOS`` when set, else every repo the
orchestrator's own runs touched in the window (#60). The explicit list exists
because most of the work on these repos happens in interactive sessions that
leave no run behind.

The "memory corpus" here is the repo's ``CLAUDE.md``: it is the file the
sandbox agents actually load, and feliu-dev's own Agentic Protocols section
defines it as memory ("If you learn a new persistent pattern about this
codebase, update this file"). The evidence is two things: the transcripts in
``agent_events``, and the pull requests merged in the window — whoever wrote
them, so interactive and epic-runner work counts too (#60). This module builds
the *digest* of both — mechanically, no LLM — at enqueue time, so the run's
payload is a durable, self-contained record of the evidence the dreamer was
shown, exactly like the triage prompt's Sentry detail (#7).

One memory pull request at a time, accumulating (#45). A dream used to cut its
branch from main unconditionally, so while a memory PR sat unmerged every later
run re-derived the same edits onto a rival branch — feliu-dev's runs 21, 23 and
25 opened three PRs, two of them byte-identical. Now the enqueue looks for the
open one and points the run at its branch: the audit sees the earlier edits as
already applied, adds only what is new, and either commits onto that branch or
just comments. The human merges one PR whenever they get to it.
"""

import argparse
import json
import logging
import sys
import urllib.error
from datetime import datetime, timedelta
from typing import Any

import psycopg

from orchestrator import auth, config, github, jobs, queue
from orchestrator.db import connect
from orchestrator.enums import EventType
from orchestrator.log import create_run, load_events
from orchestrator.queue import get_run
from orchestrator.sandbox import DEFAULT_BRANCH
from orchestrator.sources import github_prs

logger = logging.getLogger(__name__)

#: Most recent runs per repo that make it into the digest, before the byte
#: bound below. Sized for a busy week now that dreaming is weekly (#60).
#: Anything past it is not dropped silently: the payload records how many runs
#: were left out, and the brief says so.
MAX_DIGEST_RUNS = 40

#: Byte bound on the serialized runs + merged-PR evidence. The entrypoint reads
#: /work/prompt.txt into one shell variable and passes it as `claude -p
#: "$PROMPT"`, a single argv string, and Linux caps one argument at 128 KiB
#: (MAX_ARG_STRLEN). Past that exec fails with "Argument list too long" on every
#: retry and the repo is never audited. A result-bearing run is ~5 KB (dream
#: run 168: 47.7 KB for 9 entries), so 40 of them would blow it; 64 KB leaves
#: the rest of the prompt ample room. Oldest entries are trimmed first.
MAX_EVIDENCE_BYTES = 64 * 1024

#: Most recent merged pull requests per repo in the digest, same reasoning.
MAX_MERGED_PRS = 40

#: Changed-file names listed per merged PR; the rest are counted, not named.
_MAX_FILES_LISTED = 15

#: Pages of 100 closed PRs walked looking for the window's merges. Sorted by
#: last update, so the walk stops as soon as a page reaches back past the
#: window; this only bounds a pathological repo.
_MAX_CLOSED_PAGES = 5

#: What a GitHub failure is, for the lookups that must never stop an audit.
_GITHUB_ERRORS = (urllib.error.URLError, OSError, ValueError)

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

    Without this the dreamer re-derives the same edits every run: each run
    cuts a fresh branch from main, main still lacks the last run's unmerged fix,
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
    full_repo = _full_repo(repo)
    try:
        pulls = client.open_pulls(full_repo)
    except _GITHUB_ERRORS as exc:
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


def _full_repo(repo: str) -> str:
    owner = config.GITHUB_REMOTE_BASE.rstrip("/").rsplit("/", 1)[-1]
    return f"{owner}/{repo}"


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def default_branch(client: github_prs.PullsClient, *, repo: str) -> str:
    """The repo's default branch as GitHub reports it, else ``main``.

    Not every dreamed repo is on ``main`` (atelier-new-cli uses ``master``),
    and the sandbox cuts the dream's branch from this and the PR targets it
    (#60). Falling back is safe in the loud direction: a wrong guess makes the
    sandbox's fetch fail visibly rather than audit the wrong history.
    """
    full_repo = _full_repo(repo)
    try:
        branch = client.repository(full_repo).get("default_branch")
    except _GITHUB_ERRORS as exc:
        logger.warning("could not read %s's default branch: %s", full_repo, exc)
        return DEFAULT_BRANCH
    return branch or DEFAULT_BRANCH


def _files_summary(
    client: github_prs.PullsClient, full_repo: str, number: int
) -> dict[str, Any] | None:
    try:
        files = client.files(full_repo, number)
    except _GITHUB_ERRORS as exc:
        logger.warning("could not list files of %s#%s: %s", full_repo, number, exc)
        return None
    names = [f.get("filename") or "?" for f in files]
    summary: dict[str, Any] = {
        "count": len(names),
        "additions": sum(f.get("additions") or 0 for f in files),
        "deletions": sum(f.get("deletions") or 0 for f in files),
        "files": names[:_MAX_FILES_LISTED],
    }
    if len(names) > _MAX_FILES_LISTED:
        summary["more_files"] = len(names) - _MAX_FILES_LISTED
    return summary


def merged_pulls(
    client: github_prs.PullsClient, *, repo: str, since: datetime
) -> tuple[list[dict[str, Any]], int]:
    """Pull requests merged into ``repo`` since ``since``, newest first.

    Returns the digested entries (at most ``MAX_MERGED_PRS``) and how many
    merges the window actually held, so the caller can say what it omitted.

    Whoever opened them: the point (#60) is to show the dreamer the work that
    happens outside this orchestrator — interactive sessions, the epic runner —
    which leaves no run in the log. Title and changed files, never the diff:
    the dreamer has the repository itself for detail.

    A GitHub failure yields what was gathered so far rather than stopping the
    audit, same as :func:`open_memory_pr`.
    """
    full_repo = _full_repo(repo)
    found: list[dict[str, Any]] = []
    for page in range(1, _MAX_CLOSED_PAGES + 1):
        try:
            pulls = client.closed_pulls(full_repo, page)
        except _GITHUB_ERRORS as exc:
            logger.warning("could not list closed PRs for %s: %s", full_repo, exc)
            break
        for pull in pulls:
            merged_at = pull.get("merged_at")
            if merged_at and _parse_time(merged_at) >= since:
                found.append(pull)
        # Sorted by last update, newest first, and a merge is an update: once a
        # page reaches back past the window, no later page can hold a merge
        # inside it.
        oldest = pulls[-1].get("updated_at") if pulls else None
        if len(pulls) < 100 or not oldest or _parse_time(oldest) < since:
            break

    found.sort(key=lambda p: p["merged_at"], reverse=True)
    if len(found) > MAX_MERGED_PRS:
        logger.info(
            "%s: %s merged PRs in the window, digesting the newest %s",
            full_repo,
            len(found),
            MAX_MERGED_PRS,
        )
    entries = [
        {
            "number": pull.get("number"),
            "title": _clip(pull.get("title") or ""),
            "author": (pull.get("user") or {}).get("login"),
            "merged_at": pull["merged_at"],
            "changed": _files_summary(client, full_repo, pull.get("number")),
        }
        for pull in found[:MAX_MERGED_PRS]
    ]
    return entries, len(found)


def _line_bytes(entry: dict[str, Any]) -> int:
    """Bytes one entry adds to the brief: one JSON line, as jobs.py writes it."""
    return len(json.dumps(entry).encode()) + 1


def fit_evidence(
    runs: list[dict[str, Any]],
    prs: list[dict[str, Any]],
    max_bytes: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Trim both lists, oldest first, until their serialized size fits.

    Runs go first: a PR entry is small and covers work no run saw, while a
    result-bearing run is ~5 KB. Both lists arrive newest first, so trimming
    drops from the tail. The caller counts what was dropped into the payload,
    so the brief always says what it left out.
    """
    limit = MAX_EVIDENCE_BYTES if max_bytes is None else max_bytes
    runs, prs = list(runs), list(prs)
    size = sum(map(_line_bytes, runs)) + sum(map(_line_bytes, prs))
    while size > limit and runs:
        size -= _line_bytes(runs.pop())
    while size > limit and prs:
        size -= _line_bytes(prs.pop())
    return runs, prs


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


def run_count(conn: psycopg.Connection, repo: str, days: int) -> int:
    """All runs on ``repo`` in the window, so the brief can say what it omits."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_runs r"
            " WHERE r.updated_at > now() - make_interval(days => %s)"
            "   AND (SELECT e.payload->>'repo' FROM agent_events e"
            "         WHERE e.run_id = r.id AND e.type = 'run_queued') = %s",
            (days, repo),
        )
        return cur.fetchone()["n"]


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

    ``pulls`` is optional. Given a client, the digest also carries the pull
    requests merged in the window, the run targets the repo's real default
    branch (#60), and it is pointed at whatever memory pull request is still
    open so it accumulates onto that branch instead of opening a rival one
    (#45). Without it — no GitHub App configured, or a caller that does not
    care — the evidence is the orchestrator's own runs and the base is ``main``.
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
    since = queue.db_now(conn) - timedelta(days=days)
    merged, prs_total = (
        merged_pulls(pulls, repo=repo, since=since) if pulls else ([], 0)
    )
    if not entries and not merged:
        logger.info(
            "skipping %s: no runs and no merged PRs in the last %s day(s)", repo, days
        )
        return None

    runs_total = (
        run_count(conn, repo, days) if len(entries) == MAX_DIGEST_RUNS else len(entries)
    )
    entries, merged = fit_evidence(entries, merged)
    payload: dict[str, Any] = {
        "repo": repo,
        "days": days,
        "base_branch": default_branch(pulls, repo=repo) if pulls else DEFAULT_BRANCH,
        "digest": entries,
        "merged_prs": merged,
    }
    if runs_total > len(entries):
        payload["runs_omitted"] = runs_total - len(entries)
    if prs_total > len(merged):
        payload["prs_omitted"] = prs_total - len(merged)
    open_pr = open_memory_pr(conn, pulls, repo=repo) if pulls else None
    if open_pr:
        # Recorded in the payload, like the digest, so the run stays a durable
        # self-contained account of what the dreamer was pointed at.
        payload["open_pr"] = open_pr
    if not dry_run:
        with conn.transaction():
            create_run(conn, jobs.DREAM_KIND, subject, payload)
    logger.info(
        "%s %s (%s run(s) and %s merged PR(s) in the digest, base %s%s)",
        "would enqueue" if dry_run else "enqueued",
        subject,
        len(entries),
        len(merged),
        payload["base_branch"],
        f"; accumulating onto PR #{open_pr['number']}" if open_pr else "",
    )
    return subject


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo",
        help="dream about one repo; default: ORCHESTRATOR_DREAM_REPOS when set,"
        " else every repo with recent runs",
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
    with connect() as conn:
        if auth.skip_enqueue(conn, "memory dreams"):
            return 0
        pulls = _pulls_client()
        repos = (
            [args.repo]
            if args.repo
            else list(config.DREAM_REPOS) or recent_repos(conn, args.days)
        )
        if not repos:
            logger.info("no repos with run activity in the last %s day(s)", args.days)
        for repo in repos:
            enqueue(conn, repo=repo, days=args.days, dry_run=args.dry_run, pulls=pulls)
    return 0


def _pulls_client() -> github_prs.PullsClient | None:
    """A read-only pulls client, or None when the App is not usable.

    Degrading is correct rather than fatal — the audit itself needs no GitHub
    API, only the branch-reuse, merged-PR and default-branch lookups do — but
    it is said out loud, because the silent version of this failure is a
    duplicate PR every week and a digest blind to everything merged by hand.
    """
    try:
        return github_prs.PullsClient(github.from_config().installation_token())
    except github.GitHubAppError as exc:
        logger.warning(
            "no GitHub lookups (App not usable: %s): no merged PRs in the"
            " digest, base branch assumed main, and a dream may open a second"
            " memory PR alongside one already open",
            exc,
        )
        return None


if __name__ == "__main__":
    sys.exit(main())
