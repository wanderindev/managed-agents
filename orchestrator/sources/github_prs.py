"""The GitHub change-request work source. Issue #10.

Polls the open pull requests this orchestrator's App authored and enqueues a
``pr_revision`` run when a human has asked for changes: a new
``CHANGES_REQUESTED`` review, or new inline review comments, since the last
revision run for that pull request.

Boundaries, stated up front because each one is a guard against a real failure:

* Only PRs authored by the App's bot login are considered. A human's PR is
  never touched, however reviewable it looks.
* Only PRs whose originating run was a ``sentry_triage`` are considered. This
  loop's revision prompt is written for a Sentry fix; pointing it at a memory
  audit's pull request (markdown only, no Sentry issue behind it) would ask an
  agent to revise a fix that does not exist. An allowlist rather than a
  denylist, so any future kind that learns to open a PR is opted out until
  someone writes a prompt that fits it.
* A PR with commits by a human (Javier pushing to the agent's branch) is left
  alone entirely: the human has taken the wheel, and an agent appending to —
  let alone rewriting — their work is the exact incident this check prevents.
  Note the inverse check would not work: sandbox commits are authored by
  whatever identity the committing tool used (PR #431's commit shows login
  ``claude``), so "everything not by the bot is foreign" would flag the
  agent's own work.
* At most :data:`MAX_REVISION_ROUNDS` revision runs per pull request, ever.
  A PR that cannot converge in three rounds is a design disagreement, not a
  code problem, and the poll stops enqueueing and says so.

It also closes parked runs whose pull request a human has already decided
(#90): see :func:`close_decided`.

Read-only against GitHub. The revision sandbox is what pushes.
"""

import json
import logging
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import psycopg

from orchestrator import config, log, queue
from orchestrator.enums import EventType, RunStatus
from orchestrator.log import create_run
from orchestrator.queue import Run, get_run
from orchestrator.sandbox import DEFAULT_BRANCH
from orchestrator.sources.sentry import RUN_KIND as TRIAGE_KIND
from orchestrator.sources.sentry import PollReport

logger = logging.getLogger(__name__)

RUN_KIND = "pr_revision"

#: Revision runs per pull request, total, ever.
MAX_REVISION_ROUNDS = 3

#: Branches the triage stage cuts. The run id inside is how the poll finds the
#: originating run's payload (issue facts) without a second Sentry call.
_AGENT_BRANCH = re.compile(r"^agent/run-(\d+)$")

#: has_recent_run's cooldown, zeroed: unlike Sentry issues, a pull request
#: should get its revision as soon as the previous one is done — the human is
#: actively waiting. The cursor is what prevents re-handling the same comments.
_NO_COOLDOWN = timedelta(seconds=0)

#: How much of one comment body to carry in the payload. The sandbox has `gh`
#: and a token; it can read the full threads itself. The payload is the brief,
#: not the archive.
_MAX_COMMENT_CHARS = 1000

#: The Sentry fix chain (#90): kinds that park AWAITING_HUMAN on a pull request
#: a human is meant to merge or close. Literals because ``jobs`` imports this
#: module; a test keeps them in step with ``jobs.REVIEW_KIND`` and friends. An
#: allowlist, like the revision poll's: a dream's or a research run's park
#: means something else, and closing it on a merge is not this loop's call.
CHAIN_KINDS = (TRIAGE_KIND, "adversarial_review", "fix_revision", RUN_KIND)

#: ``html_url`` of a pull request, which is what every gate's ``pr_url`` holds.
_PR_URL = re.compile(r"^https://github\.com/([^/\s]+/[^/\s]+)/pull/(\d+)/?$")

#: ``run_done`` / ``run_abandoned`` payload ``reason`` for a run closed because
#: its pull request was decided. ``report.abandon_bucket`` keys on the prefix.
REASON_MERGED = "pr merged"
REASON_CLOSED = "pr closed without merging"


@dataclass(frozen=True, slots=True)
class ChangeRequest:
    """One thing a human asked for: a review verdict or an inline comment."""

    kind: str  # "review" | "comment"
    author: str
    body: str
    at: str  # ISO-8601; GitHub emits a uniform format, so strings compare
    path: str = ""
    line: int | None = None
    url: str = ""

    def as_payload(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "author": self.author,
            "body": self.body[:_MAX_COMMENT_CHARS],
            "at": self.at,
            "path": self.path,
            "line": self.line,
            "url": self.url,
        }


class PullsClient:
    """Minimal read-only GitHub pulls client, one installation token per poll.

    stdlib urllib for the same reason SentryClient uses it: one fewer
    dependency in a process whose job is to be boring.
    """

    def __init__(
        self,
        token: str,
        *,
        api_url: str = "https://api.github.com",
        opener=urllib.request.urlopen,
    ) -> None:
        self.token = token
        self.api_url = api_url.rstrip("/")
        self._open = opener

    def _get(self, path: str) -> Any:
        request = urllib.request.Request(
            f"{self.api_url}{path}",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "wanderindev-managed-agents",
            },
        )
        with self._open(request, timeout=30) as response:
            return json.loads(response.read().decode())

    def open_pulls(self, full_repo: str) -> list[dict]:
        return self._get(f"/repos/{full_repo}/pulls?state=open&per_page=100")

    def reviews(self, full_repo: str, number: int) -> list[dict]:
        return self._get(f"/repos/{full_repo}/pulls/{number}/reviews?per_page=100")

    def review_comments(self, full_repo: str, number: int) -> list[dict]:
        return self._get(f"/repos/{full_repo}/pulls/{number}/comments?per_page=100")

    def commits(self, full_repo: str, number: int) -> list[dict]:
        return self._get(f"/repos/{full_repo}/pulls/{number}/commits?per_page=100")

    def pull(self, full_repo: str, number: int) -> dict:
        """One pull request, open or not; ``merged`` says how it closed."""
        return self._get(f"/repos/{full_repo}/pulls/{number}")

    # --- for the dreaming job (#60) --------------------------------------------

    def repository(self, full_repo: str) -> dict:
        """The repo itself; the dreamer reads its ``default_branch``."""
        return self._get(f"/repos/{full_repo}")

    def closed_pulls(self, full_repo: str, page: int = 1) -> list[dict]:
        """Closed PRs, most recently updated first, one page of 100."""
        return self._get(
            f"/repos/{full_repo}/pulls?state=closed&sort=updated&direction=desc"
            f"&per_page=100&page={page}"
        )

    def files(self, full_repo: str, number: int) -> list[dict]:
        return self._get(f"/repos/{full_repo}/pulls/{number}/files?per_page=100")


def _subject(repo: str, number: int) -> str:
    """Stable per pull request; what the dedup and the round bound key on."""
    return f"pr:{repo}#{number}"


def _change_requests(
    reviews: list[dict], comments: list[dict], *, bot_login: str
) -> list[ChangeRequest]:
    """What humans asked for, oldest first.

    Only ``CHANGES_REQUESTED`` verdicts and inline review comments count. Plain
    conversation comments stay out: the revision agent itself replies there
    (`gh pr comment`), and treating chat as a change request would put the loop
    in conversation with itself.
    """
    items: list[ChangeRequest] = []
    for review in reviews:
        author = (review.get("user") or {}).get("login") or ""
        if author == bot_login or review.get("state") != "CHANGES_REQUESTED":
            continue
        items.append(
            ChangeRequest(
                kind="review",
                author=author,
                body=review.get("body") or "(no summary text)",
                at=review.get("submitted_at") or "",
                url=review.get("html_url") or "",
            )
        )
    for comment in comments:
        author = (comment.get("user") or {}).get("login") or ""
        if author == bot_login:
            continue
        items.append(
            ChangeRequest(
                kind="comment",
                author=author,
                body=comment.get("body") or "",
                at=comment.get("created_at") or "",
                path=comment.get("path") or "",
                line=comment.get("line"),
                url=comment.get("html_url") or "",
            )
        )
    return sorted(items, key=lambda i: i.at)


def _foreign_commit(commits: list[dict], human_logins: tuple[str, ...]) -> str | None:
    """The login of the first human commit on the branch, if any."""
    for commit in commits:
        for role in ("author", "committer"):
            login = (commit.get(role) or {}).get("login") or ""
            if login in human_logins:
                return login
    return None


def _cursor(conn: psycopg.Connection, subject: str) -> tuple[str, int]:
    """The newest change-request timestamp already handled, and how many
    revision runs this pull request has burned.

    Read from the runs table rather than kept anywhere else, for the same
    reason the queue is a table: the poll has to be able to die and come back
    without noticing.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n, max((SELECT e.payload->>'cursor'"
            "   FROM agent_events e"
            "   WHERE e.run_id = agent_runs.id AND e.type = 'run_queued')) AS cursor"
            " FROM agent_runs WHERE kind = %s AND subject = %s",
            (RUN_KIND, subject),
        )
        row = cur.fetchone()
    return row["cursor"] or "", row["n"]


def agent_branch_run_id(head_ref: str) -> int | None:
    """The run id an ``agent/run-N`` branch name carries, or None.

    Public because the dreaming job resolves its own open pull request the same
    way (#45): a branch name is the only link from a PR back to the run that
    cut it, and two modules parsing it two ways is how they drift apart.
    """
    match = _AGENT_BRANCH.match(head_ref)
    return int(match.group(1)) if match else None


def _origin_run(conn: psycopg.Connection, head_ref: str) -> Run | None:
    """The run whose branch this pull request is, or None.

    The branch name carries the run id, and that run supplies two things: its
    *kind*, which decides whether this loop may touch the PR at all, and its
    ``run_queued`` payload, which already holds everything the revision prompt
    needs to say about the Sentry issue — no second Sentry call, and the chain
    stays traceable in the log.
    """
    run_id = agent_branch_run_id(head_ref)
    if run_id is None:
        return None
    try:
        return get_run(conn, run_id)
    except LookupError:
        return None


def poll(
    conn: psycopg.Connection,
    client: PullsClient,
    *,
    repos: tuple[str, ...] | None = None,
    bot_login: str | None = None,
    human_logins: tuple[str, ...] | None = None,
    max_per_poll: int | None = None,
    dry_run: bool = False,
) -> PollReport:
    """Fetch, filter, and enqueue. Returns what happened.

    Idempotent by construction, like the Sentry poll: the cursor skips comments
    already handed to a run, the open-run check skips in-flight subjects, and
    the partial unique index is the backstop.
    """
    repos = repos if repos is not None else config.GITHUB_EXPECTED_REPOS
    bot_login = bot_login or config.GITHUB_BOT_LOGIN
    human_logins = human_logins or config.GITHUB_HUMAN_LOGINS
    max_per_poll = max_per_poll or config.GITHUB_PR_MAX_PER_POLL
    report = PollReport(window="open PRs")

    for full_repo in repos:
        repo = full_repo.split("/", 1)[-1]
        try:
            pulls = client.open_pulls(full_repo)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            logger.warning("pr poll failed for %s: %s", full_repo, exc)
            report.drop(f"repo {full_repo} unreachable")
            continue

        for pull in pulls:
            report.seen += 1
            number = pull.get("number")
            author = (pull.get("user") or {}).get("login") or ""
            if author != bot_login:
                report.drop("not authored by this orchestrator")
                continue
            if len(report.enqueued) >= max_per_poll:
                report.drop(f"over the {max_per_poll}-per-poll cap")
                continue

            # Which run cut this branch decides whether this loop may touch the
            # PR at all, so resolve it before spending three API calls on
            # reviews, comments and commits.
            head_ref = (pull.get("head") or {}).get("ref") or ""
            origin_run = _origin_run(conn, head_ref)
            if origin_run is None:
                report.drop("head branch is not an agent branch")
                continue
            if origin_run.kind != TRIAGE_KIND:
                # An allowlist, not a denylist: the revision prompt is written
                # for a Sentry fix (address the refutation, keep the failing-
                # then-passing test green). Pointing it at a memory-audit PR —
                # a markdown-only diff with no Sentry issue behind it — asks an
                # agent to revise a fix that does not exist. Every other kind
                # that learns to open a PR is opted out by default rather than
                # needing a new deny entry here.
                report.drop(f"{origin_run.kind} PR; the revision prompt does not fit")
                continue

            subject = _subject(repo, number)
            cursor, rounds = _cursor(conn, subject)
            requests = _change_requests(
                client.reviews(full_repo, number),
                client.review_comments(full_repo, number),
                bot_login=bot_login,
            )
            fresh = [r for r in requests if r.at > cursor]
            if not fresh:
                report.drop("no new change requests")
                continue

            foreign = _foreign_commit(client.commits(full_repo, number), human_logins)
            if foreign:
                # The human has taken the wheel; say so loudly, once per poll,
                # and leave the branch entirely alone.
                logger.warning(
                    "%s has commits by %s; leaving it to the human", subject, foreign
                )
                report.drop("human commits on the branch; not touching it")
                continue

            if rounds >= MAX_REVISION_ROUNDS:
                logger.warning(
                    "%s has burned all %s revision rounds; further change"
                    " requests need a human",
                    subject,
                    MAX_REVISION_ROUNDS,
                )
                report.drop("revision rounds exhausted; needs a human")
                continue

            if queue.has_recent_run(conn, RUN_KIND, subject, cooldown=_NO_COOLDOWN):
                report.drop("a revision is already in flight")
                continue

            origin = origin_run.payload or {}
            payload = {
                **{
                    k: origin[k]
                    for k in (
                        "issue_id",
                        "short_id",
                        "title",
                        "culprit",
                        "project",
                        "permalink",
                    )
                    if k in origin
                },
                "repo": repo,
                "branch": head_ref,
                # The PR's own base, not an assumed `main` (#68): the sandbox
                # fetches it and the chained review diffs against it.
                "base_branch": (pull.get("base") or {}).get("ref") or DEFAULT_BRANCH,
                "pr_url": pull.get("html_url") or "",
                "number": number,
                "change_requests": [r.as_payload() for r in fresh],
                "cursor": max(r.at for r in fresh),
                "revision": rounds + 1,
                # The adversarial chain this revision feeds re-enters at round
                # 1: one refutation gets one more agent pass, then a human.
                "round": 1,
            }
            if not dry_run:
                with conn.transaction():
                    create_run(conn, RUN_KIND, subject, payload)
            report.enqueued.append(subject)
            logger.info(
                "%s %s (%s new change request(s), revision %s/%s)",
                "would enqueue" if dry_run else "enqueued",
                subject,
                len(fresh),
                rounds + 1,
                MAX_REVISION_ROUNDS,
            )

    logger.info("pr poll: saw %s, enqueued %s", report.seen, len(report.enqueued))
    for reason, count in sorted(report.dropped.items(), key=lambda kv: -kv[1]):
        logger.info("  dropped %s: %s", count, reason)
    return report


# --- closing parked runs whose PR was decided (#90) ---------------------------


@dataclass(frozen=True, slots=True)
class Parked:
    """An AWAITING_HUMAN chain run and the pull request its gate names."""

    run_id: int
    kind: str
    subject: str
    pr_url: str


def parse_pr_url(url: str) -> tuple[str, int] | None:
    """``https://github.com/o/r/pull/7`` -> ``("o/r", 7)``; None otherwise."""
    match = _PR_URL.match(url or "")
    return (match.group(1), int(match.group(2))) if match else None


def parked_runs(conn: psycopg.Connection) -> list[Parked]:
    """Chain runs parked AWAITING_HUMAN on a pull request, oldest first.

    The PR is the newest ``human_gate``'s ``pr_url``, falling back to the run's
    own ``run_queued`` payload: the gate is what the human was asked about, and
    a revision or review always carries the PR it worked on in its payload.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT r.id, r.kind, r.subject,"
            " coalesce("
            "   (SELECT e.payload->>'pr_url' FROM agent_events e"
            "     WHERE e.run_id = r.id AND e.type = %s"
            "     ORDER BY e.seq DESC LIMIT 1),"
            "   (SELECT e.payload->>'pr_url' FROM agent_events e"
            "     WHERE e.run_id = r.id AND e.type = %s"
            "     ORDER BY e.seq LIMIT 1)"
            " ) AS pr_url"
            " FROM agent_runs r"
            " WHERE r.status = %s AND r.kind = ANY(%s)"
            " ORDER BY r.id",
            (
                EventType.HUMAN_GATE.value,
                EventType.RUN_QUEUED.value,
                RunStatus.AWAITING_HUMAN.value,
                list(CHAIN_KINDS),
            ),
        )
        return [
            Parked(row["id"], row["kind"], row["subject"], row["pr_url"] or "")
            for row in cur.fetchall()
        ]


def _close(conn: psycopg.Connection, parked: Parked, pull: dict) -> str | None:
    """Close one parked run for a decided PR; the reason, or None if not closed.

    Locks the run and re-reads its status in the same transaction as the
    append, so a run a human or another poll moved since :func:`parked_runs`
    read it is left alone. No ``email_sent`` marker is needed: the park was
    already emailed, and the notifier mails a run once, ever.
    """
    merged = bool(pull.get("merged") or pull.get("merged_at"))
    facts = {
        "pr_url": parked.pr_url,
        "pr_state": pull.get("state"),
        "merged_at": pull.get("merged_at"),
        "closed_at": pull.get("closed_at"),
        "merge_commit_sha": pull.get("merge_commit_sha") if merged else None,
    }
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM agent_runs WHERE id = %s FOR UPDATE",
                (parked.run_id,),
            )
            row = cur.fetchone()
        if row is None or row["status"] != RunStatus.AWAITING_HUMAN.value:
            return None
        if merged:
            log.append(
                conn,
                parked.run_id,
                EventType.RUN_DONE,
                {"reason": REASON_MERGED, **facts},
            )
            return REASON_MERGED
        log.append(
            conn,
            parked.run_id,
            EventType.RUN_ABANDONED,
            # requeued written explicitly: the fold reads a missing key as
            # terminal, but every abandon says so out loud (see log.apply_event).
            {"requeued": False, "reason": REASON_CLOSED, **facts},
        )
        return REASON_CLOSED


def close_decided(
    conn: psycopg.Connection,
    client: PullsClient,
    *,
    repos: tuple[str, ...] | None = None,
    dry_run: bool = False,
) -> PollReport:
    """Close every parked chain run whose pull request a human has decided.

    A review that returns STANDS parks AWAITING_HUMAN for the human to merge,
    and nothing else ever moves it on (#90). Left open, it holds the subject:
    the Sentry poll will not re-triage an issue an open chained run owns, so a
    regression — the expected case after a MITIGATION — would never be seen
    again. A merged PR ends the run ``run_done``; a PR closed unmerged ends it
    ``run_abandoned``; an open PR, or one this poll cannot read, leaves it
    parked until the next poll. The event carries the PR's state and times, so
    the log says why the run closed.
    """
    repos = repos if repos is not None else config.GITHUB_EXPECTED_REPOS
    report = PollReport(window="parked runs")
    for parked in parked_runs(conn):
        report.seen += 1
        target = parse_pr_url(parked.pr_url)
        if target is None:
            report.drop("no pull request on the gate")
            continue
        full_repo, number = target
        if full_repo not in repos:
            report.drop("pull request outside the expected repos")
            continue
        try:
            pull = client.pull(full_repo, number)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            logger.warning(
                "could not read %s for parked run %s: %s",
                parked.pr_url,
                parked.run_id,
                exc,
            )
            report.drop("pull request unreadable; retried next poll")
            continue
        if pull.get("state") != "closed":
            report.drop("pull request still open")
            continue
        merged = bool(pull.get("merged") or pull.get("merged_at"))
        if dry_run:
            reason = REASON_MERGED if merged else REASON_CLOSED
        else:
            reason = _close(conn, parked, pull)
            if reason is None:
                report.drop("no longer parked")
                continue
        report.enqueued.append(f"run {parked.run_id}")
        logger.info(
            "%s parked %s run %s (%s): %s %s",
            "would close" if dry_run else "closed",
            parked.kind,
            parked.run_id,
            parked.subject,
            reason,
            parked.pr_url,
        )
    logger.info(
        "parked-run sweep: saw %s, closed %s", report.seen, len(report.enqueued)
    )
    for reason, count in sorted(report.dropped.items(), key=lambda kv: -kv[1]):
        logger.info("  kept %s: %s", count, reason)
    return report
