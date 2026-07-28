"""The GitHub change-request work source. Issue #10.

Polls the open pull requests this orchestrator's App authored and enqueues a
``pr_revision`` run when a human has asked for changes: a new
``CHANGES_REQUESTED`` review, or new inline review comments, since the last
revision run for that pull request.

Boundaries, stated up front because each one is a guard against a real failure:

* Only PRs authored by the App's bot login are considered. A human's PR is
  never touched, however reviewable it looks.
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

from orchestrator import config, queue
from orchestrator.log import create_run
from orchestrator.queue import get_run
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


def _origin_payload(conn: psycopg.Connection, head_ref: str) -> dict | None:
    """The issue facts from the run whose branch this PR is, or None.

    The branch name carries the run id, and that run's ``run_queued`` payload
    already holds everything the revision prompt needs to say about the Sentry
    issue — no second Sentry call, and the chain stays traceable in the log.
    """
    match = _AGENT_BRANCH.match(head_ref)
    if match is None:
        return None
    try:
        origin = get_run(conn, int(match.group(1)))
    except LookupError:
        return None
    return origin.payload or {}


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

            origin = _origin_payload(conn, (pull.get("head") or {}).get("ref") or "")
            if origin is None:
                report.drop("head branch is not an agent branch")
                continue

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
                "branch": (pull.get("head") or {}).get("ref") or "",
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
