"""The Sentry work source. Issue #6.

Polls the two projects' Sentry for unresolved issues and enqueues a
``sentry_triage`` run for each one worth an agent's attention.

The filters here are not guesses. They were written against the 25 unresolved
issues actually sitting in ``javier-feliu`` on 2026-07-25, and every pattern
below corresponds to something real in that list. Start narrow and widen once the
loop has a track record: a filter that is too tight wastes an issue, one that is
too loose wastes tokens and buries the signal.

Read-only against Sentry. Resolving an issue is a downstream effect of merging a
PR, never something this does.
"""

import json
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import timedelta

import psycopg

from orchestrator import queue
from orchestrator.log import create_run

logger = logging.getLogger(__name__)

RUN_KIND = "sentry_triage"

#: Sentry project -> the repo a fix would land in.
#:
#: `atelier-loyalty-app` and `pic-cert-watcher` are deliberately absent. They are
#: real projects in the same org but they are not repos this orchestrator has, so
#: an agent could only ever report back that it cannot help.
PROJECT_REPOS = {
    "trd-python": "feliu-dev",
    "trd-javascript-react": "feliu-dev",
    "pic-python-fastapi": "panama-in-context",
    "pic-javascript-react": "panama-in-context",
}

#: (pattern, why) matched case-insensitively against title and culprit.
#: Every entry earns its place from a real issue in the org today.
IGNORE_PATTERNS: tuple[tuple[re.Pattern, str], ...] = (
    (
        re.compile(r"iabjs://", re.IGNORECASE),
        "Android in-app browser injecting its own JS, not our code",
    ),
    (
        re.compile(r"window\.webkit\.messageHandlers", re.IGNORECASE),
        "iOS in-app browser injecting its own JS, not our code",
    ),
    (
        re.compile(r"Java object is gone", re.IGNORECASE),
        "in-app browser tearing down its bridge, not our code",
    ),
    (
        re.compile(r"Failed to fetch dynamically imported module", re.IGNORECASE),
        "stale chunk: an open tab requesting a hash that a deploy replaced",
    ),
    (
        re.compile(r"Request failed: 502", re.IGNORECASE),
        "backend restarting mid-deploy; watchtower pulls take a moment",
    ),
    (
        re.compile(r"remaining connection slots are reserved", re.IGNORECASE),
        "managed Postgres connection ceiling, an infrastructure ceiling not a bug",
    ),
    (
        re.compile(r"connection to server at .* Connection refused", re.IGNORECASE),
        "database briefly unreachable, infrastructure not code",
    ),
    (
        re.compile(r"GET /\.env", re.IGNORECASE),
        "bot probing for secrets; there is nothing to fix",
    ),
    (
        re.compile(
            r"TypeError: Load failed|NetworkError when attempting to fetch",
            re.IGNORECASE,
        ),
        "browser-side network failure, usually a visitor losing connectivity",
    ),
)

#: Titles Sentry emits when it could not group the error into anything.
UNUSABLE_TITLES = frozenset({"", "<unknown>", "unknown"})


@dataclass(frozen=True, slots=True)
class SentryIssue:
    id: str
    short_id: str
    title: str
    culprit: str
    project: str
    count: int
    user_count: int
    first_seen: str
    last_seen: str
    permalink: str

    @property
    def subject(self) -> str:
        """Stable across polls and across the issue's whole life.

        `shortId` rather than the numeric id because it is what a human sees in
        Sentry and in the email #9 sends, so a subject is greppable by a person.
        """
        return f"sentry:{self.short_id}"


@dataclass(frozen=True, slots=True)
class Filters:
    #: These are low-traffic personal sites, not a service with real volume, so
    #: a single occurrence is already signal rather than noise. Raising this
    #: would trade away real bugs: on the day this was written, a floor of 2
    #: dropped 10 of 25 issues, and among them a genuine missing-column error
    #: (PIC-PYTHON-FASTAPI-1H) that had happened exactly once.
    min_events: int = 1
    #: A Sentry storm during an incident must not be able to spawn a fleet.
    max_per_poll: int = 5
    #: How long a finished run suppresses its subject. Long enough that a merged
    #: fix has been deployed and had a chance to stop the errors.
    cooldown_days: int = 7
    ignore_patterns: tuple[tuple[re.Pattern, str], ...] = IGNORE_PATTERNS


@dataclass
class PollReport:
    """What a poll did, and what it chose not to do.

    Drops are counted by reason rather than discarded. A source that silently
    filters reads as "nothing was wrong" when it actually means "I decided none
    of this was worth your time", and those are very different claims.
    """

    seen: int = 0
    #: The lookback the poll used. Reported because an issue older than this is
    #: invisible to it, and a silent scope limit reads as "there was nothing".
    window: str = ""
    enqueued: list[str] = field(default_factory=list)
    dropped: dict[str, int] = field(default_factory=dict)

    def drop(self, reason: str) -> None:
        self.dropped[reason] = self.dropped.get(reason, 0) + 1


class SentryClient:
    """Minimal read-only Sentry API client.

    stdlib urllib rather than requests: one fewer dependency in a process whose
    job is to be boring, and this makes exactly one kind of call.
    """

    def __init__(
        self,
        token: str,
        org: str,
        *,
        base_url: str = "https://us.sentry.io",
        opener=urllib.request.urlopen,
    ) -> None:
        self.token = token
        self.org = org
        self.base_url = base_url.rstrip("/")
        self._open = opener

    def _get(self, url: str) -> dict | list:
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
            },
        )
        with self._open(request, timeout=30) as response:
            return json.loads(response.read().decode())

    def unresolved_issues(
        self, project: str, limit: int = 25, stats_period: str = "14d"
    ) -> list[SentryIssue]:
        """Unresolved issues *active within ``stats_period``*.

        That window is a real scope limit, not a detail: an issue whose last
        event is older than it simply does not come back, however real it is. At
        14d the live poll returned 16 of the 25 unresolved issues in the org, and
        the 9 it left out included a genuine missing-column error last seen 23
        days ago. Widen it with ORCHESTRATOR_SENTRY_STATS_PERIOD to reach back
        further; the cap and the dedup both still hold.
        """
        params = urllib.parse.urlencode(
            {
                "query": "is:unresolved",
                "project": project,
                "statsPeriod": stats_period,
                "limit": limit,
            }
        )
        payload = self._get(
            f"{self.base_url}/api/0/organizations/{self.org}/issues/?{params}"
        )
        return [_to_issue(raw) for raw in payload]

    def latest_event(self, issue_id: str) -> dict:
        """The issue's most recent event: stack trace, breadcrumbs, tags.

        Fetched by the orchestrator at spec-build time (#7) and embedded in the
        prompt, so the Sentry token never has to enter a sandbox and the prompt
        stays a durable, self-contained record of what the agent was told.
        """
        return self._get(
            f"{self.base_url}/api/0/organizations/{self.org}"
            f"/issues/{issue_id}/events/latest/"
        )


def _to_issue(raw: dict) -> SentryIssue:
    return SentryIssue(
        id=str(raw.get("id", "")),
        short_id=raw.get("shortId") or str(raw.get("id", "")),
        title=raw.get("title") or "",
        culprit=raw.get("culprit") or "",
        project=(raw.get("project") or {}).get("slug", ""),
        count=int(raw.get("count", 0) or 0),
        user_count=int(raw.get("userCount", 0) or 0),
        first_seen=raw.get("firstSeen") or "",
        last_seen=raw.get("lastSeen") or "",
        permalink=raw.get("permalink") or "",
    )


def classify(issue: SentryIssue, filters: Filters) -> str | None:
    """Return why an issue should be skipped, or None to enqueue it."""
    if issue.project not in PROJECT_REPOS:
        return f"project {issue.project!r} has no repo here"
    if issue.count < filters.min_events:
        return f"only {issue.count} event(s), below the {filters.min_events} floor"
    if issue.title.strip().lower() in UNUSABLE_TITLES and not issue.culprit.strip():
        # Sentry could not group this into anything. With no title and no
        # culprit there is no starting point, so an agent would burn a run to
        # conclude the same thing. Matters much more at min_events=1.
        return "no title or culprit; nothing for an agent to start from"
    haystack = f"{issue.title}\n{issue.culprit}"
    for pattern, reason in filters.ignore_patterns:
        if pattern.search(haystack):
            return reason
    return None


def poll(
    conn: psycopg.Connection,
    client: SentryClient,
    *,
    projects: list[str] | None = None,
    filters: Filters | None = None,
    dry_run: bool = False,
    stats_period: str = "14d",
) -> PollReport:
    """Fetch, filter, and enqueue. Returns what happened.

    Idempotent by construction: an issue already queued or recently finished is
    skipped, and the partial unique index on ``(kind, subject)`` is the backstop
    if this bookkeeping is ever wrong.

    ``dry_run`` does everything except enqueue. That is how the filters get tuned:
    the drop tally tells you what a change would have kept or thrown away, without
    committing an agent to any of it.
    """
    filters = filters or Filters()
    projects = projects if projects is not None else list(PROJECT_REPOS)
    report = PollReport(window=stats_period)
    cooldown = timedelta(days=filters.cooldown_days)

    for project in projects:
        try:
            issues = client.unresolved_issues(project, stats_period=stats_period)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            # One unreachable project must not lose the others. The next poll
            # retries; there is no state to corrupt.
            logger.warning("sentry poll failed for %s: %s", project, exc)
            report.drop(f"project {project} unreachable")
            continue

        for issue in issues:
            report.seen += 1

            if len(report.enqueued) >= filters.max_per_poll:
                report.drop(f"over the {filters.max_per_poll}-per-poll cap")
                continue

            reason = classify(issue, filters)
            if reason:
                report.drop(reason)
                continue

            if queue.has_recent_run(conn, RUN_KIND, issue.subject, cooldown):
                report.drop("already queued or recently handled")
                continue

            if not dry_run:
                with conn.transaction():
                    create_run(conn, RUN_KIND, issue.subject, _payload(issue))
            report.enqueued.append(issue.subject)
            logger.info(
                "%s %s (%s events)",
                "would enqueue" if dry_run else "enqueued",
                issue.subject,
                issue.count,
            )

    _report(report)
    return report


def _payload(issue: SentryIssue) -> dict:
    """Everything #7 needs to start without a second call to Sentry."""
    return {
        "issue_id": issue.id,
        "short_id": issue.short_id,
        "title": issue.title,
        "culprit": issue.culprit,
        "project": issue.project,
        "repo": PROJECT_REPOS[issue.project],
        "events": issue.count,
        "users": issue.user_count,
        "first_seen": issue.first_seen,
        "last_seen": issue.last_seen,
        "permalink": issue.permalink,
    }


#: Caps for format_event_detail. The point is the app frames and the last few
#: breadcrumbs; a full event can run to hundreds of KB of minified-JS frames
#: that would bury the signal and bloat the prompt.
_MAX_FRAMES = 12
_MAX_BREADCRUMBS = 15


def format_event_detail(event: dict) -> str:
    """Compress a Sentry event into the prompt-sized facts an agent starts from.

    Defensive on every access: event payloads vary wildly by SDK and platform,
    and a malformed one must degrade to "(no event detail available)" rather
    than fail the launch.
    """
    sections: list[str] = []
    for entry in event.get("entries") or []:
        kind = entry.get("type")
        data = entry.get("data") or {}
        if kind == "exception":
            for value in data.get("values") or []:
                sections.append(_format_exception(value))
        elif kind == "message":
            formatted = data.get("formatted")
            if formatted:
                sections.append(f"## Message\n  {str(formatted)[:500]}")
        elif kind == "breadcrumbs":
            crumbs = _format_breadcrumbs(data.get("values") or [])
            if crumbs:
                sections.append(crumbs)
        elif kind == "request" and data.get("url"):
            method = data.get("method") or ""
            sections.append(f"## Request\n  {method} {data['url']}".rstrip())
    tags = event.get("tags") or []
    if tags:
        lines = [f"  {t.get('key')}: {t.get('value')}" for t in tags]
        sections.append("## Tags\n" + "\n".join(lines))
    return "\n\n".join(s for s in sections if s) or "(no event detail available)"


def _format_exception(value: dict) -> str:
    lines = [f"{value.get('type') or 'Error'}: {value.get('value') or ''}".rstrip()]
    frames = (value.get("stacktrace") or {}).get("frames") or []
    # In-app frames are the ones an agent can act on; vendored/minified frames
    # only matter when there is nothing else.
    app_frames = [f for f in frames if f.get("inApp")] or frames
    for frame in app_frames[-_MAX_FRAMES:]:
        lines.append(
            f"  {frame.get('filename')}:{frame.get('lineNo')}"
            f" in {frame.get('function')}"
        )
        for pair in frame.get("context") or []:
            if (
                isinstance(pair, (list, tuple))
                and len(pair) == 2
                and pair[0] == frame.get("lineNo")
            ):
                lines.append(f"    > {str(pair[1]).strip()}")
    return "## Exception (innermost frames last)\n" + "\n".join(lines)


def _format_breadcrumbs(crumbs: list[dict]) -> str:
    lines = []
    for crumb in crumbs[-_MAX_BREADCRUMBS:]:
        message = crumb.get("message") or crumb.get("data") or ""
        lines.append(
            f"  [{crumb.get('level') or 'info'}]"
            f" {crumb.get('category') or ''}: {str(message)[:200]}"
        )
    if not lines:
        return ""
    return "## Breadcrumbs (most recent last)\n" + "\n".join(lines)


def _report(report: PollReport) -> None:
    logger.info("sentry poll: saw %s, enqueued %s", report.seen, len(report.enqueued))
    for reason, count in sorted(report.dropped.items(), key=lambda kv: -kv[1]):
        # Every drop is stated. A silent filter reads as "nothing was wrong".
        logger.info("  dropped %s: %s", count, reason)
