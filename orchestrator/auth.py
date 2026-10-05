"""Detecting a dead Claude login, so it pauses the queue rather than burning a run a day.

The sandbox mounts the host's ``~/.claude/.credentials.json`` and Claude Code
refreshes the OAuth token itself. When that refresh fails for good (it did on
2026-08-22: the file was rewritten with ``expiresAt: 0``), every run afterwards
dies inside a second with ``authentication_failed`` — which the loop used to
report as a generic "sandbox exited nonzero", once per scheduled run, for days.

Three pieces, all stateless:

* :func:`failed_auth` reads a transcript and says whether it died on auth.
* :func:`fingerprint` names the credential file's current on-disk version
  (mtime + size). The loop stores it on the ``run_failed`` event, and refuses to
  lease anything while the file still matches — the operator fixing it is the
  only thing that changes the fingerprint (``claude auth login`` rewrites it).
* :func:`paused` puts the two together against the log: the one pause
  condition the loop, the poll, the dream schedule and the notifier all share
  (#59), so they can never disagree about whether the login is dead.
"""

import logging
from pathlib import Path
from typing import Any

import psycopg

from orchestrator import config, log, queue

logger = logging.getLogger(__name__)

#: What the stream carries on the assistant/result events of a login failure.
_AUTH_ERROR = "authentication_failed"
_AUTH_TEXT = "Failed to authenticate"

#: ``run_failed`` payload key, and its value, for a run that died on auth.
REASON_KEY = "reason"
REASON_AUTH = "auth"
FINGERPRINT_KEY = "credentials"


def failed_auth(events: list[dict[str, Any]]) -> bool:
    """True when the transcript shows the CLI could not authenticate at all."""
    for event in events:
        if event.get("error") == _AUTH_ERROR:
            return True
        if (
            event.get("type") == "result"
            and event.get("is_error")
            and str(event.get("result", "")).startswith(_AUTH_TEXT)
        ):
            return True
    return False


def fingerprint(path: Path) -> str:
    """The credential file's current version, or ``"missing"``.

    mtime and size, not a content hash: the token is a secret and its hash has
    no business in the event log. A re-login rewrites the file, which is the
    only change this needs to notice.
    """
    try:
        stat = path.stat()
    except FileNotFoundError:
        return "missing"
    return f"{stat.st_mtime_ns}:{stat.st_size}"


def paused(conn: psycopg.Connection, credentials: Path) -> str | None:
    """The pinned fingerprint while dispatch is paused for a dead login, else None.

    Paused means: the newest ``run_failed`` that named a dead login recorded a
    credential fingerprint, and the file on disk still has it. A re-login
    rewrites the file, the fingerprint moves, and this goes back to None with
    nothing to reset.
    """
    stamped = log.latest_auth_failure(conn)
    if stamped is None or stamped != fingerprint(Path(credentials)):
        return None
    return stamped


def skip_enqueue(
    conn: psycopg.Connection, what: str, credentials: str | Path | None = None
) -> bool:
    """True (and says why) when a work source must not enqueue right now.

    While dispatch is paused nothing runs, so every scheduled enqueue only
    lengthens the backlog that dispatches all at once on re-login — eleven
    days of dreams, after 2026-09-24. The sources ask the same question the
    loop does and stand down; Sentry issues and PR reviews are still there
    to be found by the first poll after the login is fixed.
    """
    path = Path(credentials or config.CLAUDE_CREDENTIALS)
    if paused(conn, path) is None:
        return False
    logger.warning(
        "not enqueuing %s: dispatch is paused because the Claude login expired"
        " (%s unchanged since the failed run); %s run(s) already queued."
        " Fix: `claude auth login` on the host",
        what,
        path,
        queue.queued_count(conn),
    )
    return True
