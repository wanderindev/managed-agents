"""Detecting a dead Claude login, so it is reported once rather than burned once a day.

The sandbox mounts the host's ``~/.claude/.credentials.json`` and Claude Code
refreshes the OAuth token itself. When that refresh fails for good (it did on
2026-08-22: the file was rewritten with ``expiresAt: 0``), every run afterwards
dies inside a second with ``authentication_failed`` — which the loop used to
report as a generic "sandbox exited nonzero", once per scheduled run, for days.

Two pieces, both stateless:

* :func:`failed_auth` reads a transcript and says whether it died on auth.
* :func:`fingerprint` names the credential file's current on-disk version
  (mtime + size). The loop stores it on the ``run_failed`` event, and refuses to
  lease anything while the file still matches — the operator fixing it is the
  only thing that changes the fingerprint (``claude auth login`` rewrites it).
"""

from pathlib import Path
from typing import Any

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
