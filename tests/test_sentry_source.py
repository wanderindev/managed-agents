"""The Sentry work source.

The filter cases are drawn from the 25 unresolved issues actually in
`javier-feliu` on 2026-07-25, so a regression here means the source has started
either burning tokens on in-app-browser noise or ignoring a real bug.
"""

import json
import urllib.error
from datetime import timedelta

import pytest

from orchestrator.enums import EventType, RunStatus
from orchestrator.log import append, create_run, load_events
from orchestrator.queue import get_run, has_recent_run, queued_count
from orchestrator.sources import sentry
from orchestrator.sources.sentry import (
    Filters,
    SentryClient,
    SentryIssue,
    classify,
    poll,
)


def issue(**overrides):
    base = {
        "id": "5001",
        "shortId": "PIC-PYTHON-FASTAPI-1Q",
        "title": 'ProgrammingError: relation "instagram_posts" does not exist',
        "culprit": "app.services.instagram_scheduler in _sweep_stale_claims",
        "project": {"slug": "pic-python-fastapi"},
        "count": 8,
        "userCount": 0,
        "firstSeen": "2026-07-17T00:00:00Z",
        "lastSeen": "2026-07-17T00:00:00Z",
        "permalink": "https://javier-feliu.sentry.io/issues/PIC-PYTHON-FASTAPI-1Q",
    }
    base.update(overrides)
    return base


class FakeSentry(SentryClient):
    def __init__(self, by_project=None, fail=()):
        super().__init__("token", "javier-feliu")
        self.by_project = by_project or {}
        self.fail = set(fail)
        self.asked = []

    def unresolved_issues(self, project, limit=25):
        self.asked.append(project)
        if project in self.fail:
            raise urllib.error.URLError("connection reset")
        return [sentry._to_issue(raw) for raw in self.by_project.get(project, [])]


# --- the subject key ---------------------------------------------------------


def test_the_subject_is_the_human_readable_short_id(conn):
    """A subject you can grep for in Sentry and in the email #9 sends."""
    parsed = sentry._to_issue(issue())
    assert parsed.subject == "sentry:PIC-PYTHON-FASTAPI-1Q"


def test_the_subject_falls_back_to_the_numeric_id(conn):
    parsed = sentry._to_issue(issue(shortId=None))
    assert parsed.subject == "sentry:5001"


# --- classification ----------------------------------------------------------


def test_a_real_backend_bug_is_kept():
    assert classify(sentry._to_issue(issue()), Filters()) is None


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        # Every one of these is a live issue in the org today.
        (
            {
                "title": "Error: Error invoking postMessage: Java object is gone",
                "culprit": "sendDataToNative(iabjs://navigation_performance_logger_android)",
                "project": {"slug": "pic-javascript-react"},
            },
            "in-app browser",
        ),
        (
            {
                "title": "TypeError: undefined is not an object (evaluating 'window.webkit.messageHandlers')",
                "project": {"slug": "pic-javascript-react"},
            },
            "in-app browser",
        ),
        (
            {
                "title": "TypeError: Failed to fetch dynamically imported module: https://feliu.dev/assets/Projects-CB.js",
                "project": {"slug": "trd-javascript-react"},
            },
            "stale chunk",
        ),
        (
            {
                "title": "Error: Request failed: 502",
                "project": {"slug": "trd-javascript-react"},
            },
            "restarting mid-deploy",
        ),
        (
            {
                "title": "OperationalError: FATAL:  remaining connection slots are reserved for roles with the SUPERUSER attribute",
            },
            "connection ceiling",
        ),
        (
            {"title": "Error: [object Object]", "culprit": "GET /.env"},
            "bot probing",
        ),
        (
            {
                "title": "TypeError: Load failed",
                "culprit": "https://panamaincontext.com/excursiones-academicas",
                "project": {"slug": "pic-javascript-react"},
            },
            "browser-side network failure",
        ),
    ],
)
def test_known_noise_is_dropped_with_a_reason(overrides, expected):
    reason = classify(sentry._to_issue(issue(**overrides)), Filters())
    assert reason is not None
    assert expected in reason


def test_a_project_without_a_repo_here_is_dropped():
    """atelier-loyalty-app and pic-cert-watcher are real, just not ours to fix."""
    reason = classify(
        sentry._to_issue(issue(project={"slug": "pic-cert-watcher"})), Filters()
    )
    assert "no repo here" in reason


def test_a_single_occurrence_is_kept_by_default():
    """These are low-traffic personal sites; one occurrence is already signal.

    PIC-PYTHON-FASTAPI-1H is the case that settled this: a genuine
    missing-column error that had happened exactly once. A floor of 2 threw it
    away along with 9 others.
    """
    assert classify(sentry._to_issue(issue(count=1)), Filters()) is None


def test_the_floor_is_still_configurable_upward():
    reason = classify(sentry._to_issue(issue(count=1)), Filters(min_events=2))
    assert "below the 2 floor" in reason


def test_an_issue_with_no_title_and_no_culprit_is_dropped():
    """PIC-JAVASCRIPT-REACT-Q. Sentry could not group it into anything.

    Matters much more at min_events=1: without this, every ungroupable one-off
    becomes a run that can only conclude there was nothing to work from.
    """
    reason = classify(
        sentry._to_issue(issue(title="<unknown>", culprit="", count=1)), Filters()
    )
    assert "nothing for an agent to start from" in reason


def test_a_missing_title_is_survivable_when_there_is_a_culprit():
    """A stack location is a starting point even without a good title."""
    parsed = sentry._to_issue(
        issue(title="<unknown>", culprit="app.services.foo in bar", count=1)
    )
    assert classify(parsed, Filters()) is None


# --- polling -----------------------------------------------------------------


def test_a_poll_enqueues_a_real_bug(conn):
    client = FakeSentry({"pic-python-fastapi": [issue()]})

    report = poll(conn, client, projects=["pic-python-fastapi"])

    assert report.enqueued == ["sentry:PIC-PYTHON-FASTAPI-1Q"]
    assert queued_count(conn) == 1


def test_the_payload_is_enough_to_start_without_calling_sentry_again(conn):
    client = FakeSentry({"pic-python-fastapi": [issue()]})
    poll(conn, client, projects=["pic-python-fastapi"])

    run_id = conn.execute(
        "SELECT id FROM agent_runs WHERE subject = %s",
        ("sentry:PIC-PYTHON-FASTAPI-1Q",),
    ).fetchone()["id"]
    payload = load_events(conn, run_id)[0].payload

    assert payload["repo"] == "panama-in-context"
    assert payload["permalink"].startswith("https://")
    assert payload["events"] == 8
    assert payload["culprit"]


def test_a_second_poll_with_no_new_activity_enqueues_nothing(conn):
    """The acceptance criterion for #6."""
    client = FakeSentry({"pic-python-fastapi": [issue()]})

    first = poll(conn, client, projects=["pic-python-fastapi"])
    second = poll(conn, client, projects=["pic-python-fastapi"])

    assert len(first.enqueued) == 1
    assert second.enqueued == []
    assert queued_count(conn) == 1


def test_a_finished_run_stays_suppressed_through_its_cooldown(conn):
    client = FakeSentry({"pic-python-fastapi": [issue()]})
    poll(conn, client, projects=["pic-python-fastapi"])
    run_id = conn.execute(
        "SELECT id FROM agent_runs WHERE subject = %s",
        ("sentry:PIC-PYTHON-FASTAPI-1Q",),
    ).fetchone()["id"]
    append(conn, run_id, EventType.RUN_DONE)

    report = poll(conn, client, projects=["pic-python-fastapi"])

    assert report.enqueued == []
    assert "already queued or recently handled" in report.dropped


def test_the_same_issue_can_come_back_after_the_cooldown(conn):
    client = FakeSentry({"pic-python-fastapi": [issue()]})
    poll(conn, client, projects=["pic-python-fastapi"])
    run_id = conn.execute(
        "SELECT id FROM agent_runs WHERE subject = %s",
        ("sentry:PIC-PYTHON-FASTAPI-1Q",),
    ).fetchone()["id"]
    append(conn, run_id, EventType.RUN_DONE)
    conn.execute(
        "UPDATE agent_runs SET updated_at = now() - interval '30 days' WHERE id = %s",
        (run_id,),
    )

    report = poll(conn, client, projects=["pic-python-fastapi"])

    assert report.enqueued == ["sentry:PIC-PYTHON-FASTAPI-1Q"]


def test_a_storm_cannot_spawn_a_fleet(conn):
    """The cap is the thing standing between an incident and 40 containers."""
    storm = [
        issue(id=str(n), shortId=f"PIC-PYTHON-FASTAPI-{n}", count=50) for n in range(20)
    ]
    client = FakeSentry({"pic-python-fastapi": storm})

    report = poll(
        conn, client, projects=["pic-python-fastapi"], filters=Filters(max_per_poll=3)
    )

    assert len(report.enqueued) == 3
    assert queued_count(conn) == 3
    # And it says so, rather than looking like there were only three issues.
    assert any("per-poll cap" in reason for reason in report.dropped)


def test_one_unreachable_project_does_not_lose_the_others(conn):
    client = FakeSentry(
        {"pic-python-fastapi": [issue()], "trd-python": []},
        fail=["trd-python"],
    )

    report = poll(conn, client, projects=["trd-python", "pic-python-fastapi"])

    assert report.enqueued == ["sentry:PIC-PYTHON-FASTAPI-1Q"]
    assert any("unreachable" in reason for reason in report.dropped)


def test_every_drop_is_counted_by_reason(conn):
    """A source that filters silently reads as "nothing was wrong"."""
    client = FakeSentry(
        {
            "pic-javascript-react": [
                issue(
                    shortId="PIC-JAVASCRIPT-REACT-K",
                    title="Error invoking postMessage: Java object is gone",
                    project={"slug": "pic-javascript-react"},
                ),
                issue(
                    shortId="PIC-JAVASCRIPT-REACT-X",
                    title="TypeError: Failed to fetch dynamically imported module",
                    project={"slug": "pic-javascript-react"},
                ),
            ]
        }
    )

    report = poll(conn, client, projects=["pic-javascript-react"])

    assert report.seen == 2
    assert report.enqueued == []
    assert sum(report.dropped.values()) == 2
    assert len(report.dropped) == 2, "two different reasons, not one bucket"


def test_polling_defaults_to_every_mapped_project(conn):
    client = FakeSentry({})
    poll(conn, client)
    assert set(client.asked) == set(sentry.PROJECT_REPOS)


# --- the HTTP client ---------------------------------------------------------


def test_the_client_asks_for_unresolved_issues_with_a_bearer_token():
    captured = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return json.dumps([issue()]).encode()

    def opener(request, timeout=None):
        captured["url"] = request.full_url
        captured["headers"] = request.headers
        return FakeResponse()

    client = SentryClient("secret-token", "javier-feliu", opener=opener)
    issues = client.unresolved_issues("pic-python-fastapi")

    assert "is%3Aunresolved" in captured["url"]
    assert "project=pic-python-fastapi" in captured["url"]
    assert "us.sentry.io" in captured["url"], "the org is in the US region"
    assert captured["headers"]["Authorization"] == "Bearer secret-token"
    assert isinstance(issues[0], SentryIssue)


# --- has_recent_run ----------------------------------------------------------


def test_has_recent_run_sees_an_in_flight_run(conn):
    create_run(conn, "sentry_triage", "sentry:X-1")
    assert has_recent_run(conn, "sentry_triage", "sentry:X-1", timedelta(days=7))


def test_has_recent_run_is_false_for_an_unknown_subject(conn):
    assert not has_recent_run(conn, "sentry_triage", "sentry:NOPE", timedelta(days=7))


def test_has_recent_run_ignores_a_different_kind(conn):
    create_run(conn, "pr_revision", "sentry:X-2")
    assert not has_recent_run(conn, "sentry_triage", "sentry:X-2", timedelta(days=7))


def test_an_abandoned_run_stops_suppressing_once_it_is_old(conn):
    """Giving up on an issue must not blacklist it forever."""
    run_id = create_run(conn, "sentry_triage", "sentry:X-3")
    append(conn, run_id, EventType.RUN_ABANDONED, {"requeued": False})
    assert get_run(conn, run_id).status is RunStatus.ABANDONED

    conn.execute(
        "UPDATE agent_runs SET updated_at = now() - interval '30 days' WHERE id = %s",
        (run_id,),
    )
    assert not has_recent_run(conn, "sentry_triage", "sentry:X-3", timedelta(days=7))


def test_a_dry_run_reports_without_enqueuing(conn):
    """How the filters get tuned: see the effect of a change before living with it."""
    client = FakeSentry({"pic-python-fastapi": [issue()]})

    report = poll(conn, client, projects=["pic-python-fastapi"], dry_run=True)

    assert report.enqueued == ["sentry:PIC-PYTHON-FASTAPI-1Q"]
    assert queued_count(conn) == 0, "nothing was actually queued"
