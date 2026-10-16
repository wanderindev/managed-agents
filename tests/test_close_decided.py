"""Closing parked chain runs whose pull request a human decided (#90).

A STANDS review parks AWAITING_HUMAN for the human to merge, and before this
nothing ever moved it on, so the Sentry issue behind it could never be
re-triaged. The cases that matter: merged, closed unmerged, still open, and
never touching a run that is not ours to close.
"""

import urllib.error

from orchestrator import jobs, report
from orchestrator.enums import EventType, RunStatus
from orchestrator.log import append, create_run, load_events, verify_replay
from orchestrator.queue import get_run
from orchestrator.sources import github_prs, sentry
from orchestrator.sources.sentry import Filters, SentryClient

REPO = "wanderindev/panama-in-context"
SUBJECT = "sentry:PIC-PYTHON-FASTAPI-1W"


def url(number, repo=REPO):
    return f"https://github.com/{repo}/pull/{number}"


def merged(number):
    return {
        "number": number,
        "state": "closed",
        "merged": True,
        "merged_at": "2026-10-13T12:00:00Z",
        "closed_at": "2026-10-13T12:00:00Z",
        "merge_commit_sha": "abc123",
    }


def closed(number):
    return {
        "number": number,
        "state": "closed",
        "merged": False,
        "merged_at": None,
        "closed_at": "2026-10-13T12:00:00Z",
        "merge_commit_sha": "def456",
    }


def still_open(number):
    return {"number": number, "state": "open", "merged": False, "merged_at": None}


class FakePulls:
    def __init__(self, pulls=None, fail=()):
        self.pulls = pulls or {}
        self.fail = set(fail)
        self.asked = []

    def pull(self, full_repo, number):
        self.asked.append((full_repo, number))
        if number in self.fail:
            raise urllib.error.URLError("connection reset")
        return self.pulls[number]


def parked_review(conn, number, *, subject=SUBJECT, kind=jobs.REVIEW_KIND, gate=True):
    run_id = create_run(conn, kind, subject, {"pr_url": url(number), "round": 1})
    payload = {"why": "adversarial review passed; PR marked ready"}
    if gate:
        payload["pr_url"] = url(number)
    append(conn, run_id, EventType.HUMAN_GATE, payload)
    return run_id


def sweep(conn, client, **kwargs):
    kwargs.setdefault("repos", (REPO,))
    return github_prs.close_decided(conn, client, **kwargs)


def test_the_chain_kinds_match_the_jobs_that_park_on_a_pr():
    assert set(github_prs.CHAIN_KINDS) == {
        sentry.RUN_KIND,
        jobs.REVIEW_KIND,
        jobs.REVISION_KIND,
        jobs.PR_REVISION_KIND,
    }


def test_a_merged_pr_closes_its_review_done_and_says_why(conn):
    run_id = parked_review(conn, 472)

    result = sweep(conn, FakePulls({472: merged(472)}))

    assert result.enqueued == [f"run {run_id}"]
    assert get_run(conn, run_id).status is RunStatus.DONE
    assert verify_replay(conn, run_id)
    last = load_events(conn, run_id)[-1]
    assert last.type is EventType.RUN_DONE
    assert last.payload["reason"] == github_prs.REASON_MERGED
    assert last.payload["pr_url"] == url(472)
    assert last.payload["merged_at"] == "2026-10-13T12:00:00Z"
    assert last.payload["merge_commit_sha"] == "abc123"


def test_a_pr_closed_unmerged_abandons_its_review(conn):
    run_id = parked_review(conn, 16)

    sweep(conn, FakePulls({16: closed(16)}))

    assert get_run(conn, run_id).status is RunStatus.ABANDONED
    assert verify_replay(conn, run_id)
    last = load_events(conn, run_id)[-1]
    assert last.type is EventType.RUN_ABANDONED
    assert last.payload["requeued"] is False
    assert last.payload["reason"] == github_prs.REASON_CLOSED
    assert last.payload["merge_commit_sha"] is None
    assert report.abandon_bucket(last.payload["reason"]) == "pr closed"


def test_an_open_pr_leaves_the_review_parked(conn):
    run_id = parked_review(conn, 533)

    result = sweep(conn, FakePulls({533: still_open(533)}))

    assert result.enqueued == []
    assert result.dropped == {"pull request still open": 1}
    assert get_run(conn, run_id).status is RunStatus.AWAITING_HUMAN


def test_a_dry_run_reports_without_closing(conn):
    run_id = parked_review(conn, 472)

    result = sweep(conn, FakePulls({472: merged(472)}), dry_run=True)

    assert result.enqueued == [f"run {run_id}"]
    assert get_run(conn, run_id).status is RunStatus.AWAITING_HUMAN


def test_the_pr_falls_back_to_the_run_payload_when_the_gate_has_none(conn):
    run_id = parked_review(conn, 561, gate=False)

    sweep(conn, FakePulls({561: merged(561)}))

    assert get_run(conn, run_id).status is RunStatus.DONE


def test_every_chain_kind_is_swept(conn):
    ids = [
        parked_review(conn, n, subject=f"sentry:K-{n}", kind=kind)
        for n, kind in enumerate(github_prs.CHAIN_KINDS, start=1)
    ]

    sweep(conn, FakePulls({n: merged(n) for n in range(1, len(ids) + 1)}))

    assert {get_run(conn, i).status for i in ids} == {RunStatus.DONE}


def test_a_parked_run_outside_the_chain_is_never_touched(conn):
    """A dream's park means "read my findings"; a merge does not answer that."""
    run_id = parked_review(conn, 9, kind=jobs.DREAM_KIND)
    client = FakePulls({9: merged(9)})

    result = sweep(conn, client)

    assert result.seen == 0
    assert client.asked == []
    assert get_run(conn, run_id).status is RunStatus.AWAITING_HUMAN


def test_runs_that_are_not_parked_are_not_read(conn):
    done = create_run(conn, jobs.REVIEW_KIND, "sentry:DONE", {"pr_url": url(1)})
    append(conn, done, EventType.RUN_DONE)
    create_run(conn, jobs.REVIEW_KIND, "sentry:QUEUED", {"pr_url": url(2)})
    client = FakePulls({1: merged(1), 2: merged(2)})

    assert sweep(conn, client).seen == 0
    assert client.asked == []


def test_a_gate_without_a_readable_pr_url_is_kept(conn):
    run_id = create_run(conn, jobs.REVIEW_KIND, SUBJECT, {"round": 1})
    append(conn, run_id, EventType.HUMAN_GATE, {"why": "x", "pr_url": "?"})

    result = sweep(conn, FakePulls())

    assert result.dropped == {"no pull request on the gate": 1}
    assert get_run(conn, run_id).status is RunStatus.AWAITING_HUMAN


def test_a_pr_outside_the_expected_repos_is_not_asked_about(conn):
    run_id = create_run(conn, jobs.REVIEW_KIND, SUBJECT, {"round": 1})
    append(
        conn,
        run_id,
        EventType.HUMAN_GATE,
        {"pr_url": url(3, repo="someone/else")},
    )
    client = FakePulls({3: merged(3)})

    result = sweep(conn, client)

    assert result.dropped == {"pull request outside the expected repos": 1}
    assert client.asked == []


def test_one_unreadable_pr_does_not_stop_the_sweep(conn):
    stuck = parked_review(conn, 1, subject="sentry:A")
    fine = parked_review(conn, 2, subject="sentry:B")

    result = sweep(conn, FakePulls({2: merged(2)}, fail={1}))

    assert result.dropped == {"pull request unreadable; retried next poll": 1}
    assert get_run(conn, stuck).status is RunStatus.AWAITING_HUMAN
    assert get_run(conn, fine).status is RunStatus.DONE


def test_a_run_that_moved_since_it_was_read_is_left_alone(conn, monkeypatch):
    run_id = parked_review(conn, 472)
    parked = github_prs.parked_runs(conn)
    append(conn, run_id, EventType.RUN_DONE, {"reason": "someone else"})
    monkeypatch.setattr(github_prs, "parked_runs", lambda _conn: parked)

    result = sweep(conn, FakePulls({472: closed(472)}))

    assert result.dropped == {"no longer parked": 1}
    assert get_run(conn, run_id).status is RunStatus.DONE
    assert load_events(conn, run_id)[-1].payload == {"reason": "someone else"}


def test_parse_pr_url():
    assert github_prs.parse_pr_url(url(472)) == (REPO, 472)
    assert github_prs.parse_pr_url(url(472) + "/") == (REPO, 472)
    assert github_prs.parse_pr_url(url(472) + "/files") is None
    assert github_prs.parse_pr_url("") is None
    assert github_prs.parse_pr_url(None) is None


def test_the_client_reads_one_pull_request():
    seen = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b'{"state": "closed", "merged": true}'

    def opener(request, timeout):
        seen.append(request.full_url)
        return Response()

    client = github_prs.PullsClient("t", opener=opener)

    assert client.pull(REPO, 472) == {"state": "closed", "merged": True}
    assert seen == [f"https://api.github.com/repos/{REPO}/pulls/472"]


# --- the regression the sweep exists for -------------------------------------


class OneIssue(SentryClient):
    def __init__(self):
        super().__init__("token", "javier-feliu")

    def unresolved_issues(self, project, limit=25, stats_period="14d"):
        return [
            sentry._to_issue(
                {
                    "id": "5001",
                    "shortId": "PIC-PYTHON-FASTAPI-1W",
                    "title": "ValueError: boom",
                    "culprit": "app.services.x in y",
                    "project": {"slug": "pic-python-fastapi"},
                    "count": 8,
                    "userCount": 0,
                    "firstSeen": "2026-10-01T00:00:00Z",
                    "lastSeen": "2026-10-13T00:00:00Z",
                    "permalink": "https://javier-feliu.sentry.io/issues/5001/",
                }
            )
        ]


def triage_poll(conn):
    return sentry.poll(
        conn,
        OneIssue(),
        projects=["pic-python-fastapi"],
        filters=Filters(cooldown_days=7),
    )


def age(conn, run_id, days):
    conn.execute(
        "UPDATE agent_runs SET updated_at = now() - make_interval(days => %s)"
        " WHERE id = %s",
        (days, run_id),
    )


def test_a_regression_is_triaged_again_once_the_merge_has_cooled_down(conn):
    triage = create_run(conn, sentry.RUN_KIND, SUBJECT, {"repo": "panama-in-context"})
    append(conn, triage, EventType.RUN_DONE)
    age(conn, triage, 30)
    review = parked_review(conn, 472)

    # Parked: the open review holds the subject, however old the triage.
    assert triage_poll(conn).enqueued == []

    sweep(conn, FakePulls({472: merged(472)}))

    # Just merged: the fix gets the cooldown to show in Sentry, counted from
    # the end of the chain rather than from the triage a month ago.
    assert triage_poll(conn).dropped == {"its fix chain closed recently": 1}

    age(conn, review, 8)
    assert triage_poll(conn).enqueued == [SUBJECT]
