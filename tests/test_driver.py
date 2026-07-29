"""The weekly-series driver (#13): claim, call, report, and the session's end.

What matters here: the driver executes exactly the call plan PIC hands it
(including the two multi-call shapes), reports honestly, never improvises
around a failing step, and always ends its session in a state a human can
read — a summary email for a complete week, a parked run listing the stuck
tasks for anything less.
"""

from orchestrator import driver, notify
from orchestrator.driver import PicClient, execute_calls
from orchestrator.enums import EventType, RunStatus
from orchestrator.log import load_events
from orchestrator.queue import get_run


def call(method="POST", path="/api/v1/x", **kw):
    return {"method": method, "path": path, **kw}


JOB_POLL = call(
    "GET",
    "/api/v1/admin/dashboard/agent-tasks/jobs/{job_id}",
    notes="substitute the job id from the previous response",
)


class FakePic:
    """Scripted PIC. Each test declares exactly the responses it expects."""

    base_url = "https://pic.test"

    def __init__(
        self,
        *,
        next_responses=(),
        call_responses=(),
        jobs=(),
        goal=(),
        plan=None,
        report_status=200,
    ):
        self.next_responses = list(next_responses)
        self.call_responses = list(call_responses)
        self.jobs = list(jobs)
        self.goal = list(goal)
        self.plan = plan or {"goal_key": "weekly_series:2026-W31", "created": 0}
        self.report_status = report_status
        self.reports = []
        self.requests = []
        self.planned = 0

    def next_task(self):
        return self.next_responses.pop(0) if self.next_responses else None

    def plan_week(self):
        self.planned += 1
        return self.plan

    def report(self, task_id, status, *, result=None, error=None):
        self.reports.append((task_id, status, error))
        return self.report_status, {}

    def job(self, job_id):
        return self.jobs.pop(0)

    def goal_tasks(self, goal_key):
        return self.goal

    def request(self, method, path, *, body=None, query=None):
        self.requests.append((method, path, body))
        return self.call_responses.pop(0) if self.call_responses else (200, {})


def task_response(task_id=1, kind="GENERATE_TAGS", calls=None):
    return {
        "task": {
            "id": task_id,
            "kind": kind,
            "goal_key": "weekly_series:2026-W31",
            "subject_type": "ARTICLE",
            "subject_id": 7,
            "attempts": 1,
        },
        "calls": calls if calls is not None else [call(path=f"/api/v1/t/{task_id}")],
    }


def goal_row(task_id, status, kind="WRITE_ARTICLE", error=None):
    return {
        "id": task_id,
        "status": status,
        "kind": kind,
        "subject_type": "ARTICLE",
        "subject_id": 7,
        "error": error,
    }


def run_drive(conn, pic, **kw):
    kw.setdefault("max_tasks", 60)
    kw.setdefault("max_failures", 5)
    kw.setdefault("sleep", lambda s: None)
    return driver.drive(conn, pic, worker_id="driver-test", **kw)


def drive_run(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM agent_runs WHERE kind = %s ORDER BY id DESC LIMIT 1",
            (driver.RUN_KIND,),
        )
        return get_run(conn, cur.fetchone()["id"])


# --- executing one call plan ---------------------------------------------------


def test_a_plain_call_plan_runs_in_order():
    pic = FakePic(call_responses=[(200, {"a": 1}), (201, {"b": 2})])
    ok, last, error = execute_calls(
        pic, [call(path="/api/v1/one"), call(path="/api/v1/two")]
    )
    assert ok and error is None
    assert last == {"b": 2}
    assert [p for _, p, _ in pic.requests] == ["/api/v1/one", "/api/v1/two"]


def test_the_job_poll_shape_polls_to_success():
    pic = FakePic(
        call_responses=[(202, {"id": 55, "status": "PENDING"})],
        jobs=[
            (200, {"id": 55, "status": "RUNNING"}),
            (200, {"id": 55, "status": "SUCCEEDED", "stats": {"claims": 12}}),
        ],
    )
    ok, last, error = execute_calls(
        pic, [call(path="/api/v1/fact-check"), JOB_POLL], sleep=lambda s: None
    )
    assert ok and error is None
    assert last["status"] == "SUCCEEDED"


def test_a_failed_job_fails_the_task():
    pic = FakePic(
        call_responses=[(202, {"id": 55, "status": "PENDING"})],
        jobs=[(200, {"id": 55, "status": "FAILED", "error": "grounding blew up"})],
    )
    ok, _, error = execute_calls(
        pic, [call(path="/api/v1/fact-check"), JOB_POLL], sleep=lambda s: None
    )
    assert not ok
    assert "grounding blew up" in error


def test_a_job_that_never_finishes_times_out():
    pic = FakePic(
        call_responses=[(202, {"id": 55, "status": "PENDING"})],
        jobs=[(200, {"id": 55, "status": "RUNNING"})] * 50,
    )
    clock = iter(range(0, 10_000, 400))
    ok, _, error = execute_calls(
        pic,
        [call(path="/api/v1/fact-check"), JOB_POLL],
        sleep=lambda s: None,
        now=lambda: next(clock),
        timeout_seconds=1000,
    )
    assert not ok
    assert "after 1000s" in error


def test_corrections_apply_carries_the_previewed_edits():
    edits = [{"claim_id": 3, "find": "x", "replace": "y"}]
    pic = FakePic(call_responses=[(200, {"edits": edits}), (200, {})])
    base = "/api/v1/admin/dashboard/fact-check/ARTICLE/7/corrections"
    ok, _, _ = execute_calls(
        pic, [call(path=f"{base}/preview"), call(path=f"{base}/apply")]
    )
    assert ok
    assert pic.requests[-1] == ("POST", f"{base}/apply", {"edits": edits})


def test_a_non_2xx_ends_the_plan_there():
    pic = FakePic(call_responses=[(400, {"detail": "no feature image yet"})])
    ok, _, error = execute_calls(
        pic, [call(path="/api/v1/publish"), call(path="/api/v1/never")]
    )
    assert not ok
    assert "400" in error and "no feature image" in error
    assert len(pic.requests) == 1, "a failing step must not run its successors"


def test_a_success_false_flag_fails_the_task():
    pic = FakePic(call_responses=[(200, {"success": False, "message": "no parent"})])
    ok, _, error = execute_calls(pic, [call(path="/api/v1/series-sections")])
    assert not ok
    assert "success=false" in error


# --- the session ----------------------------------------------------------------


def test_a_session_works_the_queue_and_completes(conn):
    pic = FakePic(
        next_responses=[task_response(1), task_response(2), None, None],
        goal=[goal_row(1, "DONE"), goal_row(2, "DONE")],
    )

    summary = run_drive(conn, pic)

    assert [(t, s) for t, s, _ in pic.reports] == [(1, "DONE"), (2, "DONE")]
    assert pic.planned == 1, "the 204 triggers exactly one ensure-pass"
    assert summary["outcome"] == "COMPLETE"
    run = drive_run(conn)
    assert run.status is RunStatus.DONE
    artifacts = [e for e in load_events(conn, run.id) if e.type is EventType.ARTIFACT]
    assert [a.payload["task_id"] for a in artifacts] == [1, 2]
    from orchestrator.log import verify_replay

    assert verify_replay(conn, run.id)


def test_tasks_created_by_the_ensure_pass_are_worked(conn):
    pic = FakePic(
        next_responses=[None, task_response(3), None, None],
        plan={"goal_key": "weekly_series:2026-W31", "created": 5},
        goal=[goal_row(3, "DONE")],
    )

    summary = run_drive(conn, pic)

    assert [(t, s) for t, s, _ in pic.reports] == [(3, "DONE")]
    assert summary["outcome"] == "COMPLETE"


def test_a_failing_task_is_reported_failed_and_the_session_continues(conn):
    pic = FakePic(
        next_responses=[task_response(1), task_response(2), None, None],
        call_responses=[(500, {"detail": "boom"}), (200, {})],
        goal=[goal_row(1, "FAILED", error="boom"), goal_row(2, "DONE")],
    )

    summary = run_drive(conn, pic)

    assert [(t, s) for t, s, _ in pic.reports] == [(1, "FAILED"), (2, "DONE")]
    assert summary["outcome"] == "INCOMPLETE"
    assert summary["parked"][0]["task_id"] == 1
    assert drive_run(conn).status is RunStatus.AWAITING_HUMAN


def test_the_failure_budget_stops_a_broken_session(conn):
    pic = FakePic(
        next_responses=[task_response(n) for n in range(1, 10)],
        call_responses=[(500, {"detail": "down"})] * 9,
        goal=[goal_row(1, "FAILED", error="down")],
    )

    summary = run_drive(conn, pic, max_failures=2)

    assert len(pic.reports) == 2, "the budget ends the session, not the report"
    assert "2 failures" in summary["stopped_early"]
    assert drive_run(conn).status is RunStatus.AWAITING_HUMAN


def test_the_task_cap_stops_a_looping_session(conn):
    pic = FakePic(
        next_responses=[task_response(n) for n in range(1, 10)],
        goal=[],
    )
    summary = run_drive(conn, pic, max_tasks=3)
    assert len(pic.reports) == 3
    assert "3-task session cap" in summary["stopped_early"]
    assert drive_run(conn).status is RunStatus.AWAITING_HUMAN


def test_a_rejected_report_counts_as_a_failure(conn):
    """A 409'd report leaves the row LEASED until PIC's sweep reclaims it, so
    the goal state shows the seam and the session parks for a human."""
    pic = FakePic(
        next_responses=[task_response(1), None, None],
        report_status=409,
        goal=[goal_row(1, "LEASED")],
    )
    summary = run_drive(conn, pic)
    assert summary["tasks_failed"] == 1
    assert summary["outcome"] == "INCOMPLETE"
    assert summary["unfinished"] == 1


# --- the emails ------------------------------------------------------------------


def test_an_incomplete_drive_emails_the_parked_tasks(conn):
    pic = FakePic(
        next_responses=[task_response(1), None, None],
        call_responses=[(500, {"detail": "boom"})],
        goal=[goal_row(1, "FAILED", kind="WRITE_ARTICLE", error="boom")],
    )
    run_drive(conn, pic)

    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    body = sent[0].body
    assert "weekly drive needs attention" in body
    assert "task 1" in body and "WRITE_ARTICLE" in body and "boom" in body


def test_a_complete_drive_still_says_so_once(conn):
    pic = FakePic(
        next_responses=[task_response(1), None, None],
        goal=[goal_row(1, "DONE")],
    )
    run_drive(conn, pic)

    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    assert "weekly drive: COMPLETE" in sent[0].subject
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 0


# --- the client ------------------------------------------------------------------


def test_the_client_treats_http_errors_as_data():
    import io
    import urllib.error

    def opener(request, timeout=0):
        raise urllib.error.HTTPError(
            request.full_url, 401, "nope", None, io.BytesIO(b'{"detail": "bad token"}')
        )

    client = PicClient("https://pic.test", "tok", opener=opener)
    status, data = client.request("GET", "/api/v1/x")
    assert status == 401
    assert data == {"detail": "bad token"}
