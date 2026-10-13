"""The weekly-series driver (#13): claim, call, report, and the session's end.

What matters here: the driver executes exactly the call plan PIC hands it
(including the two multi-call shapes), reports honestly, never improvises
around a failing step, and always ends its session in a state a human can
read — a summary email for a complete week, a parked run listing the stuck
tasks for anything less.
"""

from orchestrator import config, driver, notify
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
        self.goal = goal if isinstance(goal, dict) else list(goal)
        self.goals_read = []
        self.waiting_for = []
        self.plan = plan or {"goal_key": "weekly_series:2026-W31", "created": 0}
        self.report_status = report_status
        self.reports = []
        self.requests = []
        self.timeouts = []
        self.planned = 0
        self.report_raises = None

    def next_task(self):
        return self.next_responses.pop(0) if self.next_responses else None

    def plan_week(self):
        self.planned += 1
        return self.plan

    def report(self, task_id, status, *, result=None, error=None, waiting_for=None):
        self.reports.append((task_id, status, error))
        self.waiting_for.append(waiting_for)
        if self.report_raises is not None:
            raise self.report_raises
        return self.report_status, {}

    def job(self, job_id):
        return self.jobs.pop(0)

    def goal_tasks(self, goal_key):
        self.goals_read.append(goal_key)
        if isinstance(self.goal, dict):
            return self.goal.get(goal_key, [])
        return self.goal

    def request(self, method, path, *, body=None, query=None, timeout=60):
        self.requests.append((method, path, body))
        self.timeouts.append(timeout)
        response = self.call_responses.pop(0) if self.call_responses else (200, {})
        if isinstance(response, BaseException):
            raise response
        return response


def task_response(
    task_id=1, kind="GENERATE_TAGS", calls=None, goal_key="weekly_series:2026-W31"
):
    return {
        "task": {
            "id": task_id,
            "kind": kind,
            "goal_key": goal_key,
            "subject_type": "ARTICLE",
            "subject_id": 7,
            "attempts": 1,
        },
        "calls": calls if calls is not None else [call(path=f"/api/v1/t/{task_id}")],
    }


def goal_row(task_id, status, kind="WRITE_ARTICLE", error=None, waiting_for=None):
    return {
        "id": task_id,
        "status": status,
        "kind": kind,
        "subject_type": "ARTICLE",
        "subject_id": 7,
        "error": error,
        "waiting_for": waiting_for,
    }


def wait_task(task_id, kind):
    return task_response(
        task_id,
        kind=kind,
        calls=[
            call(
                "GET",
                f"/api/v1/admin/dashboard/agent-tasks/wait-check/{kind}/7",
            )
        ],
    )


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


def test_a_transport_timeout_fails_the_task_not_the_session():
    """The 2026-09-01 drive: generate-outlines outlived the read timeout and
    the raw TimeoutError killed the whole session. It must be a task outcome."""
    pic = FakePic(call_responses=[TimeoutError("The read operation timed out")])
    ok, _, error = execute_calls(pic, [call(path="/api/v1/generate-outlines")])
    assert not ok
    assert "transport failure" in error and "timed out" in error


def test_task_calls_get_the_long_llm_timeout():
    pic = FakePic(call_responses=[(200, {})])
    execute_calls(pic, [call(path="/api/v1/generate-outlines")])
    assert pic.timeouts == [config.DRIVER_CALL_TIMEOUT_SECONDS]


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


def test_an_unreportable_outcome_counts_as_a_failure(conn):
    """A report that dies in transport must not crash the session; the lease
    sweep re-queues the row, and the session records the failure."""
    pic = FakePic(
        next_responses=[task_response(1), None, None],
        goal=[goal_row(1, "LEASED")],
    )
    pic.report_raises = TimeoutError("The read operation timed out")
    summary = run_drive(conn, pic)
    assert summary["tasks_failed"] == 1
    assert summary["outcome"] == "INCOMPLETE"


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
    assert "daily drive needs attention" in body
    assert "task 1" in body and "WRITE_ARTICLE" in body and "boom" in body


def test_a_complete_drive_still_says_so_once(conn):
    pic = FakePic(
        next_responses=[task_response(1), None, None],
        goal=[goal_row(1, "DONE")],
    )
    run_drive(conn, pic)

    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    assert "daily drive: COMPLETE" in sent[0].subject
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


# --- the queue surface refusing the session (#55) ------------------------------


class RefusingPic(FakePic):
    """PIC whose plan-week (or next) answers with something other than 200."""

    def __init__(self, *, plan_status=None, next_status=None, **kw):
        super().__init__(**kw)
        self.plan_status = plan_status
        self.next_status = next_status

    def next_task(self):
        if self.next_status is not None:
            raise driver.ProtocolError("next", self.next_status, {"detail": "nope"})
        return super().next_task()

    def plan_week(self):
        self.planned += 1
        if self.plan_status is not None:
            raise driver.ProtocolError(
                "plan-week",
                self.plan_status,
                {"detail": "No research available for planning"},
            )
        return super().plan_week()


def test_an_empty_research_backlog_fails_the_session_and_names_the_fix(conn):
    pic = RefusingPic(plan_status=404)

    summary = run_drive(conn, pic)

    assert summary["outcome"] == "FAILED"
    assert summary["reason"] == driver.REASON_NO_RESEARCH
    run = drive_run(conn)
    assert run.status is RunStatus.FAILED
    failed = [e for e in load_events(conn, run.id) if e.type is EventType.RUN_FAILED]
    assert failed[-1].payload["reason"] == driver.REASON_NO_RESEARCH
    assert "No research available" in failed[-1].payload["detail"]
    from orchestrator.log import verify_replay

    assert verify_replay(conn, run.id)

    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    email = sent[0]
    assert "NO APPROVED RESEARCH" in email.subject
    assert "APPROVED research" in email.body
    assert "No research available for planning" in email.body
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 0, "said once"


def test_a_refusal_mid_session_keeps_the_work_already_reported(conn):
    # One task runs and is reported DONE; then the ensure-pass 404s.
    pic = RefusingPic(next_responses=[task_response(1), None], plan_status=404)

    summary = run_drive(conn, pic)

    assert [(t, s) for t, s, _ in pic.reports] == [(1, "DONE")]
    assert summary["outcome"] == "FAILED"
    assert summary["tasks_done"] == 1
    assert drive_run(conn).status is RunStatus.FAILED


def test_any_other_queue_refusal_is_a_protocol_failure_not_a_traceback(conn):
    pic = RefusingPic(next_status=401)

    summary = run_drive(conn, pic)

    assert summary["outcome"] == "FAILED"
    assert summary["reason"] == driver.REASON_PROTOCOL
    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    assert "next answered 401" in sent[0].subject
    assert "nope" in sent[0].body


def test_transport_trouble_on_the_queue_surface_fails_the_session(conn):
    class DeadPic(FakePic):
        def next_task(self):
            raise TimeoutError("read timed out")

    summary = run_drive(conn, DeadPic())

    assert summary["outcome"] == "FAILED"
    assert summary["reason"] == driver.REASON_PROTOCOL
    assert "read timed out" in summary["detail"]
    assert drive_run(conn).status is RunStatus.FAILED


def test_the_client_raises_a_typed_error_on_a_refused_plan_week():
    import io
    import urllib.error

    def opener(request, timeout=0):
        raise urllib.error.HTTPError(
            request.full_url,
            404,
            "nope",
            None,
            io.BytesIO(b'{"detail": "No research available for planning"}'),
        )

    client = PicClient("https://pic.test", "tok", opener=opener)
    try:
        client.plan_week()
    except driver.ProtocolError as exc:
        assert exc.status == 404
        assert exc.reason == driver.REASON_NO_RESEARCH
        assert "No research available" in str(exc)
    else:
        raise AssertionError("plan-week 404 must raise ProtocolError")


def test_main_exits_nonzero_and_commits_as_it_goes(monkeypatch, migrated_dsn):
    """A failed drive must (a) exit 1 for cron, and (b) have committed its run
    before returning: the 2026-09-07 crash rolled the whole run back because
    the session's connection never committed until a clean exit."""
    seen = {}

    def fake_drive(conn, client, **kw):
        seen["autocommit"] = conn.autocommit
        return {"outcome": "FAILED"}

    monkeypatch.setattr(driver, "drive", fake_drive)
    monkeypatch.setattr(config, "PIC_DRIVER_TOKEN", "tok")
    monkeypatch.setattr(config, "DATABASE_URL", migrated_dsn)

    assert driver.main([]) == 1
    assert seen["autocommit"] is True


# --- wait tasks (#62, PIC's #553) ------------------------------------------------


def test_an_unmet_wait_reports_waiting_not_failed(conn):
    pic = FakePic(
        next_responses=[wait_task(1, "WAIT_GRAMMAR_EN"), None, None],
        call_responses=[
            (200, {"satisfied": False, "waiting_for": "Grammarly EN on article 7"})
        ],
        goal=[
            goal_row(
                1,
                "WAITING",
                kind="WAIT_GRAMMAR_EN",
                waiting_for="Grammarly EN on article 7",
            ),
            goal_row(2, "PENDING"),
        ],
    )

    summary = run_drive(conn, pic)

    assert pic.reports == [(1, "WAITING", None)]
    assert pic.waiting_for == ["Grammarly EN on article 7"]
    assert summary["tasks_failed"] == 0 and summary["tasks_waiting"] == 1
    assert summary["outcome"] == "INCOMPLETE"
    assert not summary["needs_attention"], "a dependent behind a wait is expected"
    run = drive_run(conn)
    assert run.status is RunStatus.AWAITING_HUMAN
    artifacts = [e for e in load_events(conn, run.id) if e.type is EventType.ARTIFACT]
    assert artifacts[0].payload["reported"] == "WAITING"
    assert artifacts[0].payload["waiting_for"] == "Grammarly EN on article 7"

    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    assert "waiting on you: Grammarly EN" in sent[0].subject
    body = sent[0].body
    assert "Waiting on you" in body
    assert "  Grammarly EN:" in body
    assert "task 1  WAIT_GRAMMAR_EN" in body and "Grammarly EN on article 7" in body


def test_a_met_wait_reports_done(conn):
    pic = FakePic(
        next_responses=[wait_task(1, "WAIT_RESEARCH_READY"), None, None],
        call_responses=[(200, {"satisfied": True, "waiting_for": None})],
        goal=[goal_row(1, "DONE")],
    )
    summary = run_drive(conn, pic)
    assert pic.reports == [(1, "DONE", None)]
    assert pic.waiting_for == [None]
    assert summary["outcome"] == "COMPLETE"


def test_a_malformed_wait_check_fails_the_task(conn):
    pic = FakePic(
        next_responses=[wait_task(1, "WAIT_GRAMMAR_ES"), None, None],
        call_responses=[(200, {"something": "else"})],
        goal=[goal_row(1, "READY")],
    )
    summary = run_drive(conn, pic)
    assert pic.reports[0][1] == "FAILED"
    assert "satisfied" in pic.reports[0][2]
    assert summary["tasks_failed"] == 1


def test_waiting_items_group_by_kind_in_the_email(conn):
    pic = FakePic(
        next_responses=[
            wait_task(1, "WAIT_GRAMMAR_ES"),
            wait_task(2, "WAIT_ARTICLE_DEEP_RESEARCH"),
            wait_task(3, "WAIT_SOMETHING_NEW"),
            None,
            None,
        ],
        call_responses=[
            (200, {"satisfied": False, "waiting_for": "es"}),
            (200, {"satisfied": False, "waiting_for": "deep"}),
            (200, {"satisfied": False}),
        ],
        goal=[
            goal_row(1, "WAITING", kind="WAIT_GRAMMAR_ES", waiting_for="es"),
            goal_row(2, "WAITING", kind="WAIT_ARTICLE_DEEP_RESEARCH", waiting_for="d"),
            goal_row(3, "WAITING", kind="WAIT_SOMETHING_NEW", waiting_for="x"),
        ],
    )
    run_drive(conn, pic)
    assert pic.waiting_for[2] == "operator action", "a blank text still reports"

    sent = []
    notify.pass_once(conn, sent.append, to="j@x", cap=10)
    body = sent[0].body
    assert body.index("  Deep research:") < body.index("  Grammarly ES:")
    assert "  wait something new:" in body, "an unknown wait kind still groups"


def test_a_failure_alongside_waits_still_needs_attention(conn):
    pic = FakePic(
        next_responses=[wait_task(1, "WAIT_GRAMMAR_EN"), task_response(2), None, None],
        call_responses=[(200, {"satisfied": False, "waiting_for": "g"}), (500, {})],
        goal=[
            goal_row(1, "WAITING", kind="WAIT_GRAMMAR_EN", waiting_for="g"),
            goal_row(2, "FAILED", error="boom"),
        ],
    )
    summary = run_drive(conn, pic)
    assert summary["needs_attention"]
    sent = []
    notify.pass_once(conn, sent.append, to="j@x", cap=10)
    assert "daily drive needs attention" in sent[0].subject
    assert "Grammarly EN" in sent[0].body and "Parked tasks" in sent[0].body


def test_waits_count_toward_the_task_cap(conn):
    pic = FakePic(
        next_responses=[wait_task(n, "WAIT_GRAMMAR_EN") for n in range(1, 10)],
        call_responses=[(200, {"satisfied": False, "waiting_for": "g"})] * 9,
    )
    summary = run_drive(conn, pic, max_tasks=2)
    assert len(pic.reports) == 2
    assert "2-task session cap" in summary["stopped_early"]


def test_wait_labels_match_loosely_and_fall_back_to_the_kind():
    assert driver.wait_label("WAIT_GRAMMAR_EN") == "Grammarly EN"
    assert driver.wait_label("GRAMMARLY_ES") == "Grammarly ES"
    assert driver.wait_label("GRAMMAR_CHECK") == "Grammarly"
    assert driver.wait_label("WAIT_RESEARCH_READY") == "Deep research"
    assert driver.wait_label("WAIT_ARTICLE_CLAIMS") == "Deep research"
    assert driver.wait_label("WAIT_FOR_PHOTO") == "wait for photo"
    assert driver.wait_label(None) == "unknown"


def test_the_client_sends_waiting_for_only_with_a_waiting_report():
    import json

    sent = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"{}"

    def opener(request, timeout=0):
        sent.append(json.loads(request.data))
        return Response()

    client = PicClient("https://pic.test", "tok", opener=opener)
    client.report(1, "WAITING", waiting_for="Grammarly EN")
    client.report(2, "DONE")
    assert sent[0]["waiting_for"] == "Grammarly EN"
    assert "waiting_for" not in sent[1]


# --- the summary covers every goal touched (#62) ---------------------------------


def test_the_summary_covers_every_goal_the_session_touched(conn):
    old, new = "daily_series:2026-10-04", "daily_series:2026-10-05"
    pic = FakePic(
        next_responses=[
            task_response(1, goal_key=old),
            None,
            task_response(2, goal_key=new),
            None,
            None,
        ],
        call_responses=[(500, {"detail": "boom"}), (200, {})],
        plan={"goal_key": new, "created": 30},
        goal={
            old: [goal_row(1, "FAILED", error="boom")],
            new: [goal_row(2, "DONE"), goal_row(3, "PENDING")],
        },
    )

    summary = run_drive(conn, pic)

    assert [g["goal_key"] for g in summary["goals"]] == [old, new]
    assert summary["parked"][0]["goal_key"] == old, "the older goal is not hidden"
    assert summary["unfinished"] == 1
    assert old in summary["summary"] and new in summary["summary"]
    sent = []
    notify.pass_once(conn, sent.append, to="j@x", cap=10)
    assert f"- {old}: 1 task(s)" in sent[0].body
    assert f"- {new}: 2 task(s)" in sent[0].body


# --- plan-week at capacity (#62, PIC's #559) -------------------------------------


def test_at_capacity_is_a_normal_end_not_an_error(conn):
    pic = FakePic(plan={"goal_key": None, "at_capacity": True, "created": 0})

    summary = run_drive(conn, pic)

    assert pic.planned == 1
    assert summary["outcome"] == "AT_CAPACITY"
    assert summary["goals"] == []
    run = drive_run(conn)
    assert run.status is RunStatus.DONE
    sent = []
    assert notify.pass_once(conn, sent.append, to="j@x", cap=10) == 1
    assert "at capacity" in sent[0].subject
    assert "needs attention" not in sent[0].subject


def test_at_capacity_after_working_a_goal_reports_that_goal(conn):
    pic = FakePic(
        next_responses=[task_response(1), None],
        plan={"detail": "At capacity: 2 series in flight"},
        goal=[goal_row(1, "DONE")],
    )
    summary = run_drive(conn, pic)
    assert summary["outcome"] == "COMPLETE"
    assert summary["at_capacity"]
    assert "at capacity" in summary["summary"]


def test_capacity_detection():
    assert driver.at_capacity({"at_capacity": True})
    assert driver.at_capacity({"status": "AT_CAPACITY"})
    assert driver.at_capacity({"message": "plan-week at capacity"})
    assert not driver.at_capacity({"goal_key": "daily_series:2026-10-05"})


def test_a_session_with_no_goal_and_no_capacity_still_parks(conn):
    pic = FakePic(plan={"created": 0})
    summary = run_drive(conn, pic)
    assert summary["outcome"] == "INCOMPLETE"
    assert drive_run(conn).status is RunStatus.AWAITING_HUMAN


# --- dry run uses the cadence's goal key (#62) -----------------------------------


def test_goal_keys_follow_the_cadence():
    from datetime import date

    assert driver.goal_key_for(date(2026, 10, 5), "daily") == "daily_series:2026-10-05"
    assert driver.goal_key_for(date(2026, 10, 5), "weekly") == "weekly_series:2026-W41"
    # ISO year, not calendar year: 2027-01-01 is in 2026's week 53.
    assert driver.goal_key_for(date(2027, 1, 1), "weekly") == "weekly_series:2026-W53"


def test_dry_run_reads_the_cadence_goal_and_claims_nothing(monkeypatch, capsys):
    class DryPic(FakePic):
        def __init__(self, *a, **kw):
            super().__init__(goal=[goal_row(4, "WAITING", waiting_for="Grammarly EN")])

        def next_task(self):
            raise AssertionError("dry run must not claim")

    made = []
    monkeypatch.setattr(
        driver, "PicClient", lambda *a: made.append(DryPic()) or made[0]
    )
    monkeypatch.setattr(config, "PIC_DRIVER_TOKEN", "tok")
    monkeypatch.setattr(config, "DRIVER_CADENCE", "daily")

    assert driver.main(["--dry-run"]) == 0
    assert made[0].goals_read[0].startswith("daily_series:")
    assert "waiting for: Grammarly EN" in capsys.readouterr().out
