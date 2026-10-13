"""Pipeline supervisor, phase 2 (#84): the orchestrator retries, within limits.

What is pinned down: the host-side MCP client speaks PIC's transport and calls
only the tools it was built for; a diagnosis asks for retries only when it
recommends ``retry``, and only when the kill switch and the pipeline token are
both set; the orchestrator (not the model) refuses a second retry of a task
within 24 hours, across runs, before anything is sent; never a skip, never a
publish-path kind; every attempt lands as a ``task_action`` event and in the
diagnosis email; the pipeline token is redacted like the read token.

PIC is a real HTTP server on localhost speaking JSON-RPC, so the transport is
exercised end to end; nothing here reaches PIC.
"""

import functools
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg
import pytest
from psycopg.rows import dict_row

from orchestrator import (
    auto_retry,
    config,
    jobs,
    log,
    mcp_client,
    notify,
    redact,
    supervisor,
)
from orchestrator.enums import EventType, RunStatus
from orchestrator.log import create_run, load_events
from orchestrator.queue import get_run
from tests.fakes import FakeRunner
from tests.test_adversary import WORKER, finish_one
from tests.test_notify import FakeTransport, finished, send_pass
from tests.test_supervisor import (
    DIAGNOSIS,
    GOAL,
    OTHER,
    scan,
    seed_failed_drive,
    supervise_run,
)

PIPELINE_TOKEN = "pic_mcp_pipeline_fedcba9876543210"

RETRY_DIAGNOSIS = {
    **DIAGNOSIS,
    "cause": "Gemini answered 429 at 09:12; the quota has since reset.",
    "recommended_action": "retry",
    "tasks": [
        {
            "task_id": 767,
            "kind": "GENERATE_FEATURE_IMAGE",
            "action": "retry",
            "why": "Gemini 429 at 09:12; quota reset",
        }
    ],
}


#: ``auto_retry.execute`` on the shared, rolled-back test connection: it must
#: not commit (the loop's way), or one test's rows would outlive it. The
#: committing path has its own test on its own connections.
execute = functools.partial(auto_retry.execute, durable=False)


# --- a fake PIC MCP server ---------------------------------------------------------


class FakeMcp:
    """Streamable HTTP, stateless, JSON responses: what PIC serves.

    ``tool_errors`` maps a task id to the error text PIC answers it with;
    ``mode`` switches the reply to SSE, a JSON-RPC error or an HTTP status.
    """

    def __init__(self):
        self.requests: list[dict] = []
        self.headers: list[dict] = []
        self.tool_errors: dict[int, str] = {}
        self.mode = "json"
        self.reply_override = None
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                fake.requests.append(body)
                fake.headers.append(self.headers)  # case-insensitive
                if fake.mode.startswith("http"):
                    self.send_response(int(fake.mode[4:]))
                    self.end_headers()
                    return
                if "id" not in body:
                    self.send_response(202)
                    self.end_headers()
                    return
                reply = fake.reply(body)
                if fake.mode == "sse":
                    data = (
                        b"event: message\ndata: not json\n\n"
                        + b"event: message\ndata: "
                        + json.dumps(reply).encode()
                        + b"\n\n"
                    )
                    ctype = "text/event-stream"
                else:
                    data = (
                        fake.reply_override
                        if fake.reply_override is not None
                        else json.dumps(reply).encode()
                    )
                    ctype = "application/json"
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Mcp-Session-Id", "sess-1")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/mcp"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def reply(self, body):
        if self.mode == "rpc_error":
            return {
                "jsonrpc": "2.0",
                "id": body["id"],
                "error": {"code": -32602, "message": "bad params"},
            }
        if body["method"] == "initialize":
            result = {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "pic", "version": "1"},
            }
        else:
            args = body["params"]["arguments"]
            task_id = args["task_id"]
            if task_id in self.tool_errors:
                result = {
                    "isError": True,
                    "content": [{"type": "text", "text": self.tool_errors[task_id]}],
                }
            else:
                task = {
                    "task_id": task_id,
                    "goal_key": GOAL,
                    "kind": "GENERATE_FEATURE_IMAGE",
                    "status": "READY",
                    "attempts": 0,
                }
                structured = {
                    "task": task,
                    "promoted": [],
                    "replayed": False,
                    "last_action": {"action": "retry", "reason": args["reason"]},
                    "truncated": False,
                }
                result = {
                    "content": [{"type": "text", "text": json.dumps(structured)}],
                    "structuredContent": structured,
                    "isError": False,
                }
        return {"jsonrpc": "2.0", "id": body["id"], "result": result}

    def calls(self):
        return [r for r in self.requests if r.get("method") == "tools/call"]

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture()
def pic():
    server = FakeMcp()
    yield server
    server.close()


@pytest.fixture()
def armed(monkeypatch, pic):
    """Auto-retry on, with the pipeline token, against the fake server."""
    monkeypatch.setattr(config, "SUPERVISOR_AUTO_RETRY", True)
    monkeypatch.setattr(config, "PIC_MCP_PIPELINE_TOKEN", PIPELINE_TOKEN)
    monkeypatch.setattr(config, "PIC_MCP_URL", pic.url)
    monkeypatch.setattr(config, "PIC_MCP_TIMEOUT_SECONDS", 5)
    return pic


def client(pic, tools=auto_retry.HOST_TOOLS):
    return mcp_client.Client(pic.url, PIPELINE_TOKEN, allowed_tools=tools, timeout=5)


# --- the client --------------------------------------------------------------------


def test_the_client_initializes_then_calls_the_tool(pic):
    c = client(pic)

    answer = c.call_tool("retry_agent_task", {"task_id": 767, "reason": "r"})

    assert answer["task"]["status"] == "READY"
    methods = [r["method"] for r in pic.requests]
    assert methods == ["initialize", "notifications/initialized", "tools/call"]
    assert pic.calls()[0]["params"] == {
        "name": "retry_agent_task",
        "arguments": {"task_id": 767, "reason": "r"},
    }
    first, *_, last = pic.headers
    assert first["Authorization"] == f"Bearer {PIPELINE_TOKEN}"
    assert "text/event-stream" in first["Accept"]
    assert last["Mcp-Session-Id"] == "sess-1"
    assert last["MCP-Protocol-Version"] == "2025-06-18"

    c.call_tool("retry_agent_task", {"task_id": 768, "reason": "r"})
    assert [r["method"] for r in pic.requests].count("initialize") == 1


def test_the_client_reads_an_sse_reply(pic):
    pic.mode = "sse"
    answer = client(pic).call_tool("retry_agent_task", {"task_id": 5, "reason": "r"})
    assert answer["task"]["task_id"] == 5


def test_a_tool_outside_the_allow_list_is_refused_before_anything_is_sent(pic):
    for tool in ("skip_agent_task", "request_publish", "pipeline_status"):
        with pytest.raises(PermissionError):
            client(pic).call_tool(tool, {"task_id": 1, "reason": "r"})
    assert pic.requests == []


def test_a_tool_error_is_a_tool_error(pic):
    pic.tool_errors[9] = "refused: retry on a APPROVE_ARTICLE task is reserved"
    with pytest.raises(mcp_client.ToolError, match="refused: retry on a APPROVE"):
        client(pic).call_tool("retry_agent_task", {"task_id": 9, "reason": "r"})


@pytest.mark.parametrize(
    ("mode", "message"),
    [("rpc_error", "JSON-RPC error -32602"), ("http401", "HTTP 401")],
)
def test_protocol_and_http_failures_are_mcp_errors_without_the_token(
    pic, mode, message
):
    pic.mode = mode
    with pytest.raises(mcp_client.McpError, match=message) as err:
        client(pic).call_tool("retry_agent_task", {"task_id": 1, "reason": "r"})
    assert PIPELINE_TOKEN not in str(err.value)


def test_an_unreachable_server_is_an_mcp_error():
    c = mcp_client.Client(
        "http://127.0.0.1:9/mcp", PIPELINE_TOKEN, allowed_tools=["t"], timeout=2
    )
    with pytest.raises(mcp_client.McpError, match="unreachable"):
        c.call_tool("t", {})


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (b"<html>", "not JSON"),
        (b'{"jsonrpc": "2.0", "id": 99, "result": {}}', "does not answer"),
        (b'{"jsonrpc": "2.0", "id": 1, "result": 5}', "no result"),
    ],
)
def test_a_reply_that_is_not_an_answer_is_an_mcp_error(pic, body, message):
    pic.reply_override = body
    with pytest.raises(mcp_client.McpError, match=message):
        client(pic).initialize()


def test_a_batched_reply_is_searched_for_the_answer(pic):
    pic.reply_override = (
        b'[{"jsonrpc": "2.0", "id": 7, "result": {}},'
        b' {"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "x"}}]'
    )
    c = client(pic)
    c.initialize()
    assert c._protocol == "x"


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        ({"content": [{"type": "text", "text": '{"a": 1}'}]}, {"a": 1}),
        ({"content": [{"type": "text", "text": "[1, 2]"}]}, {"result": [1, 2]}),
        ({"content": [{"type": "text", "text": "plain"}]}, {"text": "plain"}),
        ({"isError": True}, mcp_client.ToolError),
    ],
)
def test_unstructured_tool_results(pic, monkeypatch, result, expected):
    c = client(pic)
    c._initialized = True
    monkeypatch.setattr(c, "_request", lambda method, params: result)
    if expected is mcp_client.ToolError:
        with pytest.raises(mcp_client.ToolError, match="tool error"):
            c.call_tool("retry_agent_task", {})
    else:
        assert c.call_tool("retry_agent_task", {}) == expected


# --- what a diagnosis asks for -----------------------------------------------------


def diagnosis_run(conn, goal=GOAL):
    run_id = create_run(conn, "pipeline_supervise", goal, {"goal_key": goal})
    return get_run(conn, run_id)


def artifact_for(result):
    run = supervise_run()
    return run, supervisor.artifact(run, result)


def test_nothing_is_asked_while_the_switch_or_the_token_is_missing(monkeypatch):
    run, art = artifact_for(RETRY_DIAGNOSIS)
    monkeypatch.setattr(config, "SUPERVISOR_AUTO_RETRY", False)
    monkeypatch.setattr(config, "PIC_MCP_PIPELINE_TOKEN", PIPELINE_TOKEN)
    assert auto_retry.requests(run, art) == ()
    monkeypatch.setattr(config, "SUPERVISOR_AUTO_RETRY", True)
    monkeypatch.setattr(config, "PIC_MCP_PIPELINE_TOKEN", "")
    assert auto_retry.requests(run, art) == ()


def test_the_switch_is_off_by_default():
    import importlib
    import os

    assert "ORCHESTRATOR_SUPERVISOR_AUTO_RETRY" not in os.environ
    fresh = importlib.reload(config)
    try:
        assert fresh.SUPERVISOR_AUTO_RETRY is False
    finally:
        importlib.reload(config)


def test_a_retry_diagnosis_asks_for_its_retry_tasks_once_each(armed):
    result = {
        **RETRY_DIAGNOSIS,
        "tasks": [
            {"task_id": 767, "kind": "GENERATE_FEATURE_IMAGE", "action": "retry"},
            {"task_id": "767", "action": "retry", "why": "duplicate"},
            {"task_id": 768, "action": "skip", "why": "never skipped"},
            {"task_id": None, "action": "retry"},
            {"task_id": 0, "action": "retry"},
            {"task_id": 769, "action": "operator", "why": "not a retry"},
        ],
    }
    run, art = artifact_for(result)

    (req,) = auto_retry.requests(run, art)

    assert req["task_id"] == 767 and req["goal_key"] == GOAL
    # No per-task why: the diagnosis's cause is the reason.
    assert req["reason"].startswith(f"managed-agents supervisor run {run.id} (high")
    assert "Gemini answered 429" in req["reason"]


@pytest.mark.parametrize("action", ["skip", "operator_work_needed", "none"])
def test_only_a_retry_recommendation_asks(armed, action):
    run, art = artifact_for({**RETRY_DIAGNOSIS, "recommended_action": action})
    assert auto_retry.requests(run, art) == ()
    assert auto_retry.requests(run, None) == ()


def test_the_reason_fits_pics_limit(armed):
    long = {**RETRY_DIAGNOSIS["tasks"][0], "why": "x" * 5000}
    run, art = artifact_for({**RETRY_DIAGNOSIS, "tasks": [long]})
    (req,) = auto_retry.requests(run, art)
    assert 1 <= len(req["reason"]) <= auto_retry.MAX_REASON_CHARS


def test_the_chain_hands_the_retries_to_the_loop(armed):
    decision = jobs.followups(supervise_run(), RETRY_DIAGNOSIS)
    assert [r["task_id"] for r in decision.task_retries] == [767]
    assert decision.human_gate is None and len(decision.artifacts) == 1
    assert jobs.followups(supervise_run(), DIAGNOSIS).task_retries == ()


# --- the orchestrator's limits -----------------------------------------------------


def req(task_id=767, kind="GENERATE_FEATURE_IMAGE", reason="Gemini 429; reset"):
    return {"task_id": task_id, "kind": kind, "goal_key": GOAL, "reason": reason}


def actions(conn, run_id):
    return [
        e.payload for e in load_events(conn, run_id) if e.type is EventType.TASK_ACTION
    ]


def test_a_retry_is_made_with_the_diagnosis_reason_and_logged(conn, armed):
    run = diagnosis_run(conn)

    (out,) = execute(conn, run, [req()])

    assert out["outcome"] == auto_retry.EXECUTED
    assert out["status"] == "READY" and out["attempts"] == 0
    (call,) = armed.calls()
    assert call["params"]["arguments"] == {
        "task_id": 767,
        "reason": "Gemini 429; reset",
    }
    calling, executed = actions(conn, run.id)
    assert calling["outcome"] == auto_retry.CALLING
    assert executed["outcome"] == auto_retry.EXECUTED
    assert executed["tool"] == "retry_agent_task" and executed["action"] == "retry"


def test_a_second_retry_the_same_day_is_refused_by_the_orchestrator(conn, armed):
    first = diagnosis_run(conn)
    second = diagnosis_run(conn, OTHER)
    execute(conn, first, [req()])

    (out,) = execute(conn, second, [req(reason="again")])

    assert out["outcome"] == auto_retry.REFUSED_LIMIT
    assert out["previous_run"] == first.id
    assert len(armed.calls()) == 1  # nothing was sent for the second
    assert [a["outcome"] for a in actions(conn, second.id)] == [
        auto_retry.REFUSED_LIMIT
    ]
    # A different task is not limited by it.
    (other,) = execute(conn, second, [req(task_id=900)])
    assert other["outcome"] == auto_retry.EXECUTED


def test_the_limit_is_a_rolling_window(conn, armed, monkeypatch):
    run = diagnosis_run(conn)
    execute(conn, run, [req()])
    # A zero-hour window: the earlier call (at this transaction's now()) is
    # outside it, so the same task may be called again.
    monkeypatch.setattr(auto_retry, "WINDOW_HOURS", 0)
    (out,) = execute(conn, run, [req()])
    assert out["outcome"] == auto_retry.EXECUTED


def test_a_failed_call_still_uses_up_the_days_retry(conn, armed):
    run = diagnosis_run(conn)
    armed.mode = "http502"

    (out,) = execute(conn, run, [req()])

    assert out["outcome"] == auto_retry.ERROR and "HTTP 502" in out["detail"]
    armed.mode = "json"
    (again,) = execute(conn, run, [req()])
    assert again["outcome"] == auto_retry.REFUSED_LIMIT


def test_a_pic_refusal_is_logged_with_its_text(conn, armed):
    run = diagnosis_run(conn)
    armed.tool_errors[767] = "conflict: task 767 is LEASED"

    (out,) = execute(conn, run, [req()])

    assert out["outcome"] == auto_retry.REFUSED_PIC
    assert out["detail"] == "conflict: task 767 is LEASED"


@pytest.mark.parametrize(
    "kind",
    ["APPROVE_ARTICLE", "SET_PUBLISH_DATE", "approve_ig_post", "SCHEDULE_IG_POST"],
)
def test_a_publish_path_kind_is_never_retried(conn, armed, kind):
    run = diagnosis_run(conn)
    (out,) = execute(conn, run, [req(kind=kind)])
    assert out["outcome"] == auto_retry.REFUSED_KIND
    assert armed.requests == []


def test_one_run_makes_at_most_the_cap(conn, armed, monkeypatch):
    monkeypatch.setattr(config, "SUPERVISOR_MAX_RETRIES_PER_RUN", 2)
    run = diagnosis_run(conn)

    outs = execute(conn, run, [req(task_id=n) for n in (1, 2, 3)])

    assert [o["outcome"] for o in outs] == [
        auto_retry.EXECUTED,
        auto_retry.EXECUTED,
        auto_retry.REFUSED_CAP,
    ]
    assert len(armed.calls()) == 2


def test_nothing_is_made_once_the_switch_is_off(conn, armed, monkeypatch):
    run = diagnosis_run(conn)
    monkeypatch.setattr(config, "SUPERVISOR_AUTO_RETRY", False)
    assert execute(conn, run, [req()]) == []
    assert execute(conn, run, []) == []
    assert actions(conn, run.id) == [] and armed.requests == []


def test_an_unexpected_failure_is_logged_and_the_rest_go_on(conn, armed):
    run = diagnosis_run(conn)
    factories = iter([RuntimeError("boom")])

    def factory():
        error = next(factories, None)
        if error is not None:
            raise error
        return client(armed)

    outs = execute(conn, run, [req(task_id=1), req(task_id=2)], client_factory=factory)

    assert [o["outcome"] for o in outs] == [auto_retry.ERROR, auto_retry.EXECUTED]
    assert outs[0]["detail"] == "RuntimeError"


def test_run_actions_reports_a_call_with_no_outcome(conn):
    run = diagnosis_run(conn)
    log.append(
        conn,
        run.id,
        EventType.TASK_ACTION,
        {"action": "retry", "task_id": 5, "outcome": auto_retry.CALLING},
    )
    (only,) = auto_retry.run_actions(conn, run.id)
    assert only["outcome"] == auto_retry.CALLING


# --- through the loop and the email ------------------------------------------------


def retry_orchestrator(runner):
    from orchestrator.loop import Orchestrator

    return Orchestrator(
        runner,
        worker_id=WORKER,
        max_concurrent=1,
        backoff_base_seconds=0,
        followups=jobs.followups,
        task_retry=execute,
    )


@pytest.fixture()
def read_token(monkeypatch):
    from tests.test_pic_mcp import TOKEN

    monkeypatch.setattr(config, "PIC_MCP_READ_TOKEN", TOKEN)


def test_through_the_loop_a_retry_diagnosis_retries_and_the_email_says_so(
    conn, armed, read_token
):
    seed_failed_drive(conn)
    ((_, run_id),) = scan(conn)
    runner = FakeRunner(result=RETRY_DIAGNOSIS)

    finish_one(conn, runner, retry_orchestrator(runner))

    assert get_run(conn, run_id).status is RunStatus.DONE
    assert [a["outcome"] for a in actions(conn, run_id)] == ["calling", "executed"]
    (call,) = armed.calls()
    assert call["params"]["arguments"]["reason"].endswith(
        "Gemini 429 at 09:12; quota reset"
    )
    transport = FakeTransport()
    send_pass(conn, transport)
    (email,) = [e for e in transport.sent if "pipeline diagnosis" in e.subject]
    assert "pipeline diagnosis: RETRY" in email.subject
    assert "- task 767  GENERATE_FEATURE_IMAGE: RETRIED (now READY" in email.body
    assert "Nothing but the retries above was changed in PIC." in email.body


def test_without_the_switch_the_loop_stays_read_only(conn, pic, read_token):
    seed_failed_drive(conn)
    ((_, run_id),) = scan(conn)
    runner = FakeRunner(result=RETRY_DIAGNOSIS)

    finish_one(conn, runner, retry_orchestrator(runner))

    assert actions(conn, run_id) == [] and pic.requests == []


def test_the_email_names_every_outcome(conn):
    run_id = finished(
        conn,
        "pipeline_supervise",
        GOAL,
        RETRY_DIAGNOSIS,
        payload={"goal_key": GOAL, "trigger": "driver_failed"},
    )
    rows = [
        {"task_id": 1, "kind": "K", "outcome": "executed", "status": "READY",
         "attempts": 0, "replayed": True},
        {"task_id": 2, "kind": "K", "outcome": "refused_limit", "previous_run": 7,
         "previous_at": "2026-10-12T09:00:00+00:00"},
        {"task_id": 3, "kind": "K", "outcome": "refused_cap", "cap": 3},
        {"task_id": 4, "kind": "APPROVE_ARTICLE", "outcome": "refused_kind"},
        {"task_id": 5, "kind": "K", "outcome": "refused_pic", "detail": "conflict: x"},
        {"task_id": 6, "kind": "K", "outcome": "error", "detail": "HTTP 502"},
        {"task_id": 8, "kind": "K", "outcome": "calling"},
        {"task_id": 9, "kind": "K", "outcome": "executed", "status": "READY",
         "attempts": 0, "goal_key": GOAL, "pic_goal_key": OTHER},
    ]  # fmt: skip
    for row in rows:
        log.append(conn, run_id, EventType.TASK_ACTION, {"action": "retry", **row})
    transport = FakeTransport()

    send_pass(conn, transport)

    body = transport.sent[0].body
    assert "RETRIED (now READY, attempts 0), already READY: no new action" in body
    assert "already retried within 24h (run 7, 2026-10-12T09:00" in body
    assert "this run's cap of 3 was reached" in body
    assert "a publish-path task, operator only" in body
    assert "PIC refused: conflict: x" in body
    assert "call FAILED (HTTP 502)" in body
    assert "- task 8  K: call started, no outcome logged" in body
    assert f"PIC files this task under {OTHER}, not the diagnosed goal" in body
    assert body.count("not the diagnosed goal") == 1


def test_the_dry_run_prints_the_retries(conn, capsys, monkeypatch):
    run_id = finished(
        conn, "pipeline_supervise", GOAL, RETRY_DIAGNOSIS, payload={"goal_key": GOAL}
    )
    log.append(
        conn,
        run_id,
        EventType.TASK_ACTION,
        {
            "action": "retry",
            "task_id": 1,
            "kind": "K",
            "outcome": "refused_cap",
            "cap": 1,
        },
    )
    monkeypatch.setattr(notify, "connect", lambda dsn=None: _Borrowed(conn))
    notify.main(["--dry-run"])
    assert "this run's cap of 1 was reached" in capsys.readouterr().out


class _Borrowed:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, *exc):
        return False


# --- redaction ---------------------------------------------------------------------


def test_the_pipeline_token_is_redacted_from_the_log_and_email(conn, monkeypatch):
    monkeypatch.setattr(config, "PIC_MCP_PIPELINE_TOKEN", PIPELINE_TOKEN)
    assert PIPELINE_TOKEN in redact.secrets()
    run_id = finished(
        conn,
        "pipeline_supervise",
        GOAL,
        {**RETRY_DIAGNOSIS, "summary": f"token {PIPELINE_TOKEN} seen"},
        payload={"goal_key": GOAL},
    )
    log.append(
        conn,
        run_id,
        EventType.TASK_ACTION,
        {"task_id": 1, "outcome": "error", "detail": f"Bearer {PIPELINE_TOKEN}"},
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_events WHERE strpos(payload::text, %s) > 0",
            (PIPELINE_TOKEN,),
        )
        assert cur.fetchone()["n"] == 0
    transport = FakeTransport()
    send_pass(conn, transport)
    assert PIPELINE_TOKEN not in transport.sent[0].body
    assert "token [REDACTED] seen" in transport.sent[0].body


# --- durability on the loop's connection ---------------------------------------------


def test_the_claim_is_committed_before_the_call_even_inside_a_tick(migrated_dsn, armed):
    """The loop's connection is mid-transaction for the whole tick (its first
    statement is a bare SELECT), so a ``conn.transaction()`` there is only a
    savepoint. The claim must still be visible to another connection when
    the request reaches PIC, and survive the tick rolling back afterwards.

    These rows are committed for real, so the run is closed and marked
    emailed, and the task id is one no other test uses.
    """
    task_id = 2_000_000_000 + uuid.uuid4().int % 100_000_000
    subject = f"verify-84:{uuid.uuid4()}"
    tick = psycopg.connect(migrated_dsn, row_factory=dict_row)
    observer = psycopg.connect(migrated_dsn, row_factory=dict_row, autocommit=True)
    seen_at_call: list[list[str]] = []

    def visible():
        return [
            row["outcome"]
            for row in observer.execute(
                "SELECT payload->>'outcome' AS outcome FROM agent_events"
                " WHERE type = 'task_action' AND payload->>'task_id' = %s"
                " ORDER BY seq",
                (str(task_id),),
            ).fetchall()
        ]

    class Watching:
        def call_tool(self, name, arguments):
            seen_at_call.append(visible())
            return client(armed).call_tool(name, arguments)

    try:
        tick.execute("SELECT 1")  # what queue.active_runs leaves open
        with tick.transaction():  # a savepoint, as in the loop
            run_id = finished(tick, "smoke", subject, {"ok": True}, payload={})
            log.append(tick, run_id, EventType.EMAIL_SENT, {"suppressed": "test"})
        assert tick.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS

        (out,) = auto_retry.execute(
            tick, get_run(tick, run_id), [req(task_id=task_id)], client_factory=Watching
        )
        tick.rollback()  # the rest of the tick fails

        assert out["outcome"] == auto_retry.EXECUTED
        assert seen_at_call == [[auto_retry.CALLING]]
        assert visible() == [auto_retry.CALLING, auto_retry.EXECUTED]
        status = observer.execute(
            "SELECT status FROM agent_runs WHERE id = %s", (run_id,)
        ).fetchone()["status"]
        assert status == RunStatus.DONE.value  # the completion was committed too
    finally:
        tick.close()
        observer.close()


def test_a_task_id_past_int4_is_not_asked_for(armed):
    big = {**RETRY_DIAGNOSIS["tasks"][0], "task_id": 2**31}
    run, art = artifact_for({**RETRY_DIAGNOSIS, "tasks": [big]})
    assert auto_retry.requests(run, art) == ()
