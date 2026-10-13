"""The pipeline supervisor, phase 1 (#81): notice from the log, diagnose read-only.

What is pinned down: which drive sessions make an incident (parked rows on a
goal's latest session, or a goal whose counts stopped changing), that each
incident is diagnosed once, that the job can only read PIC (read token, an
allow list of read tools under dontAsk, PIC's write tools removed, no Bash),
and that the diagnosis is logged as an artifact, emailed, and measured.

The stalled goals here are seeded the way production makes them: a real drive
session (``driver.drive`` against a scripted PIC) writing to the test log.
"""

import json
from datetime import UTC, datetime, timedelta

import pytest

from orchestrator import config, jobs, notify, pic_mcp, queue, report, supervisor
from orchestrator.enums import EventType, RunStatus
from orchestrator.log import create_run, load_events, verify_replay
from orchestrator.queue import get_run
from tests.fakes import FakeRunner
from tests.test_adversary import chain_orchestrator, fake_run, finish_one
from tests.test_driver import FakePic, goal_row, run_drive, task_response
from tests.test_notify import FakeTransport, finished, send_pass
from tests.test_pic_mcp import TOKEN, needs_node, run_entrypoint

GOAL = "daily_series:2026-10-09"
OTHER = "daily_series:2026-10-10"
T0 = datetime(2026, 10, 9, 9, 0, tzinfo=UTC)

IMAGE_ERROR = (
    "POST /api/v1/admin/dashboard/articles/277/generate-image answered 502:"
    ' {"detail": "Gemini image API returned 403: permission_denied"}'
)

DIAGNOSIS = {
    "goal_key": GOAL,
    "verdict": "blocked_on_failure",
    "cause": "The image provider denies the project (403), so the image task"
    " fails on every attempt and holds the goal's capacity slot.",
    "evidence": [
        "explain_stuck_goal: task 767 GENERATE_FEATURE_IMAGE FAILED 3/3, 403",
        "pipeline_status: 1 FAILED, 9 PENDING behind it",
    ],
    "recommended_action": "operator_work_needed",
    "tasks": [
        {
            "task_id": 767,
            "kind": "GENERATE_FEATURE_IMAGE",
            "action": "operator",
            "why": "restore the Gemini key, then retry",
        }
    ],
    "confidence": "high",
    "summary": "Fix the image API access, then retry task 767.",
}


@pytest.fixture()
def token(monkeypatch):
    monkeypatch.setattr(config, "PIC_MCP_READ_TOKEN", TOKEN)
    return TOKEN


def session(run_id, at, result):
    return {
        "run_id": run_id,
        "created_at": at,
        "payload": {"outcome": "SUCCEEDED", "result": result},
    }


def new_shape(goals, parked=()):
    """A #62 session summary: per-goal counts, parked rows tagged by goal."""
    return {
        "outcome": "INCOMPLETE",
        "goals": [
            {"goal_key": key, "tasks": t, "parked": p, "waiting": w, "unfinished": u}
            for key, (t, p, w, u) in goals.items()
        ],
        "parked": list(parked),
    }


def parked_row(task_id, goal_key=GOAL, error=IMAGE_ERROR):
    return {
        "task_id": task_id,
        "kind": "GENERATE_FEATURE_IMAGE",
        "subject": "ARTICLE/277",
        "goal_key": goal_key,
        "error": error,
    }


def triggers(sessions, now, **kw):
    kw.setdefault("stall_days", 3)
    kw.setdefault("recent_hours", 48)
    kw.setdefault("lookback_days", 30)
    return supervisor.find_triggers(sessions, now=now, **kw)


# --- the triggers, from the log -------------------------------------------------


def test_a_session_that_parks_rows_is_a_driver_failure():
    sessions = [
        session(1, T0, new_shape({GOAL: (40, 1, 0, 9)}, [parked_row(767)])),
    ]

    (trig,) = triggers(sessions, T0 + timedelta(hours=2))

    assert trig.trigger == supervisor.TRIGGER_DRIVER_FAILED
    assert trig.goal_key == GOAL
    assert trig.failed_at == T0
    assert trig.trigger_key == f"drive:1:{GOAL}"
    assert trig.parked[0]["task_id"] == 767
    assert trig.snapshot == {"tasks": 40, "parked": 1, "waiting": 0, "unfinished": 9}


def test_parked_rows_are_attributed_to_their_own_goal():
    sessions = [
        session(
            1,
            T0,
            new_shape(
                {GOAL: (40, 1, 0, 9), OTHER: (10, 0, 1, 3)},
                [parked_row(767, GOAL)],
            ),
        ),
    ]

    found = triggers(sessions, T0 + timedelta(hours=1))

    assert [(t.goal_key, t.trigger) for t in found] == [
        (GOAL, supervisor.TRIGGER_DRIVER_FAILED)
    ]


def test_a_finished_goal_is_no_incident_even_after_earlier_failures():
    sessions = [
        session(1, T0, new_shape({GOAL: (40, 1, 0, 9)}, [parked_row(767)])),
        session(2, T0 + timedelta(hours=20), new_shape({GOAL: (40, 0, 0, 0)})),
    ]

    assert triggers(sessions, T0 + timedelta(days=10)) == []


def test_unchanged_counts_past_the_threshold_are_a_stall():
    waiting = new_shape({GOAL: (40, 0, 1, 9)})
    sessions = [
        session(1, T0, new_shape({GOAL: (40, 0, 1, 12)})),
        session(2, T0 + timedelta(days=1), waiting),
        session(3, T0 + timedelta(days=2), waiting),
        session(4, T0 + timedelta(days=3), waiting),
    ]
    since = T0 + timedelta(days=1)

    assert triggers(sessions, since + timedelta(days=2, hours=23)) == []
    (trig,) = triggers(sessions, since + timedelta(days=3, hours=1))

    assert trig.trigger == supervisor.TRIGGER_STALLED
    assert trig.stalled_since == since
    assert trig.failed_at == since + timedelta(days=3)
    assert trig.trigger_key == f"stall:{GOAL}:{since.isoformat()}"
    assert trig.source_run_id == 4


def test_old_failures_age_into_a_stall_when_the_driver_stops_running():
    # Production on 2026-10-12: the last drive was 2026-09-20 and left parked
    # rows. Too old to be "a session that just failed", so it is a stall.
    sessions = [session(1, T0, new_shape({GOAL: (40, 1, 0, 9)}, [parked_row(767)]))]

    (trig,) = triggers(sessions, T0 + timedelta(days=5))

    assert trig.trigger == supervisor.TRIGGER_STALLED
    assert trig.parked[0]["task_id"] == 767


def test_history_older_than_the_lookback_is_not_an_incident():
    sessions = [session(1, T0, new_shape({GOAL: (40, 1, 0, 9)}, [parked_row(767)]))]

    assert triggers(sessions, T0 + timedelta(days=31)) == []


def test_sessions_before_62_carry_one_goal_and_an_overall_outcome():
    old = {
        "outcome": "INCOMPLETE",
        "goal_key": GOAL,
        "unfinished": 23,
        "parked": [
            {
                "task_id": 767,
                "kind": "GENERATE_FEATURE_IMAGE",
                "subject": "ARTICLE/277",
                "error": IMAGE_ERROR,
            }
        ],
    }
    done = {"outcome": "COMPLETE", "goal_key": OTHER, "unfinished": 0, "parked": []}
    sessions = [session(1, T0, old), session(2, T0, done)]

    (trig,) = triggers(sessions, T0 + timedelta(hours=1))

    assert trig.goal_key == GOAL
    assert trig.snapshot == {"parked": 1, "unfinished": 23}


def test_a_session_that_failed_before_claiming_reports_no_goal():
    failed = {"outcome": "FAILED", "reason": "no_research", "tasks_done": 0}
    assert supervisor.goal_states(1, T0, {"result": failed}) == {}
    assert supervisor.goal_states(1, T0, {"result": None}) == {}
    assert supervisor.goal_states(1, T0, "garbage") == {}


def test_incidents_come_oldest_first():
    sessions = [
        session(1, T0, new_shape({GOAL: (4, 0, 1, 2)})),
        session(2, T0 + timedelta(days=1), new_shape({OTHER: (4, 0, 1, 2)})),
    ]

    found = triggers(sessions, T0 + timedelta(days=9))

    assert [t.goal_key for t in found] == [GOAL, OTHER]


# --- seeded through the real driver, enqueued once ------------------------------


def seed_failed_drive(conn):
    """A drive session that parks one image task: the production pattern."""
    pic = FakePic(
        next_responses=[
            task_response(766, goal_key=GOAL),
            task_response(767, kind="GENERATE_FEATURE_IMAGE", goal_key=GOAL),
            None,
            None,
        ],
        call_responses=[(200, {}), (502, {"detail": "Gemini image API returned 403"})],
        goal={
            GOAL: [
                goal_row(766, "DONE"),
                goal_row(767, "FAILED", kind="GENERATE_FEATURE_IMAGE", error="403"),
                goal_row(768, "PENDING"),
            ]
        },
        plan={"goal_key": GOAL, "created": 0},
    )
    return run_drive(conn, pic)


def scan(conn, now=None, dry_run=False, **kw):
    now = now or queue.db_now(conn)
    found = supervisor.find_triggers(
        supervisor.load_sessions(conn, now - timedelta(days=30)),
        now=now,
        stall_days=3,
        recent_hours=48,
        lookback_days=30,
    )
    return supervisor.enqueue(
        conn, found, stall_days=3, max_runs=kw.get("max_runs", 3), dry_run=dry_run
    )


def supervise_runs(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM agent_runs WHERE kind = %s ORDER BY id",
            (supervisor.RUN_KIND,),
        )
        return [get_run(conn, row["id"]) for row in cur.fetchall()]


def test_a_failed_drive_enqueues_one_diagnosis_and_only_one(conn):
    seed_failed_drive(conn)

    ((trig, run_id),) = scan(conn)

    run = get_run(conn, run_id)
    assert run.kind == supervisor.RUN_KIND
    assert run.subject == GOAL
    assert run.payload["trigger"] == supervisor.TRIGGER_DRIVER_FAILED
    assert run.payload["repo"] == "panama-in-context"
    assert run.payload["parked"][0]["task_id"] == 767
    assert run.payload["failed_at"] == trig.failed_at.isoformat()
    # Still open: the slot holds. Finished: the trigger key holds.
    assert scan(conn) == []
    queue.cancel_queued(conn, run_ids=[run_id], reason="test")
    assert scan(conn) == []
    assert len(supervise_runs(conn)) == 1


def test_a_seeded_goal_that_stops_moving_becomes_a_stall(conn):
    seed_failed_drive(conn)
    now = queue.db_now(conn) + timedelta(days=4)

    ((trig, run_id),) = scan(conn, now=now)

    assert trig.trigger == supervisor.TRIGGER_STALLED
    assert get_run(conn, run_id).payload["stall_days"] == 3


def test_a_dry_scan_enqueues_nothing(conn):
    seed_failed_drive(conn)

    ((trig, run_id),) = scan(conn, dry_run=True)

    assert run_id is None and trig.goal_key == GOAL
    assert supervise_runs(conn) == []


def test_the_scan_cap_bounds_one_pass(conn):
    found = [
        supervisor.Trigger(
            goal_key=f"daily_series:2026-10-0{n}",
            trigger=supervisor.TRIGGER_STALLED,
            trigger_key=f"stall:{n}",
            failed_at=T0,
        )
        for n in range(1, 5)
    ]

    done = supervisor.enqueue(conn, found, stall_days=3, max_runs=2)

    assert len(done) == 2


def test_a_manual_diagnosis_carries_the_goals_latest_evidence(conn):
    seed_failed_drive(conn)
    now = queue.db_now(conn)

    trig = supervisor.manual_trigger(conn, GOAL, now=now, lookback_days=30)
    unknown = supervisor.manual_trigger(
        conn, "daily_series:1999-01-01", now=now, lookback_days=30
    )

    assert trig.trigger == supervisor.TRIGGER_MANUAL
    assert trig.failed_at is None
    assert trig.parked[0]["task_id"] == 767
    assert unknown.parked == () and unknown.source_run_id is None


# --- the job: read-only, three ways ---------------------------------------------


def supervise_run(payload=None):
    return fake_run(
        supervisor.RUN_KIND,
        payload
        if payload is not None
        else {
            "goal_key": GOAL,
            "trigger": supervisor.TRIGGER_DRIVER_FAILED,
            "trigger_key": f"drive:1:{GOAL}",
            "failed_at": T0.isoformat(),
            "source_run_id": 1,
            "parked": [parked_row(767)],
            "snapshot": {"tasks": 40, "parked": 1, "waiting": 0, "unfinished": 9},
        },
        subject=GOAL,
    )


def test_the_job_gets_only_read_access_to_pic(token):
    spec = jobs.build_spec(supervise_run())

    assert spec.repo == ""  # no clone, no GitHub token, no Docker socket
    assert not spec.needs_github and not spec.needs_docker
    assert spec.mcp_servers == {"pic": pic_mcp.server()}
    assert spec.env["PIC_MCP_READ_TOKEN"] == TOKEN
    assert spec.env["AGENT_PERMISSION_MODE"] == "dontAsk"
    assert spec.env["AGENT_TOOLS"] == "Write"
    allowed = spec.env["AGENT_ALLOWED_TOOLS"].split()
    assert allowed == ["Write"] + [f"mcp__pic__{t}" for t in pic_mcp.READ_TOOLS]
    denied = spec.env["AGENT_DISALLOWED_TOOLS"].split()
    for write in (
        "retry_agent_task",
        "skip_agent_task",
        "request_publish",
        "submit_research",
    ):
        assert f"mcp__pic__{write}" in denied
        assert f"mcp__pic__{write}" not in allowed
    assert not set(allowed) & set(denied)
    assert spec.env["AGENT_MAX_TURNS"] == str(config.SUPERVISOR_MAX_TURNS)
    assert TOKEN not in spec.prompt


def test_the_read_list_matches_the_guide_and_misses_no_write_tool():
    for tool in pic_mcp.READ_TOOLS:
        assert pic_mcp.tool_id(tool) in pic_mcp.TOOLS_GUIDE
    for tool in pic_mcp.WRITE_TOOLS:
        assert pic_mcp.tool_id(tool) not in pic_mcp.TOOLS_GUIDE
    assert not set(pic_mcp.READ_TOOLS) & set(pic_mcp.WRITE_TOOLS)


def test_the_prompt_is_scoped_to_the_goal_and_its_evidence(token):
    prompt = jobs.build_spec(supervise_run()).prompt

    assert f"Goal: `{GOAL}`" in prompt
    assert "FAILED (parked) rows" in prompt
    assert '"task_id": 767' in prompt
    assert "mcp__pic__explain_stuck_goal" in prompt
    assert "/work/result.json" in prompt
    for action in supervisor.ACTIONS:
        assert f"`{action}`" in prompt


def test_the_prompt_names_each_trigger(token):
    stalled = supervise_run(
        {
            "goal_key": GOAL,
            "trigger": supervisor.TRIGGER_STALLED,
            "stalled_since": T0.isoformat(),
            "stall_days": 3,
        }
    )
    manual = supervise_run({"goal_key": GOAL, "trigger": supervisor.TRIGGER_MANUAL})

    assert (
        "no progress in the orchestrator's log since" in jobs.build_spec(stalled).prompt
    )
    assert "the operator asked" in jobs.build_spec(manual).prompt


def test_no_token_means_no_job(monkeypatch):
    monkeypatch.setattr(config, "PIC_MCP_READ_TOKEN", "")
    with pytest.raises(RuntimeError, match="ORCHESTRATOR_PIC_MCP_READ_TOKEN"):
        jobs.build_spec(supervise_run())


def test_the_supervisor_is_not_given_the_broad_pic_attach(token):
    # attach() would append the general addendum; the supervisor's own prompt
    # carries the tools guide once, and its server entry is the same one.
    assert supervisor.RUN_KIND not in jobs.PIC_MCP_KINDS
    prompt = jobs.build_spec(supervise_run()).prompt
    assert prompt.count("mcp__pic__check_claim") == 1


@needs_node
def test_the_entrypoint_turns_the_env_into_the_restricting_flags(tmp_path, token):
    env = {k: v for k, v in jobs.build_spec(supervise_run()).env.items()}

    result, _ = run_entrypoint(tmp_path, env=env)

    argv = result.stdout.splitlines()
    assert result.returncode == 0, result.stderr
    assert argv[argv.index("--permission-mode") + 1] == "dontAsk"
    assert argv[argv.index("--tools") + 1] == "Write"
    assert argv[argv.index("--allowedTools") + 1] == env["AGENT_ALLOWED_TOOLS"]
    assert argv[argv.index("--disallowedTools") + 1] == env["AGENT_DISALLOWED_TOOLS"]
    assert argv[argv.index("--max-turns") + 1] == env["AGENT_MAX_TURNS"]


@needs_node
def test_without_the_env_the_entrypoint_adds_no_tool_flags(tmp_path):
    result, _ = run_entrypoint(tmp_path)

    argv = result.stdout.splitlines()
    for flag in ("--tools", "--allowedTools", "--disallowedTools"):
        assert flag not in argv


# --- the diagnosis: validated, logged, emailed ------------------------------------


def test_a_diagnosis_is_bounded_and_filed_against_the_run_goal():
    found = supervisor.diagnosis(
        GOAL,
        {
            **DIAGNOSIS,
            "goal_key": "daily_series:somewhere-else",
            "recommended_action": "Operator work needed",
            "evidence": "one string",
            "confidence": "certain",
            "tasks": [*DIAGNOSIS["tasks"], "junk"],
        },
    )

    assert found["goal_key"] == GOAL
    assert found["recommended_action"] == "operator_work_needed"
    assert found["evidence"] == ["one string"]
    assert found["confidence"] == ""
    assert found["tasks"] == [
        {
            "task_id": 767,
            "kind": "GENERATE_FEATURE_IMAGE",
            "action": "operator_work_needed",
            "why": "restore the Gemini key, then retry",
        }
    ]


@pytest.mark.parametrize(
    ("raw", "action"),
    [
        ("retry", "retry"),
        ("SKIP", "skip"),
        ("none", "none"),
        ("investigate", "operator_work_needed"),
    ],
)
def test_actions_are_normalized(raw, action):
    assert supervisor.normalize_action(raw) == action


@pytest.mark.parametrize("result", [None, "text", {}, {"recommended_action": "merge"}])
def test_anything_without_a_known_action_is_no_diagnosis(result):
    assert supervisor.diagnosis(GOAL, result) is None
    assert supervisor.artifact(supervise_run(), result) is None


def test_the_chain_logs_the_diagnosis_and_parks_nothing():
    decision = jobs.followups(supervise_run(), DIAGNOSIS)

    assert decision.enqueue == () and decision.human_gate is None
    (art,) = decision.artifacts
    assert art["diagnosis"]["recommended_action"] == "operator_work_needed"
    assert art["trigger"] == supervisor.TRIGGER_DRIVER_FAILED
    assert art["failed_at"] == T0.isoformat()
    assert jobs.followups(supervise_run(), {"no": "action"}).artifacts == ()


def test_through_the_loop_a_diagnosis_lands_as_an_artifact_and_completes(conn, token):
    seed_failed_drive(conn)
    ((_, run_id),) = scan(conn)
    runner = FakeRunner(result=DIAGNOSIS)

    finish_one(conn, runner, chain_orchestrator(runner))

    assert get_run(conn, run_id).status is RunStatus.DONE
    (art,) = [e for e in load_events(conn, run_id) if e.type is EventType.ARTIFACT]
    assert art.payload["diagnosis"]["cause"].startswith("The image provider")
    assert art.payload["goal_key"] == GOAL
    assert verify_replay(conn, run_id)


def test_the_diagnosis_email_names_the_action_cause_and_evidence(conn):
    payload = {
        "repo": "panama-in-context",
        "goal_key": GOAL,
        "trigger": supervisor.TRIGGER_DRIVER_FAILED,
        "failed_at": T0.isoformat(),
    }
    finished(conn, supervisor.RUN_KIND, GOAL, DIAGNOSIS, payload=payload)
    transport = FakeTransport()

    send_pass(conn, transport)

    (email,) = transport.sent
    assert email.subject == (
        f"[managed-agents] panama-in-context {GOAL}:"
        " pipeline diagnosis: OPERATOR WORK NEEDED"
    )
    assert "The image provider denies the project" in email.body
    assert "- explain_stuck_goal: task 767" in email.body
    assert "- task 767  GENERATE_FEATURE_IMAGE: operator_work_needed" in email.body
    assert "confidence high" in email.body
    assert "nothing was changed in PIC" in email.body


def test_a_run_without_a_diagnosis_still_emails_once(conn):
    finished(conn, supervisor.RUN_KIND, GOAL, {"oops": 1}, payload={"goal_key": GOAL})
    transport = FakeTransport()

    send_pass(conn, transport)

    (email,) = transport.sent
    assert "finished without a structured result" in email.subject
    assert "without a valid diagnosis" in email.body


def test_the_supervisor_kind_is_emailed():
    assert supervisor.RUN_KIND in notify._KINDS


# --- the measure ----------------------------------------------------------------


def test_the_report_measures_failure_to_diagnosis(conn, token):
    seed_failed_drive(conn)
    ((_, run_id),) = scan(conn)
    runner = FakeRunner(result=DIAGNOSIS)
    finish_one(conn, runner, chain_orchestrator(runner))
    undiagnosed = create_run(
        conn,
        supervisor.RUN_KIND,
        OTHER,
        {"goal_key": OTHER, "trigger": supervisor.TRIGGER_MANUAL, "failed_at": None},
    )

    rows = supervisor.load_incidents(conn)
    text = report.render_supervisor(rows, generated=T0)

    assert [r["id"] for r in rows] == [run_id, undiagnosed]
    assert "Diagnoses: 1 of 2 run(s)." in text
    assert f"| {run_id} | `{GOAL}` | driver_failed |" in text
    assert "OPERATOR WORK NEEDED |  |  |" in text
    assert f"| {undiagnosed} | `{OTHER}` | manual | – | no diagnosis (QUEUED) |" in text
    assert "Operator's call | Agree?" in text


def test_the_report_says_so_when_there_is_nothing_yet():
    text = report.render_supervisor([], generated=T0)
    assert "No diagnoses in the log yet." in text


def test_parse_ts_is_tolerant():
    assert supervisor.parse_ts(None) is None
    assert supervisor.parse_ts("not a date") is None
    assert supervisor.parse_ts(T0) == T0
    assert supervisor.parse_ts(T0.isoformat()) == T0


# --- the command ----------------------------------------------------------------


@pytest.fixture()
def cli_db(conn, monkeypatch):
    """The command's ``connect()`` hands out the test connection."""

    class _Ctx:
        def __enter__(self):
            return conn

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(supervisor, "connect", lambda: _Ctx())
    return conn


def test_the_command_needs_the_token(monkeypatch, capsys):
    monkeypatch.setattr(config, "PIC_MCP_READ_TOKEN", "")
    assert supervisor.main([]) == 2


def test_the_command_scans_and_enqueues(cli_db, token, monkeypatch, capsys):
    monkeypatch.setattr(supervisor.auth, "skip_enqueue", lambda conn, what: False)
    seed_failed_drive(cli_db)

    assert supervisor.main(["--dry-run"]) == 0
    assert f"would enqueue: {GOAL} driver_failed" in capsys.readouterr().out
    assert supervisor.main([]) == 0
    assert f"{GOAL} driver_failed" in capsys.readouterr().out
    assert supervisor.main([]) == 0
    assert "no new incidents" in capsys.readouterr().out


def test_the_command_diagnoses_one_goal_on_request(cli_db, token, monkeypatch, capsys):
    monkeypatch.setattr(supervisor.auth, "skip_enqueue", lambda conn, what: False)

    assert supervisor.main(["--goal", GOAL, "--stall-days", "5"]) == 0

    (run,) = supervise_runs(cli_db)
    assert run.payload["trigger"] == supervisor.TRIGGER_MANUAL
    assert run.payload["stall_days"] == 5
    assert "failure seen n/a" in capsys.readouterr().out


def test_the_command_stands_down_while_dispatch_is_paused(cli_db, token, monkeypatch):
    monkeypatch.setattr(supervisor.auth, "skip_enqueue", lambda conn, what: True)
    seed_failed_drive(cli_db)

    assert supervisor.main([]) == 0
    assert supervise_runs(cli_db) == []


def test_the_report_command_prints_the_supervisor_table(conn, monkeypatch, capsys):
    class _Ctx:
        def __enter__(self):
            return conn

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(report, "connect", lambda: _Ctx())

    assert report.main(["supervisor"]) == 0
    assert "# Pipeline supervisor" in capsys.readouterr().out


def test_the_payload_round_trips_as_json():
    trig = supervisor.Trigger(
        goal_key=GOAL,
        trigger=supervisor.TRIGGER_STALLED,
        trigger_key="k",
        failed_at=T0,
        stalled_since=T0,
    )
    assert json.loads(json.dumps(trig.payload(3)))["stalled_since"] == T0.isoformat()


# --- malformed results (#81 review) ---------------------------------------------


@pytest.mark.parametrize("bad", [{"a": 1}, 5, None, True, "x"])
def test_tasks_that_are_not_a_list_are_dropped(bad):
    found = supervisor.diagnosis(GOAL, {"recommended_action": "retry", "tasks": bad})
    assert found["tasks"] == []


@pytest.mark.parametrize("bad", [{"a": 1}, 5, None, 1.5])
def test_evidence_that_is_not_a_list_or_string_is_dropped(bad):
    found = supervisor.diagnosis(GOAL, {"recommended_action": "retry", "evidence": bad})
    assert found["evidence"] == []


def test_per_task_fields_are_typed_and_bounded():
    tasks = [
        {"task_id": "767", "kind": "K" * 500, "why": {"nested": 1}},
        {"task_id": "abc"},
        {"task_id": True},
        {"task_id": float("inf")},
        {"task_id": [1]},
    ]
    found = supervisor.diagnosis(GOAL, {"recommended_action": "skip", "tasks": tasks})

    assert [t["task_id"] for t in found["tasks"]] == [767, None, None, None, None]
    assert len(found["tasks"][0]["kind"]) == 100
    assert found["tasks"][0]["action"] == "unknown"
    assert found["tasks"][0]["why"] == "{'nested': 1}"


def test_a_malformed_result_still_completes_through_the_loop(conn, token):
    seed_failed_drive(conn)
    ((_, run_id),) = scan(conn)
    runner = FakeRunner(
        result={"recommended_action": "retry", "tasks": {"a": 1}, "evidence": 5}
    )

    finish_one(conn, runner, chain_orchestrator(runner))

    (art,) = [e for e in load_events(conn, run_id) if e.type is EventType.ARTIFACT]
    assert art.payload["diagnosis"]["tasks"] == []


def emailed(conn, run_id):
    return [e for e in load_events(conn, run_id) if e.type is EventType.EMAIL_SENT]


def test_a_malformed_result_is_emailed_and_does_not_block_the_pass(conn):
    bad = finished(
        conn,
        supervisor.RUN_KIND,
        GOAL,
        {"recommended_action": "retry", "tasks": {"a": 1}, "evidence": {"b": 2}},
        payload={"goal_key": GOAL},
    )
    later = finished(
        conn, supervisor.RUN_KIND, OTHER, DIAGNOSIS, payload={"goal_key": OTHER}
    )
    transport = FakeTransport()

    send_pass(conn, transport)

    assert len(transport.sent) == 2
    assert "pipeline diagnosis: RETRY" in transport.sent[0].subject
    assert emailed(conn, bad) and emailed(conn, later)


def test_a_body_that_cannot_render_falls_back_and_the_pass_goes_on(conn, monkeypatch):
    first = finished(
        conn, supervisor.RUN_KIND, GOAL, DIAGNOSIS, payload={"goal_key": GOAL}
    )
    second = finished(
        conn, supervisor.RUN_KIND, OTHER, DIAGNOSIS, payload={"goal_key": OTHER}
    )
    real = notify._supervisor_body

    def flaky(cand, result, *rest):
        if cand.run_id == first:
            raise KeyError("boom")
        return real(cand, result, *rest)

    monkeypatch.setattr(notify, "_supervisor_body", flaky)
    transport = FakeTransport()

    send_pass(conn, transport)

    broken, fine = transport.sent
    assert "could not render this run's outcome" in broken.body
    assert f"run {first}" in broken.body
    assert "The image provider" in fine.body
    assert emailed(conn, first) and emailed(conn, second)


def test_a_headline_that_cannot_render_still_emails(conn, monkeypatch):
    run_id = finished(
        conn, supervisor.RUN_KIND, GOAL, DIAGNOSIS, payload={"goal_key": GOAL}
    )

    def broken(*a, **kw):
        raise TypeError("boom")

    monkeypatch.setattr(notify, "_headline", broken)
    transport = FakeTransport()

    send_pass(conn, transport)

    (email,) = transport.sent
    assert "could not be rendered" in email.subject
    assert emailed(conn, run_id)


def test_a_diagnosed_driver_failure_does_not_come_back_as_a_stall(conn):
    seed_failed_drive(conn)
    assert len(scan(conn)) == 1  # driver_failed, enqueued
    later = queue.db_now(conn) + timedelta(days=4)

    assert scan(conn, now=later) == []
