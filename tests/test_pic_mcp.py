"""Read-only PIC MCP access for sandboxes working on PIC (#80).

What is pinned down: which jobs get the server (PIC jobs of the listed kinds,
only when a token is configured), that the token travels as an env var and
never in a file on the host, that the entrypoint resolves it inside the
container, and that it never reaches agent_events or an email.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from orchestrator import config, jobs, pic_mcp, redact
from orchestrator.enums import EventType
from orchestrator.log import append, create_run, load_events, truncate_payload
from orchestrator.queue import get_run
from orchestrator.sandbox import MCP_CONFIG_FILENAME, PROMPT_FILENAME, JobSpec
from orchestrator.sources import sentry
from tests.fakes import FakeCommands
from tests.test_adversary import CHAIN_PAYLOAD, fake_run
from tests.test_notify import FakeTransport, finished, send_pass
from tests.test_sandbox import make_run, make_runner, roots  # noqa: F401
from tests.test_triage import PAYLOAD, detail  # noqa: F401

TOKEN = "pic_mcp_read_0123456789abcdef"
ENTRYPOINT = (
    Path(__file__).resolve().parent.parent / "docker" / "sandbox" / "entrypoint.sh"
)


@pytest.fixture()
def token(monkeypatch):
    monkeypatch.setattr(config, "PIC_MCP_READ_TOKEN", TOKEN)
    return TOKEN


# --- which jobs get it --------------------------------------------------------


def test_a_pic_job_gets_the_server_the_env_var_and_the_guide(token):
    spec = pic_mcp.attach(JobSpec(prompt="fix it", repo="panama-in-context"))

    assert spec.env == {"PIC_MCP_READ_TOKEN": TOKEN}
    assert spec.mcp_servers == {
        "pic": {
            "type": "http",
            "url": "https://api.panamaincontext.com/mcp",
            "headers": {"Authorization": "Bearer ${PIC_MCP_READ_TOKEN}"},
        }
    }
    assert spec.prompt.startswith("fix it")
    assert "mcp__pic__pipeline_status" in spec.prompt
    assert "mcp__pic__get_article" in spec.prompt
    assert TOKEN not in spec.prompt


def test_attach_keeps_the_jobs_own_env_and_servers(token):
    spec = pic_mcp.attach(
        JobSpec(
            prompt="p",
            repo="panama-in-context",
            env={"A": "1"},
            mcp_servers={"other": {"type": "http", "url": "u"}},
        )
    )
    assert spec.env == {"A": "1", "PIC_MCP_READ_TOKEN": TOKEN}
    assert set(spec.mcp_servers) == {"other", "pic"}


def test_another_repo_gets_nothing(token):
    spec = JobSpec(prompt="p", repo="feliu-dev")
    assert pic_mcp.attach(spec) is spec


def test_no_token_configured_means_no_mcp_at_all(monkeypatch):
    monkeypatch.setattr(config, "PIC_MCP_READ_TOKEN", "")
    spec = JobSpec(prompt="p", repo="panama-in-context")
    assert pic_mcp.attach(spec) is spec


def test_a_pic_triage_run_is_briefed_on_the_tools(conn, detail, token):  # noqa: F811
    run = get_run(conn, create_run(conn, sentry.RUN_KIND, "sentry:M-1", PAYLOAD))
    spec = jobs.build_spec(run)
    assert "pic" in spec.mcp_servers
    assert spec.env["PIC_MCP_READ_TOKEN"] == TOKEN
    assert "mcp__pic__pipeline_status" in spec.prompt


def test_a_non_pic_triage_run_gets_no_token(conn, detail, token):  # noqa: F811
    payload = dict(PAYLOAD, repo="feliu-dev")
    run = get_run(conn, create_run(conn, sentry.RUN_KIND, "sentry:M-2", payload))
    spec = jobs.build_spec(run)
    assert spec.mcp_servers == {}
    assert "PIC_MCP_READ_TOKEN" not in spec.env
    assert "mcp__pic__" not in spec.prompt


def test_a_pic_revision_gets_it_and_a_feliu_one_does_not(token):
    payload = dict(CHAIN_PAYLOAD, refutation="the test passes on main")
    pic = jobs.build_spec(fake_run(jobs.REVISION_KIND, payload))
    feliu = jobs.build_spec(
        fake_run(jobs.REVISION_KIND, dict(payload, repo="feliu-dev"))
    )
    assert "pic" in pic.mcp_servers
    assert feliu.mcp_servers == {} and feliu.env == {}


def test_the_kinds_that_reason_about_production_get_it():
    assert {
        sentry.RUN_KIND,
        jobs.REVIEW_KIND,
        jobs.REVISION_KIND,
        jobs.PR_REVISION_KIND,
        jobs.RESEARCH_WRITE_KIND,
        jobs.RUBRIC_VERIFY_KIND,
        jobs.RESEARCH_REVISE_KIND,
    } == jobs.PIC_MCP_KINDS
    assert jobs.DREAM_KIND not in jobs.PIC_MCP_KINDS
    assert "smoke" not in jobs.PIC_MCP_KINDS


def test_a_kind_outside_the_set_gets_nothing_even_on_pic(monkeypatch, token):
    monkeypatch.setitem(
        jobs.REGISTRY,
        "other",
        lambda run: JobSpec(prompt="p", repo="panama-in-context"),
    )
    spec = jobs.build_spec(fake_run("other", {}))
    assert spec.mcp_servers == {} and spec.env == {}


# --- the runner: env var in, placeholder on disk ------------------------------


def test_the_job_dir_holds_a_placeholder_and_the_token_rides_in_env(
    roots,  # noqa: F811
    token,
):
    commands = FakeCommands()
    spec = JobSpec(
        prompt="p",
        env={"PIC_MCP_READ_TOKEN": TOKEN},
        mcp_servers={"pic": pic_mcp.server()},
    )
    runner = make_runner(roots, commands, spec=spec)
    run = make_run()
    runner.start(run)

    job_dir = runner.job_path(run)
    written = json.loads((job_dir / MCP_CONFIG_FILENAME).read_text())
    assert written == {"mcpServers": {"pic": pic_mcp.server()}}
    for path in job_dir.iterdir():
        assert TOKEN not in path.read_text()
    argv = commands.commands("run")[0]
    assert f"PIC_MCP_READ_TOKEN={TOKEN}" in argv


def test_a_job_without_servers_gets_no_config_file(roots):  # noqa: F811
    runner = make_runner(roots, FakeCommands())
    run = make_run()
    runner.start(run)
    assert (runner.job_path(run) / PROMPT_FILENAME).exists()
    assert not (runner.job_path(run) / MCP_CONFIG_FILENAME).exists()


# --- redaction ----------------------------------------------------------------


def test_redact_walks_nested_values_and_keys(token):
    payload = {
        "message": {"content": [{"type": "text", "text": f"token is {TOKEN}!"}]},
        TOKEN: ("a", TOKEN, 3),
        "n": 1,
    }
    assert redact.redact(payload) == {
        "message": {"content": [{"type": "text", "text": "token is [REDACTED]!"}]},
        "[REDACTED]": ("a", "[REDACTED]", 3),
        "n": 1,
    }


def test_a_short_or_empty_setting_redacts_nothing(monkeypatch):
    monkeypatch.setattr(config, "PIC_MCP_READ_TOKEN", "abc")
    assert redact.secrets() == ()
    assert redact.redact({"x": "abc"}) == {"x": "abc"}


def test_no_event_in_the_log_carries_the_token(conn, token):
    run_id = create_run(conn, sentry.RUN_KIND, "sentry:R-1", {"note": TOKEN})
    append(conn, run_id, EventType.RUN_LEASED, worker_id="w", attempts=1)
    append(
        conn,
        run_id,
        EventType.CLAUDE_EVENT,
        {"type": "user", "tool_result": f"PIC_MCP_READ_TOKEN={TOKEN}"},
    )
    # Big enough to be truncated: the preview must not carry it either.
    append(
        conn,
        run_id,
        EventType.CLAUDE_EVENT,
        {"type": "assistant", "text": TOKEN + "x" * 40000},
    )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM agent_events"
            " WHERE run_id = %s AND payload::text LIKE %s",
            (run_id, f"%{TOKEN}%"),
        )
        assert cur.fetchone()["n"] == 0
    texts = [json.dumps(e.payload) for e in load_events(conn, run_id)]
    assert any("[REDACTED]" in t for t in texts)


def test_truncation_alone_would_have_kept_a_prefix():
    """Why redaction runs before truncation: the preview keeps the head."""
    assert TOKEN in json.dumps(truncate_payload({"text": TOKEN + "x" * 40000}))


def test_an_email_never_carries_the_token(conn, monkeypatch):
    """Even text logged before the token was configured (or rotated in) is
    scrubbed on the way out."""
    finished(
        conn,
        sentry.RUN_KIND,
        "sentry:R-2",
        {"outcome": "NEEDS_HUMAN", "reason": f"the MCP token {TOKEN} expired"},
    )
    monkeypatch.setattr(config, "PIC_MCP_READ_TOKEN", TOKEN)
    transport = FakeTransport()

    assert send_pass(conn, transport) == 1
    email = transport.sent[0]
    assert TOKEN not in email.body and TOKEN not in email.subject
    assert "the MCP token [REDACTED] expired" in email.body


# --- the entrypoint resolves the placeholder inside the container --------------

needs_node = pytest.mark.skipif(
    shutil.which("node") is None or shutil.which("bash") is None,
    reason="the entrypoint resolves MCP config with node",
)


def run_entrypoint(tmp_path, *, config_text=None, env=None):
    home = tmp_path / "home"
    (home / ".claude").mkdir(parents=True)
    (home / ".claude" / ".credentials.json").write_text("{}")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "claude"
    # Prints its argv, then the resolved config it was handed, then removes
    # it (in a real sandbox it goes with the container).
    fake.write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\\n" "$@"\n'
        "while [ $# -gt 0 ]; do\n"
        '  if [ "$1" = --mcp-config ]; then echo "CONFIG:$(cat "$2")"; rm -f "$2"; fi\n'
        "  shift\n"
        "done\n"
    )
    fake.chmod(0o755)
    source = tmp_path / "work" / "mcp.json"
    source.parent.mkdir()
    if config_text is not None:
        source.write_text(config_text)
    full_env = {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "HOME": str(home),
        "AGENT_MCP_CONFIG": str(source),
        **(env or {}),
    }
    result = subprocess.run(
        ["bash", str(ENTRYPOINT), "do it"],
        cwd=tmp_path,
        env=full_env,
        capture_output=True,
        text=True,
        check=False,
    )
    return result, source


@needs_node
def test_the_entrypoint_passes_a_resolved_config(tmp_path):
    text = json.dumps({"mcpServers": {"pic": pic_mcp.server()}})
    result, source = run_entrypoint(
        tmp_path, config_text=text, env={"PIC_MCP_READ_TOKEN": TOKEN}
    )

    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert "--strict-mcp-config" in lines
    config_line = next(line for line in lines if line.startswith("CONFIG:"))
    resolved = json.loads(config_line.removeprefix("CONFIG:"))
    assert resolved["mcpServers"]["pic"]["headers"] == {
        "Authorization": f"Bearer {TOKEN}"
    }
    # The host-side file is untouched, and the resolved one is not under it.
    assert source.read_text() == text
    path = lines[lines.index("--mcp-config") + 1]
    assert not path.startswith(str(tmp_path / "work"))


@needs_node
def test_an_unset_variable_skips_mcp_with_a_warning(tmp_path):
    text = json.dumps({"mcpServers": {"pic": pic_mcp.server()}})
    result, _ = run_entrypoint(tmp_path, config_text=text)

    assert result.returncode == 0, result.stderr
    assert "--mcp-config" not in result.stdout.splitlines()
    assert "unset: PIC_MCP_READ_TOKEN" in result.stderr


@needs_node
def test_a_broken_config_skips_mcp_without_echoing_it(tmp_path):
    result, _ = run_entrypoint(
        tmp_path,
        config_text='{"x": "${PIC_MCP_READ_TOKEN}"',
        env={"PIC_MCP_READ_TOKEN": TOKEN},
    )
    assert result.returncode == 0, result.stderr
    assert "--mcp-config" not in result.stdout.splitlines()
    assert TOKEN not in result.stderr


@needs_node
def test_no_config_file_means_no_mcp_flags(tmp_path):
    result, _ = run_entrypoint(tmp_path)
    assert result.returncode == 0, result.stderr
    assert "--mcp-config" not in result.stdout.splitlines()
    assert "--strict-mcp-config" not in result.stdout.splitlines()
