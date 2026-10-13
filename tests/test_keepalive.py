"""scripts/orch-loop-keepalive.sh: start the loop only when no real loop runs (#72).

The script's check is ``pgrep -f``, which sees every process on the machine, so
each test plants its own processes and asserts on whether the script started
the (fake) loop it points at.
"""

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "orch-loop-keepalive.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("pgrep") is None, reason="keepalive's check needs pgrep"
)


@pytest.fixture
def host(tmp_path):
    """A fake /srv/orchestrator whose `.venv/bin/python` only records that it ran."""
    orch = tmp_path / "orch"
    venv_bin = orch / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    marker = tmp_path / "started"
    fake = venv_bin / "python"
    fake.write_text(f'#!/bin/sh\necho "$@" > "{marker}"\n')
    fake.chmod(0o755)
    env_file = tmp_path / "orchestrator.env"
    env_file.write_text("")
    logs = tmp_path / "logs"
    logs.mkdir()
    env = {
        **os.environ,
        "ORCH_DIR": str(orch),
        "ORCH_ENV_FILE": str(env_file),
        "ORCH_LOG_DIR": str(logs),
    }
    return {"env": env, "marker": marker, "logs": logs}


@pytest.fixture
def spawn():
    """Start background processes; kill them all after the test."""
    procs = []

    def _spawn(args, **kwargs):
        proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, **kwargs)
        procs.append(proc)
        time.sleep(0.2)  # let it exec, so pgrep sees its final cmdline
        assert proc.poll() is None, f"{args} exited early"
        return proc

    yield _spawn
    for proc in procs:
        proc.kill()
        proc.wait()


@pytest.fixture
def fake_loop_module(tmp_path):
    """A directory where `python -m orchestrator.main` just sleeps."""
    pkg = tmp_path / "fakeloop" / "orchestrator"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "main.py").write_text("import time\ntime.sleep(30)\n")
    return pkg.parent


def run_keepalive(host):
    subprocess.run(["bash", str(SCRIPT)], env=host["env"], check=True, timeout=10)


def started(host):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if host["marker"].exists():
            return True
        time.sleep(0.05)
    return False


def test_starts_the_loop_when_none_runs(host):
    run_keepalive(host)

    assert started(host)
    assert host["marker"].read_text().strip() == "-m orchestrator.main"
    assert (
        "keepalive: started orchestrator.main pid"
        in (host["logs"] / "keepalive.log").read_text()
    )


def test_a_command_that_mentions_the_module_does_not_block_a_start(host, spawn):
    # The 2026-10-05 trap: a wait loop whose text names the module.
    spawn(["bash", "-c", 'sleep 30; pgrep -f "orchestrator.main"'])
    spawn(["bash", "-c", "sleep 30 # python -m orchestrator.main"])

    run_keepalive(host)

    assert started(host)


def test_a_loop_started_by_absolute_path_blocks_a_second_one(
    host, spawn, fake_loop_module
):
    spawn([sys.executable, "-m", "orchestrator.main"], cwd=fake_loop_module)

    run_keepalive(host)

    assert not started(host)


def test_a_loop_started_by_keepalive_blocks_a_second_one(host, spawn, fake_loop_module):
    # The exact shape keepalive itself starts: relative `.venv/bin/python`.
    venv_bin = fake_loop_module / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    (venv_bin / "python").symlink_to(sys.executable)
    spawn([".venv/bin/python", "-m", "orchestrator.main"], cwd=fake_loop_module)

    run_keepalive(host)

    assert not started(host)
