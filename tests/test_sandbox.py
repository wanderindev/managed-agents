"""DockerRunner: the shape of the commands it issues and what it makes of the
results. The docker and git CLIs are faked, because the point under test is the
runner's decisions, not Docker's behaviour.
"""

import json
import subprocess

import pytest

from orchestrator.enums import Outcome, RunStatus
from orchestrator.queue import Run
from orchestrator.sandbox import PROMPT_FILENAME, RESULT_FILENAME, DockerRunner, JobSpec
from tests.fakes import FakeCommands


def make_run(run_id=1, attempts=1, kind="smoke"):
    return Run(
        id=run_id,
        kind=kind,
        subject=f"{kind}:{run_id}",
        status=RunStatus.LEASED,
        attempts=attempts,
        worker_id="orchestrator-test",
        lease_expires_at=None,
    )


@pytest.fixture()
def roots(tmp_path):
    creds = tmp_path / "credentials.json"
    creds.write_text("{}")
    return {
        "repos_root": tmp_path / "repos",
        "worktrees_root": tmp_path / "worktrees",
        "jobs_root": tmp_path / "jobs",
        "credentials": creds,
    }


def make_runner(roots, commands, spec=None, **kwargs):
    spec = spec or JobSpec(prompt="do the thing")
    return DockerRunner(
        lambda run: spec, run_command=commands, timeout_seconds=900, **roots, **kwargs
    )


# --- start -------------------------------------------------------------------


def test_start_returns_an_attempt_scoped_handle(roots):
    commands = FakeCommands()
    runner = make_runner(roots, commands)
    assert runner.start(make_run(7, attempts=2)) == "ma-run-7-2"


def test_start_writes_the_prompt_to_the_job_dir_not_the_repo(roots):
    """A job must not be able to commit its own instructions."""
    commands = FakeCommands()
    runner = make_runner(roots, commands, spec=JobSpec(prompt="fix the bug"))
    run = make_run()

    runner.start(run)

    assert (runner.job_path(run) / PROMPT_FILENAME).read_text() == "fix the bug"
    assert not (runner.workspace_path(run) / PROMPT_FILENAME).exists()


def test_start_does_not_pass_rm_so_logs_survive(roots):
    """The container's logs are the transcript; --rm would discard them."""
    commands = FakeCommands()
    make_runner(roots, commands).start(make_run())

    argv = commands.commands("run")[0]
    assert "--rm" not in argv
    assert "--detach" in argv


def test_workspace_is_mounted_at_the_same_path_as_on_the_host(roots):
    """The host daemon resolves bind mounts against ITS filesystem (#43): a
    container-only /workspace made repo tooling that mounts `git rev-parse
    --show-toplevel` into a sibling container silently mount empty dirs."""
    commands = FakeCommands()
    runner = make_runner(roots, commands)
    run = make_run()
    runner.start(run)

    argv = commands.commands("run")[0]
    workspace = str(runner.workspace_path(run))
    assert f"{workspace}:{workspace}" in argv
    assert not any(v.endswith(":/workspace") for v in argv)
    assert argv[argv.index("--workdir") + 1] == workspace


def test_prompts_reference_the_real_workspace_path(roots):
    """Prompts are written against the canonical /workspace name; the runner
    rewrites them to the per-attempt mount path in the one place every prompt
    passes through (#43)."""
    commands = FakeCommands()
    spec = JobSpec(
        prompt="The repo is at /workspace. Stay inside /workspace and /work."
    )
    runner = make_runner(roots, commands, spec=spec)
    run = make_run(9, attempts=2)
    runner.start(run)

    prompt = (runner.job_path(run) / PROMPT_FILENAME).read_text()
    workspace = str(runner.workspace_path(run))
    assert "ma-run-9-2" in workspace
    assert f"The repo is at {workspace}." in prompt
    assert f"Stay inside {workspace} and /work." in prompt
    assert "/workspace" not in prompt.replace(workspace, "")


def test_start_mounts_the_credential_and_bounds_the_clock(roots):
    commands = FakeCommands()
    runner = make_runner(roots, commands)
    runner.start(make_run())

    argv = " ".join(commands.commands("run")[0])
    assert "credentials.json:/home/agent/.claude/.credentials.json" in argv
    assert "AGENT_TIMEOUT_SECONDS=900" in argv
    assert "--memory" in argv


def test_start_refuses_without_a_credential(roots):
    roots["credentials"].unlink()
    with pytest.raises(RuntimeError, match="no Claude credential"):
        make_runner(roots, FakeCommands()).start(make_run())


def test_the_docker_socket_is_opt_in(roots):
    commands = FakeCommands()
    make_runner(roots, commands, with_docker=False).start(make_run())
    assert "/var/run/docker.sock" not in " ".join(commands.commands("run")[0])

    commands = FakeCommands()
    make_runner(roots, commands, with_docker=True).start(make_run(2))
    assert "/var/run/docker.sock" in " ".join(commands.commands("run")[0])


def test_a_job_that_needs_docker_gets_the_socket_and_its_group(roots, tmp_path):
    """Per-job opt-in (#7): a triage run gets the socket for its testcontainers
    suite without flipping the runner-wide default. The group flag is #26: the
    socket is root:docker mode 660 and the agent user is uid 1000, so a mount
    without the group is present but unusable."""
    import os

    socket = tmp_path / "docker.sock"
    socket.write_text("")
    commands = FakeCommands()
    spec = JobSpec(prompt="triage", needs_docker=True)
    make_runner(
        roots, commands, spec=spec, with_docker=False, docker_socket=str(socket)
    ).start(make_run())

    argv = commands.commands("run")[0]
    assert f"{socket}:{socket}" in argv
    gid = argv[argv.index("--group-add") + 1]
    assert gid == str(os.stat(socket).st_gid)


def test_a_missing_socket_skips_the_group_but_keeps_the_mount(roots, tmp_path):
    """A daemon that is down should fail as "docker run failed", not as a
    stat crash inside the orchestrator."""
    commands = FakeCommands()
    spec = JobSpec(prompt="triage", needs_docker=True)
    make_runner(
        roots,
        commands,
        spec=spec,
        docker_socket=str(tmp_path / "nope.sock"),
    ).start(make_run())

    argv = commands.commands("run")[0]
    assert "--group-add" not in argv
    assert any("nope.sock" in a for a in argv)


def test_a_failed_docker_run_cleans_up_and_raises(roots):
    commands = FakeCommands({("run",): (125, "", "port is already allocated")})
    runner = make_runner(roots, commands)
    run = make_run()

    with pytest.raises(RuntimeError, match="port is already allocated"):
        runner.start(run)

    # A retry must get a clean workspace rather than inherit a half-built one.
    assert not runner.workspace_path(run).exists()
    assert not runner.job_path(run).exists()


def test_start_clones_locally_and_points_origin_at_github(roots):
    """A standalone clone (#33), not a worktree: a worktree's .git links into
    the parent's .git on the host, which is never mounted into the sandbox, so
    git was dead in /workspace."""
    (roots["repos_root"] / "feliu-dev").mkdir(parents=True)
    commands = FakeCommands()
    runner = make_runner(
        roots, commands, spec=JobSpec(prompt="p", repo="feliu-dev", branch="agent/x")
    )
    run = make_run()

    runner.start(run)

    clone = commands.commands("clone")[0]
    assert "--local" in clone
    assert str(roots["repos_root"] / "feliu-dev") in clone
    assert str(runner.workspace_path(run)) in clone
    set_url = commands.commands("remote", "set-url")[0]
    assert "https://github.com/wanderindev/feliu-dev.git" in set_url
    assert not commands.commands("worktree")


def test_a_fresh_branch_is_cut_from_origins_main(roots):
    """The parent clone is a static template that only goes staler (#30); the
    branch must be based on GitHub's main, freshly fetched."""
    (roots["repos_root"] / "feliu-dev").mkdir(parents=True)
    commands = FakeCommands()
    runner = make_runner(
        roots, commands, spec=JobSpec(prompt="p", repo="feliu-dev", branch="agent/x")
    )

    runner.start(make_run())

    fetch = commands.commands("fetch")[0]
    assert "+refs/heads/main:refs/remotes/origin/main" in fetch
    checkout = commands.commands("checkout", "-B")[0]
    assert "agent/x" in checkout and "origin/main" in checkout


def test_local_main_is_forced_to_origins_main(roots):
    """The prompts compare against local main (`git diff main...HEAD`), so it
    has to match origin's, not the parent template's."""
    (roots["repos_root"] / "feliu-dev").mkdir(parents=True)
    commands = FakeCommands()
    runner = make_runner(roots, commands, spec=JobSpec(prompt="p", repo="feliu-dev"))

    runner.start(make_run())

    branch = commands.commands("branch", "-f")[0]
    assert "main" in branch and "origin/main" in branch


def test_start_fails_clearly_when_the_clone_is_missing(roots):
    runner = make_runner(
        roots, FakeCommands(), spec=JobSpec(prompt="p", repo="not-cloned")
    )
    with pytest.raises(RuntimeError, match="no clone at"):
        runner.start(make_run())


# --- is_alive ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("stdout", "returncode", "expected"),
    [
        ("true\n", 0, True),
        ("false\n", 0, False),
        ("", 1, False),  # unknown container: gone, not running
    ],
)
def test_is_alive(roots, stdout, returncode, expected):
    commands = FakeCommands({("inspect",): (returncode, stdout, "")})
    assert make_runner(roots, commands).is_alive("ma-run-1-1") is expected


# --- finish ------------------------------------------------------------------


def _logs(*objects):
    return "\n".join(json.dumps(o) for o in objects) + "\n"


def test_finish_parses_the_transcript_and_reports_success(roots):
    commands = FakeCommands(
        {
            ("inspect",): (0, "0\n", ""),
            ("logs",): (0, _logs({"type": "system"}, {"type": "result"}), ""),
        }
    )
    result = make_runner(roots, commands).finish(make_run(), "ma-run-1-1")

    assert result.outcome is Outcome.SUCCEEDED
    assert result.exit_code == 0
    assert [e["type"] for e in result.events] == ["system", "result"]


def test_finish_reports_failure_on_a_nonzero_exit(roots):
    commands = FakeCommands(
        {("inspect",): (0, "1\n", ""), ("logs",): (0, "", "traceback")}
    )
    result = make_runner(roots, commands).finish(make_run(), "ma-run-1-1")

    assert result.outcome is Outcome.FAILED
    assert result.exit_code == 1
    # stderr is kept separately: a CLI that dies before emitting anything
    # produces no events at all, and that case still has to be diagnosable.
    assert result.stderr == "traceback"


def test_finish_names_a_timeout_rather_than_calling_it_a_random_failure(roots):
    commands = FakeCommands({("inspect",): (0, "124\n", ""), ("logs",): (0, "", "")})
    result = make_runner(roots, commands).finish(make_run(), "ma-run-1-1")

    assert result.outcome is Outcome.FAILED
    assert "exceeded its 900s wall clock" in result.stderr


def test_finish_reports_gone_when_the_container_has_vanished(roots):
    """A host reboot or a manual docker rm. Retryable loss, not a verdict."""
    commands = FakeCommands({("inspect",): (1, "", "No such object")})
    result = make_runner(roots, commands).finish(make_run(), "ma-run-1-1")

    assert result.outcome is Outcome.GONE
    assert result.events == []


def test_finish_survives_a_non_json_log_line(roots):
    """A stray warning on stdout must not lose the rest of the transcript."""
    commands = FakeCommands(
        {
            ("inspect",): (0, "0\n", ""),
            ("logs",): (0, "npm warn something\n" + _logs({"type": "result"}), ""),
        }
    )
    result = make_runner(roots, commands).finish(make_run(), "ma-run-1-1")

    assert [e["type"] for e in result.events] == ["result"]


def test_finish_reads_the_structured_result(roots):
    commands = FakeCommands({("inspect",): (0, "0\n", ""), ("logs",): (0, "", "")})
    runner = make_runner(roots, commands)
    run = make_run()
    runner.start(run)
    (runner.job_path(run) / RESULT_FILENAME).write_text('{"outcome": "FIX", "pr": 42}')

    result = runner.finish(run, "ma-run-1-1")

    assert result.result == {"outcome": "FIX", "pr": 42}


def test_finish_tolerates_an_unparseable_result_file(roots):
    commands = FakeCommands({("inspect",): (0, "0\n", ""), ("logs",): (0, "", "")})
    runner = make_runner(roots, commands)
    run = make_run()
    runner.start(run)
    (runner.job_path(run) / RESULT_FILENAME).write_text("not json{")

    assert runner.finish(run, "ma-run-1-1").result is None


def test_finish_removes_the_container_only_after_reading_it(roots):
    commands = FakeCommands({("inspect",): (0, "0\n", ""), ("logs",): (0, "", "")})
    make_runner(roots, commands).finish(make_run(), "ma-run-1-1")

    order = [c for c in commands.calls if "logs" in c or "rm" in c]
    assert "logs" in order[0]
    assert "rm" in order[1]


def test_finish_discards_a_workspace_with_nothing_in_it(roots):
    commands = FakeCommands({("inspect",): (0, "0\n", ""), ("logs",): (0, "", "")})
    runner = make_runner(roots, commands)
    run = make_run()
    runner.start(run)

    runner.finish(run, "ma-run-1-1")

    assert not runner.workspace_path(run).exists()
    assert not runner.job_path(run).exists()


def test_finish_removes_the_workspace_even_when_it_has_commits(roots):
    """Committed work reaches the world by being pushed to origin from inside
    the sandbox; the standalone clone is disposable, so a plain rm -rf is the
    whole cleanup — no worktree deregistration machinery (#33)."""
    repo = roots["repos_root"] / "feliu-dev"
    repo.mkdir(parents=True)
    commands = FakeCommands({("inspect",): (0, "0\n", ""), ("logs",): (0, "", "")})
    runner = make_runner(roots, commands, spec=JobSpec(prompt="p", repo="feliu-dev"))
    run = make_run()
    runner.start(run)
    workspace = runner.workspace_path(run)
    (workspace / ".git").mkdir(parents=True, exist_ok=True)

    runner.finish(run, "ma-run-1-1")

    assert not workspace.exists()
    assert not commands.commands("worktree")


# --- the default command runner ----------------------------------------------


def test_the_real_command_runner_captures_output():
    """The one place the module actually touches subprocess."""
    from orchestrator.sandbox import _run_command

    result = _run_command(["printf", "hello"])
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.stdout == "hello"
    assert result.returncode == 0


def test_kill_shells_out_to_docker(roots):
    commands = FakeCommands()
    make_runner(roots, commands).kill("ma-run-1-1")
    assert commands.commands("kill")


def test_job_env_is_passed_through(roots):
    """How #11 will inject a short-lived GH_TOKEN."""
    commands = FakeCommands()
    runner = make_runner(
        roots, commands, spec=JobSpec(prompt="p", env={"GH_TOKEN": "ghs_x"})
    )
    runner.start(make_run())
    assert "GH_TOKEN=ghs_x" in commands.commands("run")[0]


def test_an_unreadable_exit_code_is_none_rather_than_a_crash(roots):
    commands = FakeCommands(
        {("inspect",): (0, "<no value>\n", ""), ("logs",): (0, "", "")}
    )
    result = make_runner(roots, commands).finish(make_run(), "ma-run-1-1")
    assert result.exit_code is None
    assert result.outcome is Outcome.FAILED


def test_reuse_branch_fetches_it_and_checks_out_without_resetting(roots):
    """The fixer's commits live only on GitHub — it pushed from its own
    /workspace — so the branch must be fetched, and checked out as-is: -B
    would reset it and erase the commits a review/revision run (#8) needs."""
    (roots["repos_root"] / "feliu-dev").mkdir(parents=True)
    commands = FakeCommands()
    spec = JobSpec(
        prompt="p", repo="feliu-dev", branch="agent/run-9", reuse_branch=True
    )
    make_runner(roots, commands, spec=spec).start(make_run(9))

    fetch = commands.commands("fetch")[0]
    assert "+refs/heads/agent/run-9:refs/remotes/origin/agent/run-9" in fetch
    checkout = commands.commands("checkout", "-b")[0]
    assert "-B" not in checkout
    assert "agent/run-9" in checkout and "origin/agent/run-9" in checkout


def test_a_git_failure_cleans_the_half_built_clone_and_raises(roots):
    (roots["repos_root"] / "feliu-dev").mkdir(parents=True)
    commands = FakeCommands({("fetch",): (128, "", "could not read from remote")})
    runner = make_runner(roots, commands, spec=JobSpec(prompt="p", repo="feliu-dev"))
    run = make_run()

    with pytest.raises(RuntimeError, match="git fetch failed"):
        runner.start(run)

    assert not runner.workspace_path(run).exists()


def test_the_host_side_fetch_carries_the_token_but_the_remote_url_does_not(roots):
    """The token authenticates the fetch inline, per command; the clone's
    stored origin stays clean because that config is mounted into the sandbox,
    where the credential helper supplies GH_TOKEN instead."""
    minted = []

    def token():
        minted.append(1)
        return f"ghs_token_{len(minted)}"

    (roots["repos_root"] / "feliu-dev").mkdir(parents=True)
    commands = FakeCommands()
    spec = JobSpec(prompt="p", repo="feliu-dev", needs_github=True)
    make_runner(roots, commands, spec=spec, github_token=token).start(make_run())

    fetch = " ".join(commands.commands("fetch")[0])
    assert "x-access-token:ghs_token_1@github.com" in fetch
    set_url = " ".join(commands.commands("remote", "set-url")[0])
    assert "ghs_token_1" not in set_url
    # One token per job, shared between the fetch and the sandbox env, so a
    # leaked token still traces to a single job.
    assert "GH_TOKEN=ghs_token_1" in commands.commands("run")[0]
    assert len(minted) == 1


def test_a_failed_tokenized_fetch_never_leaks_the_token(roots):
    """git echoes the remote URL on failure, and the error message lands in
    the append-only event log."""
    (roots["repos_root"] / "feliu-dev").mkdir(parents=True)
    stderr = "fatal: unable to access 'https://x-access-token:ghs_secret@github.com/'"
    commands = FakeCommands({("fetch",): (128, "", stderr)})
    spec = JobSpec(prompt="p", repo="feliu-dev", needs_github=True)
    runner = make_runner(roots, commands, spec=spec, github_token=lambda: "ghs_secret")

    with pytest.raises(RuntimeError) as excinfo:
        runner.start(make_run())

    assert "ghs_secret" not in str(excinfo.value)
    assert "***" in str(excinfo.value)


def test_a_repo_job_without_an_app_fetches_the_plain_url(roots):
    """No App configured and no GitHub needed: the fetch still runs, against
    the plain URL, which serves a public repo."""
    (roots["repos_root"] / "feliu-dev").mkdir(parents=True)
    commands = FakeCommands()
    runner = make_runner(roots, commands, spec=JobSpec(prompt="p", repo="feliu-dev"))

    runner.start(make_run())

    fetch = " ".join(commands.commands("fetch")[0])
    assert "https://github.com/wanderindev/feliu-dev.git" in fetch
    assert "x-access-token" not in fetch


# --- GitHub token injection (#11) --------------------------------------------


def test_a_job_that_needs_github_gets_a_freshly_minted_token(roots):
    minted = []

    def token():
        minted.append(1)
        return f"ghs_token_{len(minted)}"

    commands = FakeCommands()
    runner = make_runner(
        roots,
        commands,
        spec=JobSpec(prompt="p", needs_github=True),
        github_token=token,
    )
    runner.start(make_run())

    assert "GH_TOKEN=ghs_token_1" in commands.commands("run")[0]
    assert len(minted) == 1, "minted at launch, so the sandbox gets the full hour"


def test_a_job_that_does_not_need_github_gets_no_credential(roots):
    """Opt-in: a job with no business pushing never receives a token at all."""
    commands = FakeCommands()
    runner = make_runner(
        roots, commands, spec=JobSpec(prompt="p"), github_token=lambda: "ghs_x"
    )
    runner.start(make_run())

    assert not any("GH_TOKEN" in arg for arg in commands.commands("run")[0])


def test_needing_github_without_an_app_fails_loudly(roots):
    commands = FakeCommands()
    runner = make_runner(roots, commands, spec=JobSpec(prompt="p", needs_github=True))

    with pytest.raises(RuntimeError, match="no App is configured"):
        runner.start(make_run())
