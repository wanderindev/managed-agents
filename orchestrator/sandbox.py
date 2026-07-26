"""The real runner: one disposable Docker container per run. Issue #5.

Shells out to the `docker` CLI rather than using a client library. The commands
are the same ones `scripts/run-sandbox.sh` documents, which means an operator can
reproduce by hand exactly what the orchestrator does, and there is one fewer
dependency in a process whose job is to be boring.

Nothing here writes to the database. ``finish`` hands the transcript back and the
loop appends it, which is why the sandbox never needs the log database's password.
"""

import json
import logging
import shutil
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from orchestrator import config
from orchestrator.enums import Outcome
from orchestrator.queue import Run
from orchestrator.runner import SandboxResult

logger = logging.getLogger(__name__)

#: Where the entrypoint looks for the prompt, and where a job writes its
#: structured result. Mounted separately from the repo so a job cannot commit
#: its own instructions by accident.
WORK_MOUNT = "/work"
RESULT_FILENAME = "result.json"
PROMPT_FILENAME = "prompt.txt"

#: `timeout` exits 124 when it fires. Worth naming: otherwise a timed-out run
#: looks like an arbitrary failure.
TIMEOUT_EXIT_CODE = 124


@dataclass(frozen=True, slots=True)
class JobSpec:
    """What a run needs in order to become a container.

    Built by whatever knows about the *kind* of work. #6 and #7 fill this in for
    Sentry triage; the loop and the runner stay ignorant of what a job means.
    """

    prompt: str
    #: Directory name under ``repos_root``. Empty means the job gets a scratch
    #: workspace with no repo, which is what the smoke kind uses.
    repo: str = ""
    branch: str = ""
    env: dict[str, str] = field(default_factory=dict)
    model: str | None = None
    #: Whether this job needs to talk to GitHub. When set, the runner mints a
    #: fresh installation token (#11) and injects it as GH_TOKEN. Opt-in, so a
    #: job that has no business pushing never receives a credential at all.
    needs_github: bool = False


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess]


def _run_command(argv: Sequence[str]) -> subprocess.CompletedProcess:
    return subprocess.run(argv, capture_output=True, text=True, check=False)


class DockerRunner:
    def __init__(
        self,
        job_spec: Callable[[Run], JobSpec],
        *,
        image: str | None = None,
        repos_root: str | Path | None = None,
        worktrees_root: str | Path | None = None,
        jobs_root: str | Path | None = None,
        credentials: str | Path | None = None,
        timeout_seconds: int | None = None,
        with_docker: bool | None = None,
        memory: str | None = None,
        memory_swap: str | None = None,
        docker_bin: str = "docker",
        git_bin: str = "git",
        run_command: CommandRunner = _run_command,
        github_token: Callable[[], str] | None = None,
    ) -> None:
        self.job_spec = job_spec
        self.image = image or config.SANDBOX_IMAGE
        self.repos_root = Path(repos_root or config.REPOS_ROOT)
        self.worktrees_root = Path(worktrees_root or config.WORKTREES_ROOT)
        self.jobs_root = Path(jobs_root or config.JOBS_ROOT)
        self.credentials = Path(credentials or config.CLAUDE_CREDENTIALS)
        self.timeout_seconds = (
            config.SANDBOX_TIMEOUT_SECONDS
            if timeout_seconds is None
            else timeout_seconds
        )
        self.with_docker = (
            config.SANDBOX_WITH_DOCKER if with_docker is None else with_docker
        )
        self.memory = memory or config.SANDBOX_MEMORY
        self.memory_swap = memory_swap or config.SANDBOX_MEMORY_SWAP
        self.docker_bin = docker_bin
        self.git_bin = git_bin
        self._run = run_command
        self._github_token = github_token

    # --- naming --------------------------------------------------------------

    def container_name(self, run: Run) -> str:
        """Attempt-scoped, so a retry never collides with its predecessor's
        container and the two are distinguishable in `docker ps -a`."""
        return f"ma-run-{run.id}-{run.attempts}"

    def workspace_path(self, run: Run) -> Path:
        return self.worktrees_root / self.container_name(run)

    def job_path(self, run: Run) -> Path:
        return self.jobs_root / self.container_name(run)

    # --- Runner protocol -----------------------------------------------------

    def start(self, run: Run) -> str:
        spec = self.job_spec(run)
        name = self.container_name(run)

        if not self.credentials.is_file():
            raise RuntimeError(f"no Claude credential at {self.credentials}")

        workspace = self._make_workspace(run, spec)
        job_dir = self._make_job_dir(run, spec)

        argv = self._docker_run_argv(run, spec, name, workspace, job_dir)
        result = self._run(argv)
        if result.returncode != 0:
            # Leave nothing half-built: the loop will requeue and a retry gets a
            # clean workspace rather than inheriting this one.
            self._remove_paths(workspace, job_dir)
            raise RuntimeError(
                f"docker run failed ({result.returncode}): {result.stderr.strip()}"
            )
        logger.info("run %s started container %s", run.id, name)
        return name

    def is_alive(self, handle: str) -> bool:
        result = self._run(
            [self.docker_bin, "inspect", "-f", "{{.State.Running}}", handle]
        )
        if result.returncode != 0:
            return False  # unknown container: gone, not running
        return result.stdout.strip() == "true"

    def finish(self, run: Run, handle: str) -> SandboxResult:
        state = self._run(
            [self.docker_bin, "inspect", "-f", "{{.State.ExitCode}}", handle]
        )
        if state.returncode != 0:
            # The container is not merely stopped, it is missing. A host reboot or
            # a manual `docker rm` gets here, and it is a retryable loss rather
            # than a verdict on the work.
            logger.warning("run %s: container %s has vanished", run.id, handle)
            self._cleanup(run, keep_workspace=True)
            return SandboxResult(outcome=Outcome.GONE)

        exit_code = self._parse_exit_code(state.stdout)
        events, stderr = self._collect_logs(handle)
        job_result = self._read_result(run)

        if exit_code == TIMEOUT_EXIT_CODE:
            stderr = (
                f"sandbox exceeded its {self.timeout_seconds}s wall clock\n{stderr}"
            )

        outcome = Outcome.SUCCEEDED if exit_code == 0 else Outcome.FAILED
        self._run([self.docker_bin, "rm", "-f", handle])
        self._cleanup(run, keep_workspace=self._workspace_has_work(run))

        return SandboxResult(
            outcome=outcome,
            exit_code=exit_code,
            events=events,
            stderr=stderr,
            result=job_result,
        )

    def kill(self, handle: str) -> None:
        self._run([self.docker_bin, "kill", handle])

    # --- docker --------------------------------------------------------------

    def _docker_run_argv(
        self, run: Run, spec: JobSpec, name: str, workspace: Path, job_dir: Path
    ) -> list[str]:
        argv = [
            self.docker_bin,
            "run",
            "--detach",
            "--name",
            name,
            # Not --rm: the logs are the transcript, and they have to outlive the
            # process so a restarted orchestrator can still harvest them. The
            # container is removed in finish(), once its output is safely read.
            "--init",
            "--memory",
            self.memory,
            "--memory-swap",
            self.memory_swap,
            "--pids-limit",
            "512",
            "--volume",
            f"{workspace}:/workspace",
            "--volume",
            f"{job_dir}:{WORK_MOUNT}",
            # Read-write on purpose: Claude Code refreshes its OAuth token, and a
            # read-only mount works right until the token expires, at which point
            # every unattended run fails at once.
            "--volume",
            f"{self.credentials}:/home/agent/.claude/.credentials.json",
            "--env",
            f"AGENT_TIMEOUT_SECONDS={self.timeout_seconds}",
        ]
        if spec.model:
            argv += ["--env", f"AGENT_MODEL={spec.model}"]
        for key, value in spec.env.items():
            argv += ["--env", f"{key}={value}"]
        if spec.needs_github:
            if self._github_token is None:
                raise RuntimeError(
                    f"run {run.id} needs GitHub but no App is configured; "
                    "see docs/runbook.md"
                )
            # Minted here, at launch, so the sandbox gets the full hour rather
            # than the remains of a token minted at planning time.
            argv += ["--env", f"GH_TOKEN={self._github_token()}"]
        if self.with_docker:
            argv += ["--volume", "/var/run/docker.sock:/var/run/docker.sock"]
        argv.append(self.image)
        return argv

    def _collect_logs(self, handle: str) -> tuple[list[dict], str]:
        """Split the container's two output channels.

        stdout is the stream-json transcript. stderr is kept separately because a
        CLI that dies before emitting anything produces no events at all, and that
        is precisely the case that has to stay diagnosable.
        """
        result = self._run([self.docker_bin, "logs", handle])
        events: list[dict] = []
        unparsed = 0
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                unparsed += 1
                continue
            if isinstance(parsed, dict):
                events.append(parsed)
            else:
                unparsed += 1
        if unparsed:
            logger.warning("%s unparseable log lines from %s", unparsed, handle)
        return events, result.stderr

    @staticmethod
    def _parse_exit_code(raw: str) -> int | None:
        try:
            return int(raw.strip())
        except ValueError:
            return None

    # --- workspace -----------------------------------------------------------

    def _make_workspace(self, run: Run, spec: JobSpec) -> Path:
        path = self.workspace_path(run)
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)

        if not spec.repo:
            path.mkdir(parents=True, exist_ok=True)
            return path

        repo = self.repos_root / spec.repo
        if not repo.is_dir():
            raise RuntimeError(f"no clone at {repo}")

        branch = spec.branch or f"agent/run-{run.id}"
        path.parent.mkdir(parents=True, exist_ok=True)
        # A worktree rather than a clone: same object store, so this is cheap even
        # for a large repo, and concurrent jobs cannot collide on a branch.
        result = self._run(
            [self.git_bin, "-C", str(repo), "worktree", "add", "-B", branch, str(path)]
        )
        if result.returncode != 0:
            raise RuntimeError(f"git worktree add failed: {result.stderr.strip()}")
        return path

    def _make_job_dir(self, run: Run, spec: JobSpec) -> Path:
        path = self.job_path(run)
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        (path / PROMPT_FILENAME).write_text(spec.prompt)
        return path

    def _read_result(self, run: Run) -> dict | None:
        path = self.job_path(run) / RESULT_FILENAME
        if not path.is_file():
            return None
        try:
            parsed = json.loads(path.read_text())
        except json.JSONDecodeError:
            logger.warning("run %s wrote an unparseable %s", run.id, RESULT_FILENAME)
            return None
        return parsed if isinstance(parsed, dict) else None

    def _workspace_has_work(self, run: Run) -> bool:
        """Whether the worktree holds commits worth keeping.

        A job that produced nothing leaves nothing behind. One that committed
        keeps its worktree, because #7 still has to push that branch.
        """
        path = self.workspace_path(run)
        if not (path / ".git").exists():
            return False
        result = self._run(
            [self.git_bin, "-C", str(path), "status", "--porcelain", "--branch"]
        )
        if result.returncode != 0:
            return True  # cannot tell; keeping it is the safe direction
        return "[ahead " in result.stdout or any(
            line and not line.startswith("##") for line in result.stdout.splitlines()
        )

    def _cleanup(self, run: Run, *, keep_workspace: bool) -> None:
        self._remove_paths(self.job_path(run))
        if not keep_workspace:
            workspace = self.workspace_path(run)
            if (workspace / ".git").exists():
                # Let git forget the worktree too, or the parent repo accumulates
                # stale administrative entries that break later `worktree add`.
                self._run(
                    [
                        self.git_bin,
                        "-C",
                        str(workspace),
                        "worktree",
                        "remove",
                        "--force",
                        ".",
                    ]
                )
            self._remove_paths(workspace)

    @staticmethod
    def _remove_paths(*paths: Path) -> None:
        for path in paths:
            shutil.rmtree(path, ignore_errors=True)
