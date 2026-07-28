"""A Runner that lies about containers, so the loop can be tested without Docker.

Everything interesting about #4 is lifecycle logic. Driving real containers to
exercise it would be slow, flaky, and would test Docker rather than the loop.
"""

from typing import Any

from orchestrator.enums import Outcome
from orchestrator.queue import Run
from orchestrator.runner import SandboxResult


class FakeRunner:
    """Records what it was asked to do and lets a test kill a container."""

    def __init__(
        self,
        *,
        fail_to_start: bool = False,
        outcome: Outcome = Outcome.SUCCEEDED,
        exit_code: int = 0,
        events: list[dict[str, Any]] | None = None,
        stderr: str = "",
        result: dict[str, Any] | None = None,
    ) -> None:
        self.fail_to_start = fail_to_start
        self.outcome = outcome
        self.exit_code = exit_code
        self.events = events or []
        self.stderr = stderr
        self.result = result
        self.started: list[int] = []
        self.finished: list[str] = []
        self.killed: list[str] = []
        self.log_reads: list[str] = []
        self._alive: set[str] = set()

    def start(self, run: Run) -> str:
        if self.fail_to_start:
            raise RuntimeError("docker daemon is not listening")
        # Attempt-scoped so a retry gets a distinguishable handle, which is what
        # makes "the resumed run has its own container" observable in a test.
        handle = f"container-{run.id}-{run.attempts}"
        self.started.append(run.id)
        self._alive.add(handle)
        return handle

    def is_alive(self, handle: str) -> bool:
        return handle in self._alive

    def logs(self, handle: str) -> list[dict[str, Any]]:
        """Same transcript finish() would hand back, like `docker logs` mid-run."""
        self.log_reads.append(handle)
        return list(self.events)

    def finish(self, run: Run, handle: str) -> SandboxResult:
        self.finished.append(handle)
        return SandboxResult(
            outcome=self.outcome,
            exit_code=self.exit_code,
            events=list(self.events),
            stderr=self.stderr,
            result=self.result,
        )

    def kill(self, handle: str) -> None:
        self.killed.append(handle)
        self._alive.discard(handle)

    # --- test affordances ---

    def kill_all(self) -> None:
        for handle in list(self._alive):
            self.kill(handle)

    @property
    def alive(self) -> set[str]:
        return set(self._alive)


class VanishingRunner(FakeRunner):
    """A runner whose containers disappear rather than finish. Host reboot shape."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(outcome=Outcome.GONE, **kwargs)


class FakeCommands:
    """Stands in for the `docker` and `git` CLIs.

    Keyed by the leading words of a command, so a test declares only the calls it
    cares about and anything else succeeds silently.
    """

    def __init__(
        self, responses: dict[tuple[str, ...], tuple[int, str, str]] | None = None
    ):
        self.responses = responses or {}
        self.calls: list[list[str]] = []

    def __call__(self, argv):
        argv = list(argv)
        self.calls.append(argv)
        for key, response in self.responses.items():
            if self._matches(argv, key):
                code, out, err = response
                return _Completed(argv, code, out, err)
        return _Completed(argv, 0, "", "")

    @staticmethod
    def _matches(argv: list[str], key: tuple[str, ...]) -> bool:
        return all(part in argv for part in key)

    def commands(self, *parts: str) -> list[list[str]]:
        return [c for c in self.calls if all(p in c for p in parts)]


class _Completed:
    def __init__(self, argv, returncode, stdout, stderr):
        self.args = argv
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
