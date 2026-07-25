"""A Runner that lies about containers, so the loop can be tested without Docker.

Everything interesting about #4 is lifecycle logic. Driving real containers to
exercise it would be slow, flaky, and would test Docker rather than the loop.
"""

from orchestrator.queue import Run


class FakeRunner:
    """Records what it was asked to do and lets a test kill a container."""

    def __init__(self, *, fail_to_start: bool = False) -> None:
        self.fail_to_start = fail_to_start
        self.started: list[int] = []
        self.killed: list[str] = []
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
