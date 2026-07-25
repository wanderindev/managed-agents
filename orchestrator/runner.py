"""The seam between lifecycle decisions and container mechanics.

The loop (#4) decides *what* should happen to a run. A runner (#5) knows how to
start a container and whether one is still alive. Keeping them apart is what lets
the loop's logic be tested exhaustively without Docker, and it is the only reason
#4 can land before #5 exists.
"""

from typing import Protocol

from orchestrator.queue import Run


class Runner(Protocol):
    """Container mechanics for one run."""

    def start(self, run: Run) -> str:
        """Launch the sandbox for ``run`` and return an opaque handle.

        The handle is written into the ``sandbox_started`` event payload, which is
        how a restarted orchestrator finds the container again. It must therefore
        survive a process restart, so it has to be something like a container id,
        not an in-memory object.

        Raising is allowed and expected: the loop treats a failure to start as an
        infrastructure problem and requeues.
        """
        ...

    def is_alive(self, handle: str) -> bool:
        """Whether the container behind ``handle`` is still running."""
        ...

    def kill(self, handle: str) -> None:
        """Tear the container down. Must tolerate an already-dead handle."""
        ...
