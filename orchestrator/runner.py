"""The seam between lifecycle decisions and container mechanics.

The loop (#4) decides *what* should happen to a run. A runner (#5) knows how to
start a container, whether one is still alive, and what it left behind. Keeping
them apart is what lets the loop's logic be tested exhaustively without Docker.

Note what a runner does **not** do: it never writes to the log. It hands back
what it found and the loop appends it. That keeps every database write in one
layer, and it is why the sandbox never needs the log database's password.
"""

from dataclasses import dataclass, field
from typing import Any, Protocol

from orchestrator.enums import Outcome
from orchestrator.queue import Run


@dataclass(frozen=True, slots=True)
class SandboxResult:
    """Everything a finished sandbox left behind."""

    outcome: Outcome
    exit_code: int | None = None
    #: Parsed stream-json objects, in emission order, from the beginning of the
    #: run. The loop skips the ones it has already appended, so handing back the
    #: whole transcript every time is intentional and makes draining idempotent.
    events: list[dict[str, Any]] = field(default_factory=list)
    #: Captured separately from the event stream, because a CLI that dies before
    #: emitting anything produces no events at all and that case still has to be
    #: diagnosable.
    stderr: str = ""
    #: Set when the sandbox wrote a structured result to the agreed path.
    result: dict[str, Any] | None = None


class Runner(Protocol):
    """Container mechanics for one run."""

    def start(self, run: Run) -> str:
        """Launch the sandbox for ``run`` and return an opaque handle.

        The handle is written into the ``sandbox_started`` event payload, which is
        how a restarted orchestrator finds the container again. It must therefore
        survive a process restart, so it has to be something like a container
        name, not an in-memory object.

        Raising is allowed and expected: the loop treats a failure to start as an
        infrastructure problem and requeues.
        """
        ...

    def is_alive(self, handle: str) -> bool:
        """Whether the container behind ``handle`` is still running."""
        ...

    def logs(self, handle: str) -> list[dict[str, Any]]:
        """The stream-json events emitted so far, from the beginning of the run.

        Called while the container is still alive: the loop drains the transcript
        on every heartbeat so that a sandbox killed mid-job (#12) loses nothing
        the log has not already banked. Must tolerate a vanished handle by
        returning an empty list; the reconcile pass will notice the death itself.
        """
        ...

    def finish(self, run: Run, handle: str) -> SandboxResult:
        """Collect what a no-longer-running sandbox produced, and clean up.

        Called exactly once per container, when ``is_alive`` has gone false. Must
        tolerate a handle that has vanished entirely, reporting ``Outcome.GONE``
        rather than raising, because "the container is missing" is a normal thing
        to discover after a host reboot.
        """
        ...

    def kill(self, handle: str) -> None:
        """Tear the container down. Must tolerate an already-dead handle."""
        ...
