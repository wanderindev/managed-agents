"""Turning a run into a job. The registry #6 and #7 fill in.

The loop knows about lifecycles and the runner knows about containers. Neither
knows what a `sentry_triage` run actually *means*, and that separation is what
lets a new kind of work arrive without touching either.
"""

from collections.abc import Callable

from orchestrator.queue import Run
from orchestrator.sandbox import JobSpec

SpecBuilder = Callable[[Run], JobSpec]

_SMOKE_PROMPT = """\
You are running inside the managed-agents sandbox as an end-to-end check.

Write a file named result.json in /work containing exactly:
{"ok": true, "checked": "sandbox"}

Then reply with exactly: SMOKE OK
"""


def _smoke(run: Run) -> JobSpec:
    """A job with no repo, used to prove the whole path works.

    Deliberately exercises the structured-result contract as well as the event
    stream, because "the agent replied" and "the orchestrator can read what the
    agent decided" are different claims and #7 depends on the second one.
    """
    return JobSpec(prompt=_SMOKE_PROMPT, model="claude-opus-5")


REGISTRY: dict[str, SpecBuilder] = {
    "smoke": _smoke,
}


def build_spec(run: Run) -> JobSpec:
    builder = REGISTRY.get(run.kind)
    if builder is None:
        raise RuntimeError(f"no job spec registered for kind {run.kind!r}")
    return builder(run)
