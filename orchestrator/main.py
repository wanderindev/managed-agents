"""Entry point. Wires a real Docker runner into the loop and ticks forever.

    python -m orchestrator.main

Reads its configuration from the environment; see docs/runbook.md.
"""

import logging
import signal
import sys
from types import FrameType

from orchestrator import github, jobs
from orchestrator.loop import Orchestrator
from orchestrator.sandbox import DockerRunner

logger = logging.getLogger(__name__)

_stopping = False


def _request_stop(signum: int, _frame: FrameType | None) -> None:
    """Stop after the current tick rather than mid-transaction.

    Killing the process outright is safe too, since the loop is designed to be
    resumed. This just makes the common case tidy.
    """
    global _stopping
    logger.info("signal %s received; stopping after this tick", signum)
    _stopping = True


def _github_token():
    """A callable that mints an installation token, or None if unconfigured.

    Absent rather than fatal: the loop runs perfectly well for kinds that never
    touch GitHub, and a job that does need it fails loudly at launch with a
    message pointing at the runbook.
    """
    try:
        auth = github.from_config()
    except github.GitHubAppError as exc:
        logger.warning("GitHub App not configured (%s); github jobs will fail", exc)
        return None
    return auth.installation_token


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    runner = DockerRunner(jobs.build_spec, github_token=_github_token())
    orchestrator = Orchestrator(runner, followups=jobs.followups)
    logger.info(
        "orchestrator %s starting: image=%s max_concurrent=%s tick=%ss",
        orchestrator.worker_id,
        runner.image,
        orchestrator.max_concurrent,
        orchestrator.tick_seconds,
    )
    orchestrator.run_forever(should_stop=lambda: _stopping)
    logger.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
