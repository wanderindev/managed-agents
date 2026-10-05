"""Poll the work sources once and exit.

    python -m orchestrator.poll

Deliberately a separate command from the loop rather than a step inside a tick.
Scheduling is the operating system's job (a systemd timer or cron, hourly), which
means there is no "when did I last poll" state to keep anywhere, and the
orchestrator stays purely about runs. Running it twice by accident is harmless:
the dedup and the unique index both hold.
"""

import argparse
import functools
import logging
import sys
from collections.abc import Callable

from orchestrator import auth, config, dream, github
from orchestrator.db import connect
from orchestrator.sources import github_prs, sentry

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be enqueued without enqueuing it; how filters get tuned",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    if not config.SENTRY_TOKEN:
        logger.error(
            "ORCHESTRATOR_SENTRY_TOKEN is not set; create a Sentry auth token "
            "with event:read and org:read and put it in /srv/orchestrator.env"
        )
        return 2

    client = sentry.SentryClient(
        config.SENTRY_TOKEN, config.SENTRY_ORG, base_url=config.SENTRY_BASE_URL
    )
    filters = sentry.Filters(
        min_events=config.SENTRY_MIN_EVENTS,
        max_per_poll=config.SENTRY_MAX_PER_POLL,
        cooldown_days=config.SENTRY_COOLDOWN_DAYS,
    )
    with connect() as conn:
        if auth.skip_enqueue(conn, "Sentry issues or PR change requests"):
            # Not a failure: the timer has nothing to alert on, and the loop's
            # own email already says what the human has to do.
            return 0
        pulls = _pulls_client()
        report = sentry.poll(
            conn,
            client,
            filters=filters,
            dry_run=args.dry_run,
            stats_period=config.SENTRY_STATS_PERIOD,
            base_branch=_base_branch_resolver(pulls),
        )
        _poll_prs(conn, pulls, dry_run=args.dry_run)
    # Exit code carries nothing about how many were enqueued: a poll that finds
    # nothing is a completely normal outcome and must not look like a failure to
    # whatever timer runs this.
    return 0 if report is not None else 1


def _pulls_client() -> github_prs.PullsClient | None:
    """The GitHub App's read client, or None when the App is not usable.

    Optional: without the App there are no orchestrator PRs to poll and no
    repo metadata to read, so skipping is correct, not a degradation — but it
    is said out loud, never silently."""
    try:
        token = github.from_config().installation_token()
    except github.GitHubAppError as exc:
        logger.warning(
            "GitHub App not usable (%s): skipping the PR poll, and assuming"
            " every Sentry fix targets main",
            exc,
        )
        return None
    return github_prs.PullsClient(token)


def _base_branch_resolver(
    pulls: github_prs.PullsClient | None,
) -> Callable[[str], str] | None:
    """Each repo's default branch as GitHub reports it, read once per poll.

    The base a triage fix targets (#68): atelier-new-cli is on ``master``.
    """
    if pulls is None:
        return None
    return functools.cache(lambda repo: dream.default_branch(pulls, repo=repo))


def _poll_prs(conn, pulls: github_prs.PullsClient | None, *, dry_run: bool) -> None:
    """The change-request source (#10)."""
    if pulls is None:
        return
    github_prs.poll(conn, pulls, dry_run=dry_run)


if __name__ == "__main__":
    sys.exit(main())
