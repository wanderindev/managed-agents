"""Poll the work sources once and exit.

    python -m orchestrator.poll

Deliberately a separate command from the loop rather than a step inside a tick.
Scheduling is the operating system's job (a systemd timer or cron, hourly), which
means there is no "when did I last poll" state to keep anywhere, and the
orchestrator stays purely about runs. Running it twice by accident is harmless:
the dedup and the unique index both hold.
"""

import argparse
import logging
import sys

from orchestrator import config
from orchestrator.db import connect
from orchestrator.sources import sentry

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
        report = sentry.poll(conn, client, filters=filters, dry_run=args.dry_run)
    # Exit code carries nothing about how many were enqueued: a poll that finds
    # nothing is a completely normal outcome and must not look like a failure to
    # whatever timer runs this.
    return 0 if report is not None else 1


if __name__ == "__main__":
    sys.exit(main())
