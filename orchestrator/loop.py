"""The stateless orchestrator loop: reconcile, then dispatch.

Stateless is the whole point. The loop keeps nothing between ticks and nothing
between processes: every decision is made from ``agent_runs`` and ``agent_events``
as they are right now. Killing this process at any instant and starting a new one
must be indistinguishable from it having run continuously, which is what makes
#12's kill-and-resume demo possible at all.

Two consequences worth knowing before reading the code:

* The sandbox handle lives in the ``sandbox_started`` event payload, not in an
  attribute. A restarted orchestrator finds its containers by reading the log.
* A tick is idempotent and self-healing, so there is no separate boot path. The
  first tick after a restart reconciles whatever it inherits.
"""

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from orchestrator import config, db, log, queue
from orchestrator.enums import EventType, Outcome
from orchestrator.queue import Run
from orchestrator.runner import Runner

logger = logging.getLogger(__name__)

#: How much of a sandbox's stderr to keep on the stage_completed event. The tail
#: is what matters: a crash prints its reason last.
_STDERR_TAIL = 4000

#: How much of the computed backoff jitter may shave off. Applied downward so
#: the ceiling stays a ceiling; at MAX_CONCURRENT_RUNS=1 jitter buys nothing,
#: but it costs nothing either and stops a thundering herd if the cap rises.
_JITTER_FRACTION = 0.1


def backoff_delay(
    attempts: int,
    *,
    base: int,
    ceiling: int,
    jitter: Callable[[], float] = random.random,
) -> timedelta:
    """How long an abandoned run should wait before it is claimable again.

    Exponential in the attempts already burned: the first retry waits ``base``,
    the second twice that, capped at ``ceiling``. This is a guess about how long
    a transient problem takes to clear; when a better number is known (a rate
    limit's reset time), ``Orchestrator._abandon`` takes it explicitly and this
    function is not consulted.
    """
    delay = min(base * (2 ** max(attempts - 1, 0)), ceiling)
    return timedelta(seconds=delay * (1 - _JITTER_FRACTION * jitter()))


@dataclass
class TickResult:
    """What one tick did. Returned for logging and asserted on in tests."""

    leased: list[int] = field(default_factory=list)
    heartbeated: list[int] = field(default_factory=list)
    abandoned: list[int] = field(default_factory=list)
    finished: list[int] = field(default_factory=list)

    @property
    def idle(self) -> bool:
        return not (self.leased or self.heartbeated or self.abandoned or self.finished)


class Orchestrator:
    def __init__(
        self,
        runner: Runner,
        *,
        worker_id: str | None = None,
        max_concurrent: int | None = None,
        lease_seconds: int | None = None,
        max_attempts: int | None = None,
        tick_seconds: int | None = None,
        backoff_base_seconds: int | None = None,
        backoff_ceiling_seconds: int | None = None,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self.runner = runner
        self.worker_id = worker_id or config.WORKER_ID
        self.max_concurrent = (
            config.MAX_CONCURRENT_RUNS if max_concurrent is None else max_concurrent
        )
        self.lease_seconds = (
            config.LEASE_SECONDS if lease_seconds is None else lease_seconds
        )
        self.max_attempts = (
            config.MAX_ATTEMPTS if max_attempts is None else max_attempts
        )
        self.tick_seconds = (
            config.TICK_SECONDS if tick_seconds is None else tick_seconds
        )
        self.backoff_base_seconds = (
            config.BACKOFF_BASE_SECONDS
            if backoff_base_seconds is None
            else backoff_base_seconds
        )
        self.backoff_ceiling_seconds = (
            config.BACKOFF_CEILING_SECONDS
            if backoff_ceiling_seconds is None
            else backoff_ceiling_seconds
        )
        self.jitter = jitter

    # --- one tick ------------------------------------------------------------

    def tick(self, conn: psycopg.Connection) -> TickResult:
        """Reconcile what exists, then fill any free slots.

        Reconcile runs first on purpose: it is what frees the slots that dispatch
        then uses. The other order lets one dead lease block the queue for a whole
        tick.
        """
        result = TickResult()
        self._reconcile(conn, result)
        self._dispatch(conn, result)
        return result

    # --- reconcile -----------------------------------------------------------

    def _reconcile(self, conn: psycopg.Connection, result: TickResult) -> None:
        now = datetime.now(UTC)
        for run in queue.active_runs(conn):
            if run.worker_id == self.worker_id:
                self._reconcile_own(conn, run, result)
            elif run.lease_expired(now):
                # Someone else's run, and its orchestrator is not renewing the
                # lease. We cannot ask its container anything, so the expired
                # lease is the only evidence available and it is enough.
                self._abandon(
                    conn, run, f"lease expired (worker {run.worker_id})", result
                )

    def _reconcile_own(
        self, conn: psycopg.Connection, run: Run, result: TickResult
    ) -> None:
        handle = self._handle_for(conn, run)
        if handle is None:
            # Leased but never started: this process died between committing the
            # lease and launching the container. Nothing ran, so requeueing is
            # exactly right and cannot duplicate work.
            self._abandon(conn, run, "sandbox never started", result)
        elif self.runner.is_alive(handle):
            self._heartbeat(conn, run, result)
        else:
            self._finish(conn, run, handle, result)

    def _finish(
        self, conn: psycopg.Connection, run: Run, handle: str, result: TickResult
    ) -> None:
        """Collect a stopped sandbox and decide what its stopping meant.

        "Not running any more" covers three different things, and conflating them
        was the gap in #4 that only became visible with a real runner: a container
        that succeeded, one whose work failed, and one that was lost. Only the
        third deserves a retry.
        """
        sandbox = self.runner.finish(run, handle)
        rate_limit_reset = self._drain(conn, run, sandbox.events)

        if sandbox.outcome is Outcome.GONE:
            # If the transcript showed a rate limit, its reset time is a better
            # wait than any computed backoff: retrying sooner is guaranteed to
            # fail, and three instant retries would just burn the attempt budget.
            self._abandon(
                conn,
                run,
                f"sandbox vanished ({handle})",
                result,
                not_before=rate_limit_reset,
            )
            return

        succeeded = sandbox.outcome is Outcome.SUCCEEDED
        with conn.transaction():
            log.append(
                conn,
                run.id,
                EventType.STAGE_COMPLETED,
                {
                    "container": handle,
                    "exit_code": sandbox.exit_code,
                    "outcome": sandbox.outcome.value,
                    "result": sandbox.result,
                    "stderr": sandbox.stderr[-_STDERR_TAIL:] if sandbox.stderr else "",
                },
            )
            log.append(
                conn,
                run.id,
                EventType.RUN_DONE if succeeded else EventType.RUN_FAILED,
                {"exit_code": sandbox.exit_code},
                worker_id=None,
                lease_expires_at=None,
            )
        result.finished.append(run.id)
        logger.info(
            "run %s finished: %s (exit %s)", run.id, sandbox.outcome, sandbox.exit_code
        )

    def _drain(
        self, conn: psycopg.Connection, run: Run, events: list[dict[str, Any]]
    ) -> datetime | None:
        """Append the transcript, skipping whatever is already stored.

        The runner hands back every event from the beginning of the run, so this
        is idempotent: a crash part way through a drain costs nothing, and a
        harvest after a restart picks up exactly where the last one stopped.

        Returns the reset time of the last rate limit seen, if any, so the
        abandon path can wait until then instead of guessing.
        """
        reset_at: datetime | None = None
        already = log.count_events(conn, run.id, EventType.CLAUDE_EVENT)
        for payload in events[already:]:
            with conn.transaction():
                log.append(conn, run.id, EventType.CLAUDE_EVENT, payload)
            reset_at = self._warn_on_rate_limit(run, payload) or reset_at
        return reset_at

    def _warn_on_rate_limit(self, run: Run, payload: dict[str, Any]) -> datetime | None:
        """Surface a rate limit rather than letting it look like a random failure.

        Provisioning the host showed the stream carries `rate_limit_event` with a
        five-hour window. It is invisible unless something reads for it, and it is
        the one failure where retrying immediately is guaranteed to be useless.
        Returns the reset time when the event carries a usable one.
        """
        if payload.get("type") != "rate_limit_event":
            return None
        info = payload.get("rate_limit_info") or {}
        if info.get("status") == "allowed":
            return None
        logger.warning(
            "run %s hit a rate limit (%s, resets at %s)",
            run.id,
            info.get("rateLimitType"),
            info.get("resetsAt"),
        )
        resets_at = info.get("resetsAt")
        if isinstance(resets_at, (int, float)) and not isinstance(resets_at, bool):
            return datetime.fromtimestamp(resets_at, tz=UTC)
        return None

    def _handle_for(self, conn: psycopg.Connection, run: Run) -> str | None:
        event = log.latest_event(conn, run.id, EventType.SANDBOX_STARTED)
        if event is None:
            return None
        return event.payload.get("container")

    def _heartbeat(
        self, conn: psycopg.Connection, run: Run, result: TickResult
    ) -> None:
        """Push the lease out while the sandbox is still working.

        Deliberately does *not* append an event. A heartbeat every tick for every
        active run would bury the actual narrative under thousands of rows, and
        the lease is operational metadata that replay never derives. The log stays
        the source of truth for *status*; the lease is just how long this worker's
        claim is good for.
        """
        with conn.transaction():
            queue.extend_lease(conn, run.id, self.worker_id, self.lease_seconds)
        result.heartbeated.append(run.id)

    def _abandon(
        self,
        conn: psycopg.Connection,
        run: Run,
        reason: str,
        result: TickResult,
        *,
        not_before: datetime | None = None,
    ) -> None:
        """Give up on this attempt: requeue with a backoff, or stop for good.

        ``not_before`` lets a caller with a known-better wait (a rate limit's
        reset time) override the computed backoff. It is recorded in the event
        payload as well as the column, or reading the log would tell you a run
        was requeued but not why it then sat still for ten minutes.
        """
        requeued = run.attempts < self.max_attempts
        # `requeued` is always written explicitly. A missing key folds to
        # terminal (see log.apply_event), so omitting it would silently
        # strand a run that had retries left.
        payload: dict[str, Any] = {
            "requeued": requeued,
            "reason": reason,
            "attempts": run.attempts,
        }
        run_fields: dict[str, Any] = {"worker_id": None, "lease_expires_at": None}
        if requeued:
            if not_before is None:
                # From the database's clock, not the host's: the claim query
                # compares against now() in SQL, and a clock skew between the
                # two would silently stretch or shrink every window.
                not_before = queue.db_now(conn) + backoff_delay(
                    run.attempts,
                    base=self.backoff_base_seconds,
                    ceiling=self.backoff_ceiling_seconds,
                    jitter=self.jitter,
                )
            payload["not_before"] = not_before.isoformat()
            run_fields["not_before"] = not_before
        with conn.transaction():
            log.append(conn, run.id, EventType.RUN_ABANDONED, payload, **run_fields)
        result.abandoned.append(run.id)
        logger.warning(
            "run %s abandoned after attempt %s (%s); %s",
            run.id,
            run.attempts,
            reason,
            f"requeued, claimable after {not_before}" if requeued else "giving up",
        )

    # --- dispatch ------------------------------------------------------------

    def _dispatch(self, conn: psycopg.Connection, result: TickResult) -> None:
        slots = self.max_concurrent - len(queue.active_runs(conn))
        for _ in range(max(slots, 0)):
            run = self._lease_one(conn)
            if run is None:
                return
            result.leased.append(run.id)
            if not self._start(conn, run, result):
                # Something is wrong with the host, not with this run. Stop
                # dispatching for now rather than marching the whole queue
                # through the same failure. Also stops a requeued run from being
                # picked straight back up inside this same tick.
                return

    def _lease_one(self, conn: psycopg.Connection) -> Run | None:
        """Claim the oldest queued run. Claim and lease share one transaction.

        ``claim_next_queued`` holds a row lock that only means something until the
        transaction ends, so the ``run_leased`` append has to happen inside it.
        """
        with conn.transaction():
            run = queue.claim_next_queued(conn)
            if run is None:
                return None
            attempt = run.attempts + 1
            expires = datetime.now(UTC) + timedelta(seconds=self.lease_seconds)
            log.append(
                conn,
                run.id,
                EventType.RUN_LEASED,
                {"worker_id": self.worker_id, "attempt": attempt},
                worker_id=self.worker_id,
                lease_expires_at=expires,
                attempts=attempt,
            )
            logger.info("run %s leased (attempt %s)", run.id, attempt)
            return replace(
                run,
                attempts=attempt,
                worker_id=self.worker_id,
                lease_expires_at=expires,
            )

    def _start(self, conn: psycopg.Connection, run: Run, result: TickResult) -> bool:
        try:
            handle = self.runner.start(run)
        except Exception as exc:  # any failure to start is infrastructural
            logger.exception("run %s failed to start a sandbox", run.id)
            self._abandon(conn, run, f"sandbox failed to start: {exc}", result)
            return False
        with conn.transaction():
            log.append(conn, run.id, EventType.SANDBOX_STARTED, {"container": handle})
        return True

    # --- forever -------------------------------------------------------------

    def run_forever(
        self,
        *,
        dsn: str | None = None,
        should_stop: Callable[[], bool] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Tick until told to stop.

        A fresh connection per tick, on purpose. Managed Postgres drops idle
        connections, and a 15-second tick makes connection setup irrelevant next
        to the robustness of never holding one across a nap. It also means a
        transient database outage costs one tick instead of the process.
        """
        while not (should_stop is not None and should_stop()):
            try:
                with db.connect(dsn) as conn:
                    self.report(self.tick(conn))
            except Exception:  # never let one bad tick kill the loop
                logger.exception("tick failed; retrying next tick")
            sleep(self.tick_seconds)

    def report(self, result: TickResult) -> None:
        """Log a tick that did something. Silent when idle, or a quiet weekend
        fills the log with thousands of lines saying nothing happened."""
        if result.idle:
            return
        logger.info(
            "tick: leased=%s heartbeated=%s finished=%s abandoned=%s",
            result.leased,
            result.heartbeated,
            result.finished,
            result.abandoned,
        )
