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
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

import psycopg

from orchestrator import config, db, log, queue
from orchestrator.enums import EventType
from orchestrator.queue import Run
from orchestrator.runner import Runner

logger = logging.getLogger(__name__)


@dataclass
class TickResult:
    """What one tick did. Returned for logging and asserted on in tests."""

    leased: list[int] = field(default_factory=list)
    heartbeated: list[int] = field(default_factory=list)
    abandoned: list[int] = field(default_factory=list)

    @property
    def idle(self) -> bool:
        return not (self.leased or self.heartbeated or self.abandoned)


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
            self._abandon(conn, run, f"sandbox gone ({handle})", result)

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
        self, conn: psycopg.Connection, run: Run, reason: str, result: TickResult
    ) -> None:
        requeued = run.attempts < self.max_attempts
        with conn.transaction():
            log.append(
                conn,
                run.id,
                EventType.RUN_ABANDONED,
                # `requeued` is always written explicitly. A missing key folds to
                # terminal (see log.apply_event), so omitting it would silently
                # strand a run that had retries left.
                {"requeued": requeued, "reason": reason, "attempts": run.attempts},
                worker_id=None,
                lease_expires_at=None,
            )
        result.abandoned.append(run.id)
        logger.warning(
            "run %s abandoned after attempt %s (%s); %s",
            run.id,
            run.attempts,
            reason,
            "requeued" if requeued else "giving up",
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
            "tick: leased=%s heartbeated=%s abandoned=%s",
            result.leased,
            result.heartbeated,
            result.abandoned,
        )
