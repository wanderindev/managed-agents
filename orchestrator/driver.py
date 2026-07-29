"""The weekly-series driver (#13): a dumb consumer of PIC's agent-task queue.

    python -m orchestrator.driver [--dry-run] [--max-tasks N]

Scheduled for Saturdays (a systemd timer or cron), separate from the loop for
the same reason poll and dream are: scheduling is the operating system's job.

The division of labor is the design (PIC's epic #422, our #13): PIC owns the
pipeline logic, the dependency graph, the leases, the per-task retry budget,
and the admin UI for unsticking a week. This driver is deliberately dumb: it
claims the next READY task, executes the HTTP call plan PIC hands back *in the
claim response*, reports the outcome, and repeats. It holds no pipeline
knowledge beyond two hardcoded multi-call shapes (below) and never decides
what should happen next — reporting DONE is what promotes dependents, and
that logic lives in PIC where it can be tested next to the code it drives.

The two multi-call shapes, a prose contract with PIC's ``build_calls``:

* A path containing ``{job_id}`` means the previous call answered 202 with a
  job row; substitute its ``id`` and poll until SUCCEEDED or FAILED.
* A path ending in ``/corrections/apply`` takes ``{"edits": [...]}`` from the
  preceding preview response.

Each drive session is one run in the event log (kind ``weekly_series_drive``),
executed in-process rather than in a sandbox: the work is HTTP calls that PIC
executes server-side, so a container would be ceremony. The run is created and
leased in one transaction (the loop can never see it QUEUED and try to
dispatch it), heartbeated while the session works, and finished with a summary
the notifier emails. If a drive session crashes, its lease expires and the
loop's reconcile surfaces the corpse through the normal abandon-and-email
path — a dead Saturday is never silent.
"""

import argparse
import json
import logging
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from orchestrator import config, log, queue
from orchestrator.db import connect
from orchestrator.enums import EventType

logger = logging.getLogger(__name__)

RUN_KIND = "weekly_series_drive"

_TASKS = "/api/v1/admin/dashboard/agent-tasks"

#: How much of a response body to keep in reports and events.
_CLIP = 2000


class PicClient:
    """Minimal client for PIC's agent-tasks API. stdlib urllib, as ever."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        opener=urllib.request.urlopen,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._open = opener

    def request(
        self,
        method: str,
        path: str,
        *,
        body: dict | None = None,
        query: dict | None = None,
    ) -> tuple[int, Any]:
        """One call; non-2xx comes back as data, not an exception.

        The driver treats HTTP failure as a *task* outcome to report, so only
        transport-level trouble (network down, DNS) is allowed to raise.
        """
        url = f"{self.base_url}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            url,
            method=method,
            data=data,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
                **({"Content-Type": "application/json"} if data else {}),
            },
        )
        try:
            with self._open(request, timeout=60) as response:
                raw = response.read().decode()
                return response.status, json.loads(raw) if raw.strip() else None
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode(errors="replace")
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = {"detail": raw[:_CLIP]}
            return exc.code, parsed

    # --- the agent-tasks surface ---------------------------------------------

    def next_task(self) -> dict | None:
        status, data = self.request("GET", f"{_TASKS}/next")
        if status == 204 or data is None:
            return None
        if status != 200:
            raise RuntimeError(f"next answered {status}: {json.dumps(data)[:200]}")
        return data

    def plan_week(self) -> dict:
        status, data = self.request("POST", f"{_TASKS}/plan-week")
        if status != 200:
            raise RuntimeError(f"plan-week answered {status}: {json.dumps(data)[:200]}")
        return data

    def report(
        self,
        task_id: int,
        status: str,
        *,
        result: dict | None = None,
        error: str | None = None,
    ) -> tuple[int, Any]:
        return self.request(
            "PATCH",
            f"{_TASKS}/{task_id}",
            body={"status": status, "result": result, "error": error},
        )

    def job(self, job_id: int) -> tuple[int, Any]:
        return self.request("GET", f"{_TASKS}/jobs/{job_id}")

    def goal_tasks(self, goal_key: str) -> list[dict]:
        status, data = self.request("GET", _TASKS, query={"goal_key": goal_key})
        return data if status == 200 and isinstance(data, list) else []


def _clip_payload(data: Any) -> Any:
    if data is None:
        return None
    encoded = json.dumps(data, default=str)
    if len(encoded) <= _CLIP:
        return data
    return {"_clipped": True, "preview": encoded[:_CLIP]}


def execute_calls(
    client: PicClient,
    calls: list[dict],
    *,
    poll_seconds: int | None = None,
    timeout_seconds: int | None = None,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
    heartbeat: Callable[[], None] | None = None,
) -> tuple[bool, Any, str | None]:
    """Run one task's call plan in order. Returns ``(ok, last_response, error)``.

    Any non-2xx, a FAILED or timed-out job, or a response whose ``success``
    flag is false ends the plan there: the task is reported FAILED with the
    error, and PIC decides whether it re-queues or parks. The driver never
    improvises around a failing step — that is the admin queue page's job.
    """
    poll_seconds = (
        config.DRIVER_JOB_POLL_SECONDS if poll_seconds is None else poll_seconds
    )
    timeout_seconds = (
        config.DRIVER_JOB_TIMEOUT_SECONDS
        if timeout_seconds is None
        else timeout_seconds
    )
    prev: Any = None
    for call in calls:
        path = call["path"]

        if "{job_id}" in path:
            job_id = (prev or {}).get("id") if isinstance(prev, dict) else None
            if job_id is None:
                return False, prev, "call plan expected a job id but none was returned"
            deadline = now() + timeout_seconds
            while True:
                status, job = client.job(job_id)
                if status != 200:
                    return False, job, f"job {job_id} poll answered {status}"
                if job.get("status") == "SUCCEEDED":
                    prev = job
                    break
                if job.get("status") == "FAILED":
                    return False, job, f"job {job_id} failed: {job.get('error')}"
                if now() > deadline:
                    return (
                        False,
                        job,
                        f"job {job_id} still {job.get('status')} after {timeout_seconds}s",
                    )
                if heartbeat is not None:
                    heartbeat()
                sleep(poll_seconds)
            continue

        body = call.get("body")
        if path.endswith("/corrections/apply"):
            edits = (prev or {}).get("edits") if isinstance(prev, dict) else None
            body = {"edits": edits or []}

        status, data = client.request(
            call["method"], path, body=body, query=call.get("query")
        )
        if not 200 <= status < 300:
            return (
                False,
                data,
                f"{call['method']} {path} answered {status}: "
                f"{json.dumps(data, default=str)[:300]}",
            )
        if isinstance(data, dict) and data.get("success") is False:
            return False, data, f"{path} reported success=false"
        prev = data
    return True, prev, None


def drive(
    conn: psycopg.Connection,
    client: PicClient,
    *,
    worker_id: str | None = None,
    max_tasks: int | None = None,
    max_failures: int | None = None,
    lease_seconds: int | None = None,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """One drive session: work the queue until it is empty, capped, or broken.

    Returns the summary that was also written to the run's final event.
    """
    worker_id = worker_id or f"{config.WORKER_ID}-driver"
    max_tasks = config.DRIVER_MAX_TASKS if max_tasks is None else max_tasks
    max_failures = config.DRIVER_MAX_FAILURES if max_failures is None else max_failures
    lease_seconds = config.LEASE_SECONDS if lease_seconds is None else lease_seconds

    started = queue.db_now(conn)
    subject = f"drive:{started:%Y-%m-%dT%H:%M:%S}"
    with conn.transaction():
        # Created and leased atomically: the loop must never see this QUEUED,
        # or it would try to dispatch a kind that has no sandbox to build.
        run_id = log.create_run(conn, RUN_KIND, subject, {"base_url": client.base_url})
        log.append(
            conn,
            run_id,
            EventType.RUN_LEASED,
            {"worker_id": worker_id, "attempt": 1},
            worker_id=worker_id,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=lease_seconds),
            attempts=1,
        )
    logger.info("drive session %s started (run %s)", subject, run_id)

    def extend_lease() -> None:
        with conn.transaction():
            queue.extend_lease(conn, run_id, worker_id, lease_seconds)

    goal_key: str | None = None
    done = failed = 0
    stopped_early: str | None = None
    planned_when_empty = False

    while True:
        extend_lease()
        task_response = client.next_task()
        if task_response is None:
            if planned_when_empty:
                break  # empty even after an ensure-pass: the week can go no further
            planned = client.plan_week()
            goal_key = planned.get("goal_key") or goal_key
            planned_when_empty = True
            logger.info(
                "queue empty; plan-week ensured goal %s (created %s)",
                goal_key,
                planned.get("created"),
            )
            continue
        planned_when_empty = False

        task = task_response["task"]
        goal_key = task.get("goal_key") or goal_key
        ok, result, error = execute_calls(
            client,
            task_response.get("calls") or [],
            sleep=sleep,
            now=now,
            heartbeat=extend_lease,
        )
        status = "DONE" if ok else "FAILED"
        report_status, report_body = client.report(
            task["id"], status, result=_clip_payload(result), error=error
        )
        if not 200 <= report_status < 300:
            # A rejected report is a protocol problem, not a task problem.
            logger.warning(
                "report for task %s answered %s: %s",
                task["id"],
                report_status,
                json.dumps(report_body, default=str)[:200],
            )
            error = error or f"report answered {report_status}"
            ok = False

        with conn.transaction():
            log.append(
                conn,
                run_id,
                EventType.ARTIFACT,
                {
                    "task_id": task["id"],
                    "kind": task.get("kind"),
                    "subject": f"{task.get('subject_type')}/{task.get('subject_id')}",
                    "attempt": task.get("attempts"),
                    "reported": status,
                    "error": error,
                },
            )
        if ok:
            done += 1
            logger.info("task %s %s: DONE", task["id"], task.get("kind"))
        else:
            failed += 1
            logger.warning(
                "task %s %s: FAILED (%s)", task["id"], task.get("kind"), error
            )

        if failed >= max_failures:
            stopped_early = f"stopped after {failed} failures this session"
            break
        if done + failed >= max_tasks:
            stopped_early = f"stopped at the {max_tasks}-task session cap"
            break

    remaining = client.goal_tasks(goal_key) if goal_key else []
    parked = [t for t in remaining if t.get("status") == "FAILED"]
    unfinished = [
        t for t in remaining if t.get("status") in ("PENDING", "READY", "LEASED")
    ]
    complete = (
        goal_key is not None and not parked and not unfinished and not stopped_early
    )

    summary: dict[str, Any] = {
        "outcome": "COMPLETE" if complete else "INCOMPLETE",
        "goal_key": goal_key,
        "tasks_done": done,
        "tasks_failed": failed,
        "parked": [
            {
                "task_id": t["id"],
                "kind": t.get("kind"),
                "subject": f"{t.get('subject_type')}/{t.get('subject_id')}",
                "error": (t.get("error") or "")[:300],
            }
            for t in parked
        ],
        "unfinished": len(unfinished),
        "stopped_early": stopped_early,
        "summary": (
            f"weekly drive for {goal_key or '(no goal)'}: {done} task(s) done, "
            f"{failed} failed, {len(parked)} parked, {len(unfinished)} not yet runnable"
            + (f"; {stopped_early}" if stopped_early else "")
        ),
    }

    with conn.transaction():
        log.append(
            conn,
            run_id,
            EventType.STAGE_COMPLETED,
            {"outcome": "SUCCEEDED", "result": summary},
        )
        log.append(
            conn,
            run_id,
            EventType.RUN_DONE,
            {},
            worker_id=None,
            lease_expires_at=None,
        )
        if not complete:
            # Parked or unfinished work is a human's Saturday now; the gate is
            # what #9 emails, with the admin queue page as the fixing tool.
            log.append(
                conn,
                run_id,
                EventType.HUMAN_GATE,
                {
                    "why": "weekly drive needs attention",
                    "reason": summary["summary"],
                    "parked_tasks": summary["parked"],
                    "goal_key": goal_key,
                },
            )
    logger.info("drive session %s finished: %s", subject, summary["summary"])
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="list the current goal's tasks without claiming or planning anything",
    )
    parser.add_argument("--max-tasks", type=int, default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s"
    )
    if not config.PIC_DRIVER_TOKEN:
        logger.error(
            "ORCHESTRATOR_PIC_DRIVER_TOKEN is not set; see PIC's agent_driver_token"
        )
        return 2
    client = PicClient(config.PIC_API_BASE, config.PIC_DRIVER_TOKEN)

    if args.dry_run:
        # Read-only: no claim, no plan. The ISO week key mirrors PIC's planner.
        today = datetime.now(UTC).date()
        goal_key = f"weekly_series:{today.year}-W{today.isocalendar().week:02d}"
        tasks = client.goal_tasks(goal_key)
        print(f"{goal_key}: {len(tasks)} task(s)")
        for task in tasks:
            print(
                f"  {task['id']:>5} {task.get('status', ''):8} {task.get('kind', ''):28}"
                f" {task.get('subject_type', '')}/{task.get('subject_id', '')}"
            )
        return 0

    with connect() as conn:
        drive(conn, client, max_tasks=args.max_tasks)
    return 0


if __name__ == "__main__":
    sys.exit(main())
