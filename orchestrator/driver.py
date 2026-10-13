"""The series driver (#13): a dumb consumer of PIC's agent-task queue.

    python -m orchestrator.driver [--dry-run] [--max-tasks N]

Run daily from cron, separate from the loop for the same reason poll and dream
are: scheduling is the operating system's job. PIC decides the cadence (one
series per day or per ISO week, its ``WEEKLY_PLAN_DAILY_CADENCE``); the driver
works whatever goal each claim and plan-week answer names, so one session can
touch several goals (yesterday's waiting gate re-checked, today's new plan).

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

And one report shape (#62, PIC's #553): a **wait task**, any kind whose plan
calls ``/agent-tasks/wait-check/{kind}/{subject_id}``, answers
``{satisfied, waiting_for}``. Unsatisfied is not a failure: the driver reports
``WAITING`` with PIC's ``waiting_for`` text, PIC refunds the attempt and hands
the row out again on a later drive, and the session email lists what waits on
the operator. Which kinds are wait kinds is PIC's business; the driver keys off
the call path, never off a list of kind names.

Each drive session is one run in the event log (kind ``weekly_series_drive``),
executed in-process rather than in a sandbox: the work is HTTP calls that PIC
executes server-side, so a container would be ceremony. The run is created and
leased in one transaction (the loop can never see it QUEUED and try to
dispatch it), heartbeated while the session works, and finished with a summary
the notifier emails. If a drive session crashes, its lease expires and the
loop's reconcile surfaces the corpse through the normal abandon-and-email
path — a dead drive is never silent.
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
from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg

from orchestrator import config, log, queue
from orchestrator.db import connect
from orchestrator.enums import EventType

logger = logging.getLogger(__name__)

RUN_KIND = "weekly_series_drive"

_TASKS = "/api/v1/admin/dashboard/agent-tasks"

#: The one call of every wait kind (PIC's #553). Its presence in a call plan
#: is how the driver learns a task is a wait task.
_WAIT_CHECK = f"{_TASKS}/wait-check/"

#: Goal-row statuses: blocked on the operator, and not yet runnable.
_WAITING = "WAITING"
_UNFINISHED = ("PENDING", "READY", "LEASED")

#: How much of a response body to keep in reports and events.
_CLIP = 2000

#: ``run_failed`` payload reasons for a drive session that could not claim
#: work at all. Distinct from a task failing: nothing about the pipeline is
#: wrong, the queue itself would not talk to us.
REASON_NO_RESEARCH = "no_research"
REASON_PROTOCOL = "protocol"


class ProtocolError(RuntimeError):
    """The queue surface (``next``, ``plan-week``) answered something other
    than success. A task-call failure is a *task* outcome; this is the session
    itself being unable to proceed, and it ends the drive as FAILED with the
    answer preserved — the 2026-09-07/08 drives died on plan-week's 404
    ("No research available for planning") as an uncaught traceback, and
    since the session's transaction was never committed, the run did not even
    exist afterwards. Two days of silence, discovered by a missing series.
    """

    def __init__(self, what: str, status: int, detail: Any) -> None:
        self.what = what
        self.status = status
        self.detail = json.dumps(detail, default=str)[:_CLIP]
        super().__init__(f"{what} answered {status}: {self.detail[:200]}")

    @property
    def reason(self) -> str:
        if self.what == "plan-week" and self.status == 404:
            return REASON_NO_RESEARCH
        return REASON_PROTOCOL


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
        timeout: int = 60,
    ) -> tuple[int, Any]:
        """One call; non-2xx comes back as data, not an exception.

        The driver treats HTTP failure as a *task* outcome to report, so only
        transport-level trouble (network down, DNS, a timed-out read) is
        allowed to raise — and ``execute_calls`` catches that for task calls,
        because a slow endpoint must fail the *task*, not the session (the
        2026-09-01 drive died to an uncaught ``TimeoutError`` from a >60s
        ``generate-outlines`` call, stranding the whole week).
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
            with self._open(request, timeout=timeout) as response:
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
            raise ProtocolError("next", status, data)
        return data

    def plan_week(self) -> dict:
        status, data = self.request("POST", f"{_TASKS}/plan-week")
        if status != 200:
            raise ProtocolError("plan-week", status, data)
        return data

    def report(
        self,
        task_id: int,
        status: str,
        *,
        result: dict | None = None,
        error: str | None = None,
        waiting_for: str | None = None,
    ) -> tuple[int, Any]:
        body: dict[str, Any] = {"status": status, "result": result, "error": error}
        if waiting_for is not None:
            body["waiting_for"] = waiting_for
        return self.request("PATCH", f"{_TASKS}/{task_id}", body=body)

    def job(self, job_id: int) -> tuple[int, Any]:
        return self.request("GET", f"{_TASKS}/jobs/{job_id}")

    def goal_tasks(self, goal_key: str) -> list[dict]:
        status, data = self.request("GET", _TASKS, query={"goal_key": goal_key})
        return data if status == 200 and isinstance(data, list) else []


def is_wait_plan(calls: list[dict]) -> bool:
    """Whether a call plan is a wait kind's condition check."""
    return any(_WAIT_CHECK in (c.get("path") or "") for c in calls)


def wait_label(kind: str | None) -> str:
    """A readable group name for a wait kind, for the session email.

    The concrete gates are PIC's #554-#556 (research ready and article deep
    research, Grammarly EN and ES) and their kind names are PIC's to choose,
    so this matches loosely and falls back to the kind itself: a wait kind
    nobody anticipated still groups, just under its own name.
    """
    upper = (kind or "").upper()
    if "GRAMMAR" in upper:
        for lang in ("EN", "ES"):
            if upper.endswith(f"_{lang}"):
                return f"Grammarly {lang}"
        return "Grammarly"
    if "RESEARCH" in upper or "CLAIM" in upper:
        return "Deep research"
    return upper.replace("_", " ").lower() or "unknown"


def group_waiting(waiting: list[dict]) -> dict[str, list[dict]]:
    """Waiting tasks grouped by :func:`wait_label`, groups in sorted order."""
    groups: dict[str, list[dict]] = {}
    for item in waiting:
        label = item.get("label") or wait_label(item.get("kind"))
        groups.setdefault(label, []).append(item)
    return dict(sorted(groups.items()))


def at_capacity(planned: dict) -> bool:
    """Whether plan-week declined to start a series because enough are in
    flight (PIC's #559: a success answer saying "at capacity", not a 404).

    That is a normal end of a session, not a protocol problem. The exact
    shape is PIC's to settle, so accept an ``at_capacity`` flag or the word
    in any of the usual text fields.
    """
    if planned.get("at_capacity"):
        return True
    text = " ".join(
        str(planned.get(k) or "") for k in ("status", "detail", "message", "reason")
    )
    return "capacity" in text.lower()


def goal_key_for(day: date, cadence: str) -> str:
    """PIC's ``goal_key_for``, mirrored for ``--dry-run`` only (see
    ``config.DRIVER_CADENCE``)."""
    if cadence == "weekly":
        iso = day.isocalendar()
        return f"weekly_series:{iso.year}-W{iso.week:02d}"
    return f"daily_series:{day.isoformat()}"


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
                try:
                    status, job = client.job(job_id)
                except OSError as exc:
                    return False, prev, f"job {job_id} poll transport failure: {exc!r}"
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

        try:
            status, data = client.request(
                call["method"],
                path,
                body=body,
                query=call.get("query"),
                timeout=config.DRIVER_CALL_TIMEOUT_SECONDS,
            )
        except OSError as exc:
            # TimeoutError and URLError are both OSError. A dead or slow
            # endpoint is a task outcome (PIC re-queues or parks it), never a
            # session crash — that stranded W36 on 2026-09-01.
            return (
                False,
                prev,
                f"{call['method']} {path} transport failure: {exc!r}",
            )
        if not 200 <= status < 300:
            return (
                False,
                data,
                (
                    f"{call['method']} {path} answered {status}: "
                    f"{json.dumps(data, default=str)[:300]}"
                ),
            )
        if isinstance(data, dict) and data.get("success") is False:
            return False, data, f"{path} reported success=false"
        prev = data
    return True, prev, None


def _fail(
    conn: psycopg.Connection,
    run_id: int,
    subject: str,
    exc: ProtocolError,
    *,
    done: int,
    failed: int,
) -> dict[str, Any]:
    """End the session FAILED because the queue would not talk to us.

    Whatever tasks ran were already reported to PIC, so nothing is lost on
    that side; what must not be lost is the fact that the drive stopped, and
    why. The reason rides on ``run_failed`` (like ``auth`` does) so the
    notifier can name the fix in the subject line — for ``no_research`` that
    fix is a human approving research in PIC, and every scheduled drive will
    fail the same way until they do.
    """
    summary: dict[str, Any] = {
        "outcome": "FAILED",
        "reason": exc.reason,
        "what": exc.what,
        "status": exc.status,
        "detail": exc.detail,
        "tasks_done": done,
        "tasks_failed": failed,
        "summary": f"drive stopped: {exc} ({done} task(s) done, {failed} failed)",
    }
    with conn.transaction():
        log.append(
            conn,
            run_id,
            EventType.STAGE_COMPLETED,
            {"outcome": "FAILED", "result": summary},
        )
        log.append(
            conn,
            run_id,
            EventType.RUN_FAILED,
            {
                "reason": exc.reason,
                "what": exc.what,
                "status": exc.status,
                "detail": exc.detail,
            },
            worker_id=None,
            lease_expires_at=None,
        )
    logger.error("drive session %s failed: %s", subject, summary["summary"])
    return summary


def _summarize(
    client: PicClient,
    goals: list[str],
    *,
    done: int,
    failed: int,
    waited: int,
    stopped_early: str | None,
    capacity: bool,
) -> dict[str, Any]:
    """The session's result: the state of every goal it touched.

    ``COMPLETE`` needs every touched goal finished. ``AT_CAPACITY`` is the
    other normal end: no goal touched because PIC would not start a new
    series, and nothing of an in-flight one was due. Anything else is
    ``INCOMPLETE`` and parks the run for a human, listing what is parked and
    what is waiting on the operator.
    """
    per_goal: list[dict[str, Any]] = []
    parked: list[dict] = []
    waiting: list[dict] = []
    unfinished = 0
    for goal_key in goals:
        rows = client.goal_tasks(goal_key)
        goal_parked = [t for t in rows if t.get("status") == "FAILED"]
        goal_waiting = [t for t in rows if t.get("status") == _WAITING]
        goal_unfinished = [t for t in rows if t.get("status") in _UNFINISHED]
        per_goal.append(
            {
                "goal_key": goal_key,
                "tasks": len(rows),
                "parked": len(goal_parked),
                "waiting": len(goal_waiting),
                "unfinished": len(goal_unfinished),
            }
        )
        unfinished += len(goal_unfinished)
        parked += [
            {
                "task_id": t["id"],
                "kind": t.get("kind"),
                "subject": f"{t.get('subject_type')}/{t.get('subject_id')}",
                "goal_key": goal_key,
                "error": (t.get("error") or "")[:300],
            }
            for t in goal_parked
        ]
        waiting += [
            {
                "task_id": t["id"],
                "kind": t.get("kind"),
                "label": wait_label(t.get("kind")),
                "subject": f"{t.get('subject_type')}/{t.get('subject_id')}",
                "goal_key": goal_key,
                "waiting_for": (t.get("waiting_for") or "")[:500],
                "next_check_at": t.get("next_check_at"),
            }
            for t in goal_waiting
        ]

    if not goals:
        outcome = "AT_CAPACITY" if capacity and not stopped_early else "INCOMPLETE"
    elif parked or unfinished or waiting or stopped_early:
        outcome = "INCOMPLETE"
    else:
        outcome = "COMPLETE"
    # Waiting rows hold their dependents PENDING, so "unfinished" alongside
    # "waiting" is expected; unfinished with nothing waiting is a stuck row.
    needs_attention = bool(parked or stopped_early or (unfinished and not waiting))

    text = (
        f"daily drive over {', '.join(goals) or '(no goal)'}: {done} task(s) done, "
        f"{waited} waiting on the operator, {failed} failed; "
        f"{len(parked)} parked, {len(waiting)} waiting, "
        f"{unfinished} not yet runnable"
    )
    if capacity:
        text += "; plan-week at capacity, no new series started"
    if stopped_early:
        text += f"; {stopped_early}"

    return {
        "outcome": outcome,
        "goals": per_goal,
        "tasks_done": done,
        "tasks_failed": failed,
        "tasks_waiting": waited,
        "parked": parked,
        "waiting": waiting,
        "unfinished": unfinished,
        "needs_attention": needs_attention,
        "at_capacity": capacity,
        "stopped_early": stopped_early,
        "summary": text,
    }


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

    # Every goal the session touched, in first-seen order: under daily
    # cadence one drive re-checks yesterday's waiting gates *and* plans today,
    # so "the last goal key seen" would hide the older goal's state (#62).
    goals: list[str] = []

    def touch(goal_key: str | None) -> None:
        if goal_key and goal_key not in goals:
            goals.append(goal_key)

    done = failed = waited = 0
    stopped_early: str | None = None
    planned_when_empty = False
    capacity = False

    while True:
        extend_lease()
        try:
            task_response = client.next_task()
            if task_response is None:
                if planned_when_empty:
                    break  # empty even after an ensure-pass: the week can go no further
                planned = client.plan_week()
        except ProtocolError as exc:
            return _fail(conn, run_id, subject, exc, done=done, failed=failed)
        except OSError as exc:
            # Transport trouble on the queue surface itself: no task to fail,
            # so the session fails, with the error text instead of a traceback.
            return _fail(
                conn,
                run_id,
                subject,
                ProtocolError("queue", 0, f"transport failure: {exc!r}"),
                done=done,
                failed=failed,
            )
        if task_response is None:
            planned_when_empty = True
            if at_capacity(planned):
                # Enough series in flight: nothing new to start, and nothing
                # left to claim. A normal end, not a failure (#62, PIC's #559).
                capacity = True
                logger.info("queue empty; plan-week is at capacity")
                break
            touch(planned.get("goal_key"))
            logger.info(
                "queue empty; plan-week ensured goal %s (created %s)",
                planned.get("goal_key"),
                planned.get("created"),
            )
            continue
        planned_when_empty = False

        task = task_response["task"]
        touch(task.get("goal_key"))
        calls = task_response.get("calls") or []
        ok, result, error = execute_calls(
            client,
            calls,
            sleep=sleep,
            now=now,
            heartbeat=extend_lease,
        )
        status = "DONE" if ok else "FAILED"
        waiting_for: str | None = None
        if ok and is_wait_plan(calls):
            satisfied = result.get("satisfied") if isinstance(result, dict) else None
            if satisfied is False:
                # Blocked on the operator, not broken: PIC refunds the attempt
                # and re-offers the row on a later drive.
                status = _WAITING
                waiting_for = str(result.get("waiting_for") or "operator action")[:500]
            elif satisfied is not True:
                ok, status = False, "FAILED"
                error = "wait-check answered without a satisfied flag"
        try:
            report_status, report_body = client.report(
                task["id"],
                status,
                result=_clip_payload(result),
                error=error,
                waiting_for=waiting_for,
            )
        except OSError as exc:
            # An unreportable outcome is a protocol problem like a rejected
            # report: count the failure and let the lease sweep re-queue.
            report_status, report_body = (
                0,
                {"detail": f"report transport failure: {exc!r}"},
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
                    **({"waiting_for": waiting_for} if waiting_for else {}),
                },
            )
        if ok and status == _WAITING:
            waited += 1
            logger.info(
                "task %s %s: WAITING (%s)", task["id"], task.get("kind"), waiting_for
            )
        elif ok:
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
        if done + failed + waited >= max_tasks:
            stopped_early = f"stopped at the {max_tasks}-task session cap"
            break

    summary = _summarize(
        client,
        goals,
        done=done,
        failed=failed,
        waited=waited,
        stopped_early=stopped_early,
        capacity=capacity,
    )
    complete = summary["outcome"] in ("COMPLETE", "AT_CAPACITY")

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
            # Parked, unfinished or waiting work is a human's now; the gate is
            # what #9 emails, with the admin queue page as the fixing tool for
            # parked rows and the laptop (extension, Grammarly) for waiting ones.
            if summary["needs_attention"] or not summary["waiting"]:
                why = "daily drive needs attention"
            else:
                labels = ", ".join(group_waiting(summary["waiting"]))
                why = f"daily drive waiting on you: {labels}"
            log.append(
                conn,
                run_id,
                EventType.HUMAN_GATE,
                {
                    "why": why,
                    "reason": summary["summary"],
                    "parked_tasks": summary["parked"],
                    "waiting_tasks": summary["waiting"],
                    "goal_keys": [g["goal_key"] for g in summary["goals"]],
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
        # Read-only: no claim, no plan. The key mirrors PIC's planner, which
        # anchors on the UTC date and keys by day or ISO week per its cadence.
        goal_key = goal_key_for(datetime.now(UTC).date(), config.DRIVER_CADENCE)
        tasks = client.goal_tasks(goal_key)
        print(f"{goal_key}: {len(tasks)} task(s)")
        for task in tasks:
            line = (
                f"  {task['id']:>5} {task.get('status', ''):8} {task.get('kind', ''):28}"
                f" {task.get('subject_type', '')}/{task.get('subject_id', '')}"
            )
            if task.get("waiting_for"):
                line += f"  waiting for: {task['waiting_for']}"
            print(line)
        return 0

    with connect() as conn:
        # Autocommit, so each ``conn.transaction()`` block in the session is a
        # real transaction. With it off, the first SELECT opens an implicit
        # transaction that every later block nests into as a savepoint, and
        # nothing reaches the database until this ``with`` exits cleanly: the
        # lease and heartbeats were invisible to the loop all session, and a
        # crash rolled the whole run out of existence (runs 108 and 111 never
        # existed). Committed as it goes, a crash leaves a leased run whose
        # expired lease the loop's reconcile abandons and emails.
        conn.autocommit = True
        summary = drive(conn, client, max_tasks=args.max_tasks)
    return 1 if summary.get("outcome") == "FAILED" else 0


if __name__ == "__main__":
    sys.exit(main())
