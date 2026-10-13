"""Pipeline supervisor, phase 2: the orchestrator retries what a diagnosis names. #84.

Phase 1 (#81, orchestrator/supervisor.py) diagnoses a stuck PIC goal in a
read-only sandbox. Phase 2 lets a diagnosis that recommends ``retry`` act,
within limits the orchestrator enforces itself:

* **The sandbox stays read-only.** The model never holds a write tool or a
  write token. It recommends; the host acts, after the run, with its own
  ``read,pipeline`` token (``ORCHESTRATOR_PIC_MCP_PIPELINE_TOKEN``) and an MCP
  client allowed exactly one tool, ``retry_agent_task``
  (orchestrator/mcp_client.py). If the model called the tool itself, only its
  own restraint could stop a second retry; here the orchestrator refuses it.
* **Retry only.** Never skip, never publish: only a diagnosis whose
  ``recommended_action`` is ``retry`` acts, only on its tasks marked ``retry``,
  and never on a publish-path kind (PIC refuses those server-side too, its
  ``MCP_REFUSED_TASK_ACTIONS``, #686).
* **One retry per task per rolling 24 hours**, across all runs, read from the
  log. A ``calling`` event is committed, under a per-task advisory lock,
  *before* the call goes out, and that is what the limit counts: a crash
  between the call and its outcome still uses up the day's retry rather than
  allowing a second one. Rolling rather than calendar day so 23:59 and 00:01
  are not two days.
* **Committed, not just written.** The loop runs a whole tick on one
  connection with autocommit off, and its first statement is a bare SELECT,
  so every ``conn.transaction()`` inside the tick is a savepoint and nothing
  is durable until the tick ends; a tick that fails later rolls all of it
  back. :func:`execute` therefore commits the tick's open transaction first
  and every claim and outcome after it, so each is its own committed
  transaction. What that first commit makes durable early is exactly what the
  loop already wrote as finished blocks: the run's completion and its
  diagnosis artifact, which must survive for the retry to make sense.
* **At most** ``ORCHESTRATOR_SUPERVISOR_MAX_RETRIES_PER_RUN`` calls per
  diagnosis.
* **Kill switch**: ``ORCHESTRATOR_SUPERVISOR_AUTO_RETRY`` (off by default) and
  the pipeline token must both be set; otherwise diagnoses stay read-only.

Every attempt, made or refused, is a ``task_action`` event on the diagnosis
run, and the diagnosis email lists them. The diagnosis's reason goes to PIC as
the retry's ``reason``, so it lands on PIC's ``agent_task_actions`` row (actor
``mcp:<token name>``) next to the ``mcp_tool_calls`` audit row.
"""

import logging
from collections.abc import Callable, Iterable
from typing import Any

import psycopg

from orchestrator import config, log
from orchestrator.enums import EventType
from orchestrator.mcp_client import Client, McpError, ToolError
from orchestrator.queue import Run

logger = logging.getLogger(__name__)

TOOL = "retry_agent_task"

#: The only tools the host-side client may call.
HOST_TOOLS = (TOOL,)

#: Kinds the orchestrator never retries, by the kind the diagnosis names: they
#: approve, publish or schedule. PIC refuses them by the task's real kind.
NEVER_RETRY_KINDS = frozenset(
    {"APPROVE_ARTICLE", "SET_PUBLISH_DATE", "APPROVE_IG_POST", "SCHEDULE_IG_POST"}
)

#: Exclusive bound on a task id (Postgres ``integer``).
MAX_TASK_ID = 2**31

#: The per-task limit's window.
WINDOW_HOURS = 24

#: The retry reason's length cap. PIC accepts up to 1000 characters
#: (``MAX_ACTION_REASON_CHARS``) on ``agent_task_actions``, but its
#: ``mcp_tool_calls`` audit stores a string argument over 200 characters
#: (``MAX_INLINE_STRING``) as a length and digest only; at 200 the reason is
#: readable in both.
MAX_REASON_CHARS = 200

#: Committed before the call; what the per-task limit counts.
CALLING = "calling"
EXECUTED = "executed"
#: Refused by the orchestrator: the task had a retry within the window.
REFUSED_LIMIT = "refused_limit"
#: Refused by the orchestrator: the run's retry cap was reached.
REFUSED_CAP = "refused_cap"
#: Refused by the orchestrator: a publish-path kind.
REFUSED_KIND = "refused_kind"
#: PIC answered with a tool error (refused / conflict / not_found).
REFUSED_PIC = "refused_pic"
#: The call failed in transport: whether it took effect is unknown.
ERROR = "error"

_DETAIL_CLIP = 500

_LAST_CALL_SQL = """
SELECT e.run_id, e.created_at
FROM agent_events e
WHERE e.type = %(type)s
  AND e.payload->>'action' = 'retry'
  AND e.payload->>'outcome' = %(calling)s
  AND e.payload->>'task_id' = %(task_id)s
  AND e.created_at > now() - %(hours)s * interval '1 hour'
ORDER BY e.created_at DESC
LIMIT 1
"""


def enabled() -> bool:
    return config.SUPERVISOR_AUTO_RETRY and bool(config.PIC_MCP_PIPELINE_TOKEN)


def _reason(run: Run, diagnosis: dict[str, Any], task: dict[str, Any]) -> str:
    why = task.get("why") or diagnosis.get("cause") or "transient failure"
    confidence = diagnosis.get("confidence") or "unstated"
    text = f"managed-agents supervisor run {run.id} ({confidence} confidence): {why}"
    return text[:MAX_REASON_CHARS]


def requests(run: Run, artifact: dict[str, Any] | None) -> tuple[dict[str, Any], ...]:
    """The retries a logged diagnosis asks for; empty when auto-retry is off.

    Pure (config aside): ``artifact`` is :func:`supervisor.artifact`'s output.
    Only a diagnosis recommending ``retry`` asks, and only for its tasks
    marked ``retry`` with a usable id, each once.
    """
    if not enabled() or not artifact:
        return ()
    diagnosis = artifact.get("diagnosis") or {}
    if diagnosis.get("recommended_action") != "retry":
        return ()
    seen: set[int] = set()
    out = []
    for task in diagnosis.get("tasks") or []:
        task_id = task.get("task_id")
        if task.get("action") != "retry" or not isinstance(task_id, int):
            continue
        # PIC's ids are int4; a larger one would also reach the advisory
        # lock as int8, for which Postgres has no two-argument overload.
        if not 1 <= task_id < MAX_TASK_ID or task_id in seen:
            continue
        seen.add(task_id)
        out.append(
            {
                "task_id": task_id,
                "kind": task.get("kind") or "",
                "goal_key": diagnosis.get("goal_key"),
                "reason": _reason(run, diagnosis, task),
            }
        )
    return tuple(out)


def last_call(conn: psycopg.Connection, task_id: int) -> dict[str, Any] | None:
    """The latest retry call on ``task_id`` within the window, if any."""
    with conn.cursor() as cur:
        cur.execute(
            _LAST_CALL_SQL,
            {
                "type": EventType.TASK_ACTION.value,
                "calling": CALLING,
                "task_id": str(task_id),
                "hours": WINDOW_HOURS,
            },
        )
        return cur.fetchone()


def default_client() -> Client:
    return Client(
        config.PIC_MCP_URL,
        config.PIC_MCP_PIPELINE_TOKEN,
        allowed_tools=HOST_TOOLS,
        timeout=config.PIC_MCP_TIMEOUT_SECONDS,
    )


def _settle(conn: psycopg.Connection, durable: bool) -> None:
    """Commit whatever is open, so the next ``conn.transaction()`` is an outer,
    committing one rather than a savepoint inside the tick's transaction."""
    if durable and conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        conn.commit()


def _record(
    conn: psycopg.Connection,
    run_id: int,
    base: dict,
    outcome: str,
    *,
    durable: bool,
    **detail: Any,
) -> dict[str, Any]:
    payload = {**base, "outcome": outcome, **detail}
    _settle(conn, durable)
    with conn.transaction():
        log.append(conn, run_id, EventType.TASK_ACTION, payload)
    return payload


def _claim(
    conn: psycopg.Connection, run_id: int, base: dict, *, durable: bool
) -> dict[str, Any] | None:
    """Commit the ``calling`` event, or return the refusal when the task had
    a call within the window. Serialised per task by an advisory lock, held
    until the commit."""
    task_id = base["task_id"]
    _settle(conn, durable)
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext('pic_task_retry'), %s)",
                (task_id,),
            )
        prior = last_call(conn, task_id)
        if prior is not None:
            payload = {
                **base,
                "outcome": REFUSED_LIMIT,
                "previous_run": prior["run_id"],
                "previous_at": prior["created_at"].isoformat(),
            }
        else:
            payload = {**base, "outcome": CALLING}
        log.append(conn, run_id, EventType.TASK_ACTION, payload)
    return payload if prior is not None else None


def execute(
    conn: psycopg.Connection,
    run: Run,
    retries: Iterable[dict[str, Any]],
    *,
    client_factory: Callable[[], Client] | None = None,
    durable: bool = True,
) -> list[dict[str, Any]]:
    """Carry out ``retries`` within the limits; log and return every outcome.

    ``durable`` (the loop's way) commits the connection's open transaction
    before each claim and outcome; see the module docstring. Tests that share
    a rolled-back connection pass False.
    """
    retries = list(retries)
    if not retries:
        return []
    if not enabled():
        # The switch was turned off between the decision and now.
        logger.info(
            "run %s: auto-retry is off; %s retries not made", run.id, len(retries)
        )
        return []
    factory = client_factory or default_client
    client: Client | None = None
    calls = 0
    outcomes = []
    for request in retries:
        base = {"action": "retry", "tool": TOOL, **request}
        if str(request.get("kind") or "").upper() in NEVER_RETRY_KINDS:
            outcomes.append(_record(conn, run.id, base, REFUSED_KIND, durable=durable))
            continue
        if calls >= config.SUPERVISOR_MAX_RETRIES_PER_RUN:
            outcomes.append(
                _record(
                    conn,
                    run.id,
                    base,
                    REFUSED_CAP,
                    durable=durable,
                    cap=config.SUPERVISOR_MAX_RETRIES_PER_RUN,
                )
            )
            continue
        refused = _claim(conn, run.id, base, durable=durable)
        if refused is not None:
            logger.info(
                "run %s: task %s already retried by run %s; refused",
                run.id,
                request["task_id"],
                refused["previous_run"],
            )
            outcomes.append(refused)
            continue
        calls += 1
        try:
            client = client or factory()
            answer = client.call_tool(
                TOOL, {"task_id": request["task_id"], "reason": request["reason"]}
            )
        except ToolError as err:
            outcome = _record(
                conn,
                run.id,
                base,
                REFUSED_PIC,
                durable=durable,
                detail=str(err)[:_DETAIL_CLIP],
            )
        except McpError as err:
            outcome = _record(
                conn,
                run.id,
                base,
                ERROR,
                durable=durable,
                detail=str(err)[:_DETAIL_CLIP],
            )
        except Exception as err:  # never let one task stop the rest
            logger.exception(
                "run %s: retry of task %s failed", run.id, request["task_id"]
            )
            outcome = _record(
                conn, run.id, base, ERROR, durable=durable, detail=type(err).__name__
            )
        else:
            task = answer.get("task") if isinstance(answer.get("task"), dict) else {}
            outcome = _record(
                conn,
                run.id,
                base,
                EXECUTED,
                durable=durable,
                status=task.get("status"),
                attempts=task.get("attempts"),
                pic_goal_key=task.get("goal_key"),
                replayed=bool(answer.get("replayed")),
                promoted=answer.get("promoted") or [],
            )
        logger.info(
            "run %s: retry of task %s: %s",
            run.id,
            request["task_id"],
            outcome["outcome"],
        )
        outcomes.append(outcome)
    return outcomes


def run_actions(conn: psycopg.Connection, run_id: int) -> list[dict[str, Any]]:
    """The final outcome per task of a run's retries, in order. A ``calling``
    with no outcome after it (a crash mid-call) is reported as such."""
    final: dict[Any, dict[str, Any]] = {}
    for event in log.load_events(conn, run_id):
        if event.type is EventType.TASK_ACTION:
            payload = event.payload or {}
            final[payload.get("task_id")] = payload
    return list(final.values())
