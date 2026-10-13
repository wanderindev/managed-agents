"""Read-only reports over the event log (#79).

    python -m orchestrator.report baseline [--since YYYY-MM-DD]
    python -m orchestrator.report supervisor

``baseline`` is the "before" picture for PIC's MCP work (PIC #620, our epic
#85): how the deterministic series driver and the sandboxed agents behave
today, folded from ``agent_runs`` / ``agent_events`` and printed as Markdown.

``supervisor`` is the pipeline supervisor's measure (#81): per diagnosis, the
time from the failure the log shows to the diagnosis, and the recommended
action, with empty columns for the operator's own call and whether the two
agree. Goal keys (dates) and actions only; never the diagnosis prose.

Two rules shape it:

* **Read-only.** One SELECT per table, no writes, no locks worth naming.
* **Public output.** This repo is public and the output is pasted into PIC's
  docs, so only kinds, counts, durations, cost and *categorised* reasons are
  printed. Subjects, task error text, transcripts and abandon reasons (which
  can carry container names or operator prose) are folded into fixed buckets
  and never echoed.

A dead Claude login (``run_failed`` with ``reason: auth``) is a host condition,
not a verdict on the work: the runs it killed are reported as a pause window
and left out of the failure counts. The window opens at the first auth failure
and closes when the next sandbox run finishes without one.
"""

import argparse
import math
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg

from orchestrator import auth, driver, supervisor
from orchestrator.db import connect
from orchestrator.enums import EventType
from orchestrator.sandbox import TIMEOUT_EXIT_CODE

#: Events the report needs. Claude events are kept only when they are the
#: ``result`` line, which is where the CLI puts cost and token usage.
_EVENTS_SQL = """
SELECT e.run_id, e.seq, e.type, e.payload, e.created_at
FROM agent_events e
JOIN agent_runs r ON r.id = e.run_id
WHERE r.created_at >= %(since)s
  AND (e.type <> %(claude)s OR e.payload->>'type' = 'result')
ORDER BY e.run_id, e.seq
"""

_RUNS_SQL = """
SELECT id, kind, status, attempts, created_at
FROM agent_runs
WHERE created_at >= %(since)s
ORDER BY id
"""

_TOKEN_KEYS = (
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
)

_TERMINAL = {EventType.RUN_DONE.value, EventType.RUN_FAILED.value}


@dataclass
class RunFacts:
    """Everything the report needs about one run, folded from its events."""

    id: int
    kind: str
    status: str
    attempts: int
    created_at: datetime
    ended_at: datetime | None = None
    #: Seconds a sandbox was actually up, summed over attempts.
    sandbox_seconds: float = 0.0
    cost_usd: float = 0.0
    tokens: Counter = field(default_factory=Counter)
    #: The work's own verdict: FIX / NOT_A_BUG / NEEDS_HUMAN / WROTE /
    #: STANDS / passed ... or for a driver session COMPLETE / INCOMPLETE ...
    outcome: str | None = None
    #: Why it failed, as a fixed bucket (``auth``, ``timeout`` ...), if it did.
    failure: str | None = None
    #: Abandons that were requeued (a retry followed).
    requeues: int = 0
    #: Driver sessions only: one entry per claimed task.
    tasks: list[dict[str, Any]] = field(default_factory=list)

    @property
    def duration(self) -> float | None:
        """Creation to terminal event, or ``None`` when the log cannot say.

        Event timestamps are ``now()``, the *transaction's* start. Drive
        sessions before 2026-09-08 ran in one transaction (fixed by turning on
        autocommit in ``driver.main``), so every event shares the creation
        time: a session that claimed tasks yet "took 0s" is one of those.
        """
        if self.ended_at is None:
            return None
        if self.ended_at == self.created_at and self.tasks:
            return None
        return (self.ended_at - self.created_at).total_seconds()

    @property
    def auth_failed(self) -> bool:
        return self.failure == auth.REASON_AUTH


# --- folding -----------------------------------------------------------------


def abandon_bucket(reason: str | None) -> str:
    """A fixed label for a ``run_abandoned`` reason; the raw text is not printed."""
    text = (reason or "").lower()
    if text.startswith("lease expired"):
        return "lease expired"
    if text.startswith("sandbox vanished"):
        return "sandbox vanished"
    if text.startswith("sandbox never started"):
        return "sandbox never started"
    if text.startswith("sandbox failed to start"):
        return "sandbox failed to start"
    if text.startswith("superseded"):
        return "superseded"
    return "other"


def task_error_bucket(error: str | None) -> str:
    """A fixed label for a driver task's error text (which can name subjects)."""
    text = (error or "").lower()
    if "timeout" in text or "timed out" in text or re.search(r" after \d+s$", text):
        return "timeout"
    if "transport failure" in text:
        return "transport"
    if text.startswith("report answered"):
        return "report rejected"
    if re.search(r"job \d+ failed", text):
        return "job failed"
    if re.search(r"answered \d{3}", text):
        return "http error"
    if "success=false" in text:
        return "success=false"
    if "satisfied flag" in text:
        return "wait-check malformed"
    return "other"


def _result_outcome(result: Any) -> str | None:
    if not isinstance(result, dict):
        return None
    for key in ("outcome", "verdict"):
        if isinstance(result.get(key), str):
            return result[key]
    if "passed" in result:
        return "PASSED" if result["passed"] else "NOT_PASSED"
    return None


def fold(runs: Iterable[dict], events: Iterable[dict]) -> list[RunFacts]:
    """Fold raw rows into :class:`RunFacts`, one per run, in run order."""
    by_run: dict[int, list[dict]] = defaultdict(list)
    for event in events:
        by_run[event["run_id"]].append(event)
    facts: list[RunFacts] = []
    for row in runs:
        run = RunFacts(
            id=row["id"],
            kind=row["kind"],
            status=row["status"],
            attempts=row["attempts"],
            created_at=row["created_at"],
        )
        started: datetime | None = None
        for event in sorted(by_run.get(run.id, []), key=lambda e: e["seq"]):
            _apply(run, event, started)
            if event["type"] == EventType.SANDBOX_STARTED.value:
                started = event["created_at"]
            elif event["type"] in (
                EventType.STAGE_COMPLETED.value,
                EventType.RUN_ABANDONED.value,
            ):
                started = None
        facts.append(run)
    return facts


def _apply(run: RunFacts, event: dict, started: datetime | None) -> None:
    kind, payload, at = event["type"], event["payload"] or {}, event["created_at"]
    if kind == EventType.CLAUDE_EVENT.value:
        cost = payload.get("total_cost_usd")
        if isinstance(cost, int | float):
            run.cost_usd += cost
        usage = payload.get("usage")
        if isinstance(usage, dict):
            for key in _TOKEN_KEYS:
                if isinstance(usage.get(key), int):
                    run.tokens[key] += usage[key]
    elif kind == EventType.STAGE_COMPLETED.value:
        if started is not None:
            run.sandbox_seconds += (at - started).total_seconds()
        outcome = _result_outcome(payload.get("result"))
        if outcome:
            run.outcome = outcome
        if payload.get("exit_code") == TIMEOUT_EXIT_CODE:
            run.failure = "timeout"
    elif kind == EventType.ARTIFACT.value and run.kind == driver.RUN_KIND:
        run.tasks.append(payload)
    elif kind == EventType.RUN_ABANDONED.value:
        if started is not None:
            run.sandbox_seconds += (at - started).total_seconds()
        if payload.get("requeued"):
            run.requeues += 1
        else:
            run.ended_at = at
            run.outcome = run.outcome or "ABANDONED"
            run.failure = (
                "cancelled"
                if payload.get("cancelled")
                else "abandoned: " + abandon_bucket(payload.get("reason"))
            )
    elif kind in _TERMINAL:
        run.ended_at = at
        if kind == EventType.RUN_FAILED.value:
            reason = payload.get(auth.REASON_KEY)
            if isinstance(reason, str) and reason:
                run.failure = reason
            elif run.failure is None:
                run.failure = "sandbox failed"


# --- the pause ---------------------------------------------------------------


@dataclass(frozen=True)
class Pause:
    start: datetime
    end: datetime | None
    auth_failed_runs: int

    @property
    def days(self) -> float | None:
        if self.end is None:
            return None
        return (self.end - self.start).total_seconds() / 86400


def pauses(facts: list[RunFacts]) -> list[Pause]:
    """Login-expiry windows: first auth failure to the next healthy finish."""
    finished = sorted(
        (r for r in facts if r.kind != driver.RUN_KIND and r.ended_at is not None),
        key=lambda r: r.ended_at,
    )
    windows: list[Pause] = []
    start: datetime | None = None
    count = 0
    for run in finished:
        if run.auth_failed:
            if start is None:
                start = run.ended_at
            count += 1
        elif start is not None and _ran(run):
            windows.append(Pause(start, run.ended_at, count))
            start, count = None, 0
    if start is not None:
        windows.append(Pause(start, None, count))
    return windows


def _ran(run: RunFacts) -> bool:
    """True when the run got a working CLI: it finished by itself, auth aside.

    A cancel or an abandon proves nothing about the login, so it cannot close
    a pause window; any other ending, even a failure, means the CLI ran.
    """
    return run.failure is None or not run.failure.startswith(("cancelled", "abandoned"))


# --- formatting --------------------------------------------------------------


def percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile; ``None`` for no data."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "–"
    seconds = round(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def fmt_mix(counter: Counter) -> str:
    if not counter:
        return "–"
    return ", ".join(f"{k} {v}" for k, v in sorted(counter.items(), key=_by_count))


def _p50_p95(values: list[float]) -> str:
    return (
        f"{fmt_duration(percentile(values, 50))} / "
        f"{fmt_duration(percentile(values, 95))} (n={len(values)})"
    )


def _by_count(item: tuple[str, int]) -> tuple[int, str]:
    return (-item[1], item[0])


def _table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    out = ["| " + " | ".join(headers) + " |"]
    out.append("|" + "|".join("---" for _ in headers) + "|")
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return out


def _fmt_ts(at: datetime | None) -> str:
    return at.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC") if at else "ongoing"


# --- the report ----------------------------------------------------------------


def render(facts: list[RunFacts], *, since: date | None, generated: datetime) -> str:
    drives = [r for r in facts if r.kind == driver.RUN_KIND]
    sandboxed = [r for r in facts if r.kind != driver.RUN_KIND]
    windows = pauses(facts)
    lines = [
        "# Baseline: driver and sandbox run metrics",
        "",
        f"Generated {_fmt_ts(generated)} by `python -m orchestrator.report baseline`"
        + (f" (runs created since {since.isoformat()})." if since else " (all runs)."),
        "Source: the managed-agents event log (`agent_runs` / `agent_events`).",
        "Kinds, counts, durations and cost only; subjects and error text are",
        "folded into fixed buckets.",
        "",
    ]
    lines += _driver_section(drives)
    lines += _sandbox_section(sandboxed)
    lines += _pause_section(windows)
    lines += _weekly_section(facts, windows)
    return "\n".join(lines).rstrip() + "\n"


def _driver_section(drives: list[RunFacts]) -> list[str]:
    lines = ["## Driver sessions (`weekly_series_drive`)", ""]
    if not drives:
        return lines + ["No driver sessions in range.", ""]
    durations = [d for r in drives if (d := r.duration) is not None]
    working = [d for r in drives if r.tasks and (d := r.duration) is not None]
    unknown = sum(1 for r in drives if r.ended_at is not None and r.duration is None)
    outcomes = Counter(r.outcome or r.status for r in drives)
    failures = Counter(r.failure for r in drives if r.failure)
    tasks = [t for r in drives for t in r.tasks]
    retried = sum(
        1 for t in tasks if isinstance(t.get("attempt"), int) and t["attempt"] > 1
    )
    lines += _table(
        ["Metric", "Value"],
        [
            ["Sessions", len(drives)],
            ["Duration p50 / p95, all timed sessions", _p50_p95(durations)],
            ["Duration p50 / p95, sessions that claimed tasks", _p50_p95(working)],
            ["Sessions with no recoverable duration", unknown],
            ["Session outcomes", fmt_mix(outcomes)],
            ["Session failure reasons", fmt_mix(failures)],
            ["Tasks claimed", len(tasks)],
            [
                "Tasks reported",
                fmt_mix(Counter(t.get("reported") or "?" for t in tasks)),
            ],
            ["Task retries (claimed at attempt > 1)", retried],
            ["Sessions re-leased after an abandon", sum(r.requeues for r in drives)],
        ],
    )
    if unknown:
        lines += [
            "",
            "Sessions before 2026-09-08 wrote every event in one transaction, so",
            "their events share one timestamp and their duration is not in the log.",
        ]
    lines += ["", "### Tasks claimed by kind", ""]
    per_kind: dict[str, Counter] = defaultdict(Counter)
    errors: dict[str, Counter] = defaultdict(Counter)
    for task in tasks:
        name = task.get("kind") or "?"
        per_kind[name][task.get("reported") or "?"] += 1
        if task.get("error"):
            errors[name][task_error_bucket(task["error"])] += 1
    statuses = ["DONE", "FAILED", "WAITING", "SKIPPED"]
    rows = []
    for name in sorted(per_kind, key=lambda k: (-sum(per_kind[k].values()), k)):
        counts = per_kind[name]
        rows.append(
            [f"`{name}`", sum(counts.values())]
            + [counts.get(s, 0) for s in statuses]
            + [fmt_mix(errors[name])]
        )
    lines += _table(["Kind", "Claimed", *statuses, "Failure reasons"], rows)
    return lines + [""]


def _sandbox_section(runs: list[RunFacts]) -> list[str]:
    lines = ["## Sandbox runs by workload", ""]
    if not runs:
        return lines + ["No sandbox runs in range.", ""]
    lines += [
        "Duration is sandbox wall time summed over attempts. Runs killed by the",
        "login expiry are excluded here and counted under the pause below.",
        "",
    ]
    by_kind: dict[str, list[RunFacts]] = defaultdict(list)
    for run in runs:
        if not run.auth_failed:
            by_kind[run.kind].append(run)
    rows = []
    totals = Counter()
    total_cost = 0.0
    for name in sorted(by_kind, key=lambda k: (-len(by_kind[k]), k)):
        group = by_kind[name]
        durations = [r.sandbox_seconds for r in group if r.sandbox_seconds > 0]
        cost = sum(r.cost_usd for r in group)
        tokens = Counter()
        for r in group:
            tokens.update(r.tokens)
        totals.update(tokens)
        total_cost += cost
        rows.append(
            [
                f"`{name}`",
                len(group),
                fmt_duration(percentile(durations, 50)),
                fmt_duration(percentile(durations, 95)),
                f"${cost:,.2f}",
                f"${cost / len(group):,.2f}",
                _fmt_tokens(tokens),
                fmt_mix(Counter(r.outcome or r.status for r in group)),
                fmt_mix(Counter(r.failure for r in group if r.failure)),
                sum(r.requeues for r in group),
            ]
        )
    lines += _table(
        [
            "Workload",
            "Runs",
            "p50",
            "p95",
            "Cost",
            "Cost/run",
            "Tokens in / out / cache read / cache write",
            "Outcomes",
            "Failure reasons",
            "Retries",
        ],
        rows,
    )
    counted = sum(len(g) for g in by_kind.values())
    lines += [
        "",
        f"Total: {counted} runs, ${total_cost:,.2f}, {_fmt_tokens(totals)} tokens.",
        "",
    ]
    return lines


def _fmt_tokens(tokens: Counter) -> str:
    return " / ".join(_short(tokens.get(k, 0)) for k in _TOKEN_KEYS)


def _short(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


def _pause_section(windows: list[Pause]) -> list[str]:
    lines = ["## Login-expiry pauses", ""]
    if not windows:
        return lines + ["No runs failed on the Claude login in range.", ""]
    lines += [
        'Dispatch pauses while the Claude login is dead (docs/runbook.md, "When',
        'the login dies anyway"). These windows are gaps in sandbox work, not',
        "failures of it. The driver needs no Claude login and is not gated by them.",
        "",
    ]
    rows = [
        [
            _fmt_ts(p.start),
            _fmt_ts(p.end),
            f"{p.days:.1f}" if p.days is not None else "–",
            p.auth_failed_runs,
        ]
        for p in windows
    ]
    lines += _table(
        ["From", "To (next healthy run)", "Days", "Runs failed on auth"], rows
    )
    return lines + [""]


def _weekly_section(facts: list[RunFacts], windows: list[Pause]) -> list[str]:
    lines = ["## Weekly activity", ""]
    weeks: dict[str, Counter] = defaultdict(Counter)
    starts: dict[str, date] = {}
    for run in facts:
        iso = run.created_at.astimezone(UTC).isocalendar()
        key = f"{iso.year}-W{iso.week:02d}"
        starts[key] = date.fromisocalendar(iso.year, iso.week, 1)
        weeks[key]["drive" if run.kind == driver.RUN_KIND else "sandbox"] += 1
        weeks[key]["cost"] += run.cost_usd
    if not weeks:
        return lines + ["No runs in range.", ""]
    rows = []
    for key in sorted(weeks):
        monday = datetime.combine(starts[key], datetime.min.time(), tzinfo=UTC)
        paused = any(
            p.start < monday + timedelta(days=7) and (p.end is None or p.end >= monday)
            for p in windows
        )
        rows.append(
            [
                key,
                weeks[key]["drive"],
                weeks[key]["sandbox"],
                f"${weeks[key]['cost']:,.2f}",
                "login-expiry pause" if paused else "",
            ]
        )
    lines += _table(
        ["ISO week", "Driver sessions", "Sandbox runs", "Cost", "Note"], rows
    )
    return lines + [""]


# --- the pipeline supervisor's measure (#81) ------------------------------------


def render_supervisor(rows: list[dict], *, generated: datetime) -> str:
    """Time from failure to diagnosis, per incident, as a Markdown table.

    The operator fills in the last two columns (their own call, and whether
    it matches) for the first ten incidents; that is the agreement measure.
    """
    lines = [
        "# Pipeline supervisor: time to diagnosis and agreement",
        "",
        f"Generated {_fmt_ts(generated)} by `python -m orchestrator.report supervisor`.",
        "Source: the managed-agents event log (`pipeline_supervise` runs).",
        "Failure seen = when the log first showed the condition: the drive",
        "session's end (driver_failed) or stalled-since + the threshold",
        "(stalled). Manual diagnoses have no failure time.",
        "",
    ]
    table: list[list[Any]] = []
    latencies: list[float] = []
    for row in rows:
        queued = row.get("queued") or {}
        art = row.get("artifact") or {}
        found = art.get("diagnosis") or {}
        failed_at = supervisor.parse_ts(queued.get("failed_at"))
        diagnosed_at = row.get("diagnosed_at")
        latency = (
            (diagnosed_at - failed_at).total_seconds()
            if failed_at and diagnosed_at
            else None
        )
        if latency is not None:
            latencies.append(latency)
        enqueue_latency = (
            (diagnosed_at - row["enqueued_at"]).total_seconds()
            if diagnosed_at and row.get("enqueued_at")
            else None
        )
        action = found.get("recommended_action")
        table.append(
            [
                row["id"],
                f"`{queued.get('goal_key') or row.get('subject')}`",
                queued.get("trigger") or "?",
                _fmt_ts(failed_at) if failed_at else "–",
                _fmt_ts(diagnosed_at)
                if diagnosed_at
                else f"no diagnosis ({row['status']})",
                fmt_duration(latency),
                fmt_duration(enqueue_latency),
                supervisor.ACTION_LABELS.get(action, "–") if action else "–",
                "",
                "",
            ]
        )
    if not table:
        lines += ["No diagnoses in the log yet.", ""]
        return "\n".join(lines)
    diagnosed = sum(1 for r in rows if r.get("diagnosed_at"))
    lines += [
        f"Diagnoses: {diagnosed} of {len(rows)} run(s).",
        f"Failure to diagnosis p50 / p95: {_p50_p95(latencies)}",
        "",
    ]
    lines += _table(
        [
            "Run",
            "Goal",
            "Trigger",
            "Failure seen",
            "Diagnosed",
            "Failure → diagnosis",
            "Enqueue → diagnosis",
            "Recommended",
            "Operator's call",
            "Agree?",
        ],
        table,
    )
    return "\n".join(lines) + "\n"


# --- the command -----------------------------------------------------------------


def load(conn: psycopg.Connection, since: date | None) -> list[RunFacts]:
    bound = datetime.combine(since or date(1970, 1, 1), datetime.min.time(), tzinfo=UTC)
    params = {"since": bound, "claude": EventType.CLAUDE_EVENT.value}
    with conn.cursor() as cur:
        cur.execute(_RUNS_SQL, params)
        runs = cur.fetchall()
        cur.execute(_EVENTS_SQL, params)
        events = cur.fetchall()
    conn.rollback()
    return fold(runs, events)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m orchestrator.report",
        description="Read-only Markdown reports over the event log.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    baseline = sub.add_parser("baseline", help="driver and sandbox run metrics (#79)")
    baseline.add_argument(
        "--since",
        type=date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="only runs created on or after this UTC date",
    )
    sub.add_parser(
        "supervisor", help="pipeline supervisor: time to diagnosis, agreement (#81)"
    )
    args = parser.parse_args(argv)
    if args.command == "supervisor":
        with connect() as conn:
            rows = supervisor.load_incidents(conn)
        print(render_supervisor(rows, generated=datetime.now(UTC)), end="")
        return 0
    with connect() as conn:
        facts = load(conn, args.since)
    print(render(facts, since=args.since, generated=datetime.now(UTC)), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
