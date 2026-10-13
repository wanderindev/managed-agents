# Pipeline supervisor (phase 1): time to diagnosis and agreement

The measure for managed-agents #81 (epic #85, PIC #640): for the first ten
incidents, the time from a pipeline failure to the supervisor's diagnosis, and
whether its recommended action matches the operator's own call.

- **Time to diagnosis** comes from the event log. Regenerate the incident table
  on the agents droplet with `python -m orchestrator.report supervisor`.
  "Failure seen" is when the log first showed the condition: the end of the
  drive session that parked the rows (`driver_failed`), or stalled-since plus
  the threshold (`stalled`, default 3 days).
- **Agreement** is the operator's call, recorded by hand. For each incident,
  write down what you would have done (retry / skip / operator work /
  nothing) before or without reading the diagnosis, then whether the two
  match. Goal keys and actions only: no article text, no error bodies.

## Incidents

No incidents yet: nothing has been diagnosed from the log so far. The driver
has not run since 2026-09-20 (there is no `orchestrator.driver` cron entry),
so the `driver_failed` trigger cannot fire until it runs again. The `stalled`
trigger works from the existing log.

| # | Run | Goal | Trigger | Failure seen | Diagnosed | Failure → diagnosis | Recommended | Operator's call | Agree? |
|---|---|---|---|---|---|---|---|---|---|
| 1 | | | | | | | | | |
| 2 | | | | | | | | | |
| 3 | | | | | | | | | |
| 4 | | | | | | | | | |
| 5 | | | | | | | | | |
| 6 | | | | | | | | | |
| 7 | | | | | | | | | |
| 8 | | | | | | | | | |
| 9 | | | | | | | | | |
| 10 | | | | | | | | | |

Agreement: – of – (fill in after ten incidents).

## Acceptance dry run (2026-10-12, not an incident)

This run used the branch's code from a scratch copy and a scratch image tag on
the agents droplet. The triggers came from the production log through
read-only SELECTs; nothing was written to the log, so the run does not appear
in the report above. It called PIC's real read tools with the
`orchestrator-sandbox-read` token.

- The scan found two `stalled` goals in the log: `daily_series:2026-09-18`
  (unchanged since 2026-09-18 09:09 UTC) and `daily_series:2026-09-19`
  (unchanged since 2026-09-20 09:03 UTC, two `GENERATE_FEATURE_IMAGE` rows
  parked on an image-API 403).
- Diagnosis of `daily_series:2026-09-19`: 4 turns, 21 s, US$0.14. Tools used:
  `explain_stuck_goal`, `pipeline_status`, `Write`. Recommended action
  **none**, confidence high. PIC reports the goal finished (45/45 tasks DONE
  or SKIPPED, no blockers, no capacity slot held), with activity after the
  log's last entry for it. The diagnosis also flagged that the image-API 403
  would fail future image tasks if it was never fixed. This is correct: PIC
  shows no goal in flight.
- Why the log called the goal stalled: drive sessions from before #62 recorded
  only one goal each, so the session that finished 2026-09-19's rows (logged
  under 2026-09-20) left no trace on it. Sessions since #62 list every goal
  they touch. These old sessions leave the 30-day lookback on 2026-10-20.
- Write-tool probe, run with the same job spec and the same image: the
  session's tools were `Write` plus the read tools (and `ping`); PIC's
  retry, skip, publish and research tools and `Bash` were absent, and the
  session ran with `permissionMode: dontAsk`. Asked to call
  `retry_agent_task`, `skip_agent_task` and Bash, it could not. Nothing
  reached PIC.
