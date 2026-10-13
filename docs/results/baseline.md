# Baseline: driver and sandbox run metrics

Generated 2026-10-12 17:56 UTC by `python -m orchestrator.report baseline` (all runs).
Source: the managed-agents event log (`agent_runs` / `agent_events`).
Kinds, counts, durations and cost only; subjects and error text are
folded into fixed buckets.

## Driver sessions (`weekly_series_drive`)

| Metric | Value |
|---|---|
| Sessions | 27 |
| Duration p50 / p95, all timed sessions | 11m 53s / 18m 19s (n=15) |
| Duration p50 / p95, sessions that claimed tasks | 12m 21s / 18m 19s (n=12) |
| Sessions with no recoverable duration | 12 |
| Session outcomes | COMPLETE 17, INCOMPLETE 10 |
| Session failure reasons | – |
| Tasks claimed | 720 |
| Tasks reported | DONE 682, FAILED 38 |
| Task retries (claimed at attempt > 1) | 25 |
| Sessions re-leased after an abandon | 0 |

Sessions before 2026-09-08 wrote every event in one transaction, so
their events share one timestamp and their duration is not in the log.

### Tasks claimed by kind

| Kind | Claimed | DONE | FAILED | WAITING | SKIPPED | Failure reasons |
|---|---|---|---|---|---|---|
| `GENERATE_FEATURE_IMAGE` | 63 | 48 | 15 | 0 | 0 | http error 15 |
| `APPLY_ARTICLE_CORRECTIONS` | 55 | 52 | 3 | 0 | 0 | http error 3 |
| `ARTICLE_FACT_CHECK` | 55 | 52 | 3 | 0 | 0 | http error 3 |
| `WRITE_ARTICLE` | 54 | 53 | 1 | 0 | 0 | http error 1 |
| `GENERATE_TAGS` | 53 | 53 | 0 | 0 | 0 | – |
| `TRANSLATE` | 53 | 53 | 0 | 0 | 0 | – |
| `GENERATE_SERIES_SECTIONS` | 52 | 48 | 4 | 0 | 0 | success=false 4 |
| `APPROVE_IG_POST` | 49 | 49 | 0 | 0 | 0 | – |
| `RENDER_IG_POST` | 49 | 49 | 0 | 0 | 0 | – |
| `SCHEDULE_IG_POST` | 49 | 49 | 0 | 0 | 0 | – |
| `APPROVE_ARTICLE` | 48 | 48 | 0 | 0 | 0 | – |
| `SET_PUBLISH_DATE` | 48 | 48 | 0 | 0 | 0 | – |
| `PLAN_ARTICLES` | 32 | 32 | 0 | 0 | 0 | – |
| `GENERATE_SABIAS_QUE` | 17 | 17 | 0 | 0 | 0 | – |
| `GENERATE_OUTLINE` | 16 | 16 | 0 | 0 | 0 | – |
| `GENERATE_ARTICLE_PROMO` | 15 | 15 | 0 | 0 | 0 | – |
| `APPLY_RESEARCH_CORRECTIONS` | 12 | 0 | 12 | 0 | 0 | http error 12 |

## Sandbox runs by workload

Duration is sandbox wall time summed over attempts. Runs killed by the
login expiry are excluded here and counted under the pause below.

| Workload | Runs | p50 | p95 | Cost | Cost/run | Tokens in / out / cache read / cache write | Outcomes | Failure reasons | Retries |
|---|---|---|---|---|---|---|---|---|---|
| `memory_dream` | 134 | 5m 20s | 7m 39s | $366.54 | $2.74 | 13.8k / 2.4M / 345.2M / 13.5M | FINDINGS 92, ABANDONED 23, FAILED 10, AWAITING_HUMAN 8, CLEAN 1 | cancelled 23, sandbox failed 10 | 0 |
| `sentry_triage` | 26 | 8m 11s | 20m 04s | $69.00 | $2.65 | 10.4k / 418.3k / 64.5M / 2.3M | NOT_A_BUG 10, NEEDS_HUMAN 9, FIX 6, MITIGATION 1 | – | 2 |
| `adversarial_review` | 5 | 12m 31s | 17m 20s | $14.48 | $2.90 | 259 / 109.2k / 11.5M / 598.0k | STANDS 5 | – | 0 |
| `research_write` | 1 | 13m 26s | 13m 26s | $3.12 | $3.12 | 3.5k / 38.0k / 1.4M / 79.0k | WROTE 1 | – | 0 |
| `rubric_verify` | 1 | 1m 33s | 1m 33s | $0.68 | $0.68 | 10 / 6.3k / 196.6k / 42.5k | PASSED 1 | – | 0 |
| `smoke` | 1 | 10s | 10s | $0.08 | $0.08 | 4 / 112 / 36.0k / 5.6k | DONE 1 | – | 0 |

Total: 168 runs, $453.90, 28.0k / 2.9M / 423.0M / 16.5M tokens.

## Login-expiry pauses

Dispatch pauses while the Claude login is dead (docs/runbook.md, "When
the login dies anyway"). These windows are gaps in sandbox work, not
failures of it. The driver needs no Claude login and is not gated by them.

| From | To (next healthy run) | Days | Runs failed on auth |
|---|---|---|---|
| 2026-09-24 04:15 UTC | 2026-10-05 16:19 UTC | 11.5 | 1 |

## Weekly activity

| ISO week | Driver sessions | Sandbox runs | Cost | Note |
|---|---|---|---|---|
| 2026-W30 | 0 | 5 | $20.71 |  |
| 2026-W31 | 0 | 6 | $10.51 |  |
| 2026-W32 | 1 | 16 | $27.11 |  |
| 2026-W33 | 1 | 16 | $42.80 |  |
| 2026-W34 | 1 | 15 | $38.73 |  |
| 2026-W35 | 1 | 14 | $24.46 |  |
| 2026-W36 | 10 | 16 | $56.62 |  |
| 2026-W37 | 5 | 16 | $64.28 |  |
| 2026-W38 | 8 | 14 | $77.51 |  |
| 2026-W39 | 0 | 16 | $52.31 | login-expiry pause |
| 2026-W40 | 0 | 15 | $1.79 | login-expiry pause |
| 2026-W41 | 0 | 11 | $25.15 | login-expiry pause |
| 2026-W42 | 0 | 9 | $11.92 |  |
