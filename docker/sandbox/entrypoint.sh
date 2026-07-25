#!/usr/bin/env bash
# Sandbox entrypoint. Runs one Claude Code job and streams its events to stdout,
# where the sandbox runner (#5) reads them line by line into agent_events.
#
# The prompt comes from, in order: argv, $AGENT_PROMPT, or /work/prompt.txt.
# A file is the sane choice for anything long enough to trip over shell quoting.
set -euo pipefail

CREDENTIALS="${HOME}/.claude/.credentials.json"
if [ ! -s "$CREDENTIALS" ]; then
    echo "FATAL: no Claude credential at ${CREDENTIALS}." >&2
    echo "Mount the host's file into the container; see docs/runbook.md." >&2
    exit 78  # EX_CONFIG
fi

# Claude Code will not run non-interactively until onboarding is marked done.
# Written here rather than mounted, so the host's ~/.claude.json (machine id,
# feature caches, and every project it has ever opened) never enters a sandbox.
if [ ! -f "${HOME}/.claude.json" ]; then
    printf '{"hasCompletedOnboarding":true}\n' > "${HOME}/.claude.json"
fi

PROMPT="${1:-${AGENT_PROMPT:-}}"
if [ -z "$PROMPT" ] && [ -f /work/prompt.txt ]; then
    PROMPT="$(cat /work/prompt.txt)"
fi
if [ -z "$PROMPT" ]; then
    echo "FATAL: no prompt (argv, \$AGENT_PROMPT, or /work/prompt.txt)" >&2
    exit 64  # EX_USAGE
fi

# bypassPermissions is the right default *here* and nowhere else: the container
# is disposable, holds one repo worktree, and an interactive approval prompt in
# an unattended job just deadlocks. Note the honest caveat in docs/runbook.md
# about what mounting the Docker socket does to that boundary.
args=(
    -p "$PROMPT"
    --output-format stream-json
    --verbose
    --permission-mode "${AGENT_PERMISSION_MODE:-bypassPermissions}"
)
[ -n "${AGENT_MODEL:-}" ] && args+=(--model "$AGENT_MODEL")
[ -n "${AGENT_MAX_TURNS:-}" ] && args+=(--max-turns "$AGENT_MAX_TURNS")
[ -n "${AGENT_FALLBACK_MODEL:-}" ] && args+=(--fallback-model "$AGENT_FALLBACK_MODEL")

# The sandbox bounds its own wall clock rather than relying on a watchdog on the
# host. A container that limits itself cannot be orphaned by an orchestrator that
# died, and `timeout` exits 124 so the outcome is identifiable rather than just
# "failed". Wall clock, never an event count: a healthy long run can emit very
# few events and a stuck one can emit thousands.
if [ -n "${AGENT_TIMEOUT_SECONDS:-}" ]; then
    exec timeout -s TERM "$AGENT_TIMEOUT_SECONDS" claude "${args[@]}"
fi

exec claude "${args[@]}"
