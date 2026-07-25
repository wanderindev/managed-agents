#!/usr/bin/env bash
# Run one job in a disposable sandbox. This is the shape the sandbox runner (#5)
# will drive programmatically; having it as a script first means the container
# contract can be exercised by hand before any orchestration exists.
#
#   scripts/run-sandbox.sh <worktree-path> <prompt>
#   AGENT_WITH_DOCKER=1 scripts/run-sandbox.sh <worktree-path> <prompt>
set -euo pipefail

WORKTREE="${1:?usage: run-sandbox.sh <worktree-path> <prompt>}"
PROMPT="${2:?usage: run-sandbox.sh <worktree-path> <prompt>}"

IMAGE="${SANDBOX_IMAGE:-managed-agents/sandbox:latest}"
CREDENTIALS="${CLAUDE_CREDENTIALS:-$HOME/.claude/.credentials.json}"

[ -d "$WORKTREE" ] || { echo "no such worktree: $WORKTREE" >&2; exit 1; }
[ -s "$CREDENTIALS" ] || { echo "no credential at $CREDENTIALS" >&2; exit 78; }

args=(
    run --rm
    --init                      # reap zombies; claude spawns child processes
    --network bridge
    --memory 3g --memory-swap 4g
    --pids-limit 512
    --volume "$(realpath "$WORKTREE"):/workspace"
    # Mounted read-write on purpose: Claude Code refreshes its OAuth token, and
    # a read-only mount works right up until the access token expires and then
    # every unattended run fails at once.
    --volume "$(realpath "$CREDENTIALS"):/home/agent/.claude/.credentials.json"
)

# Opt-in, not default. See docs/runbook.md: this hands the job control of the
# host's Docker daemon, which is how it can run testcontainers suites and also
# why the container stops being a real security boundary.
if [ "${AGENT_WITH_DOCKER:-0}" = "1" ]; then
    args+=(--volume /var/run/docker.sock:/var/run/docker.sock --group-add "$(getent group docker | cut -d: -f3)")
fi

for var in AGENT_MODEL AGENT_MAX_TURNS AGENT_PERMISSION_MODE AGENT_FALLBACK_MODEL GH_TOKEN; do
    [ -n "${!var:-}" ] && args+=(--env "$var")
done

exec docker "${args[@]}" "$IMAGE" "$PROMPT"
