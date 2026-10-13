#!/bin/bash
# Start the orchestrator main loop if it is not running. Safe to run often:
# the loop is stateless and designed to be killed and restarted at any time.
# Installed on the host as ~/bin/orch-loop-keepalive.sh, run by cron every
# 5 minutes (docs/runbook.md, "Cron").
#
# The check matches the loop process itself — argv[0] a python binary (the
# relative `.venv/bin/python` this script uses, or an absolute path when the
# loop was started by hand), then `-m orchestrator.main` — never a mere mention
# of it. A bare `pgrep -f orchestrator.main` also matched an ssh/bash wait
# command whose text named the module, and kept the loop down for 20 minutes
# (#72). Anchoring on the relative path alone would miss a hand-started loop
# and start a second one.
#
# Paths are overridable for tests; the defaults are the host's.
ORCH_DIR="${ORCH_DIR:-/srv/orchestrator}"
ORCH_ENV_FILE="${ORCH_ENV_FILE:-/srv/orchestrator.env}"
ORCH_LOG_DIR="${ORCH_LOG_DIR:-$HOME/logs}"
LOOP_PATTERN='^([^ ]*/)?python[0-9.]*( -[^ ]+)* -m orchestrator\.main( |$)'

pgrep -f "$LOOP_PATTERN" >/dev/null && exit 0
cd "$ORCH_DIR" || exit 1
set -a; . "$ORCH_ENV_FILE"; set +a
nohup .venv/bin/python -m orchestrator.main >> "$ORCH_LOG_DIR/orchestrator-main.log" 2>&1 &
echo "$(date -u +%FT%TZ) keepalive: started orchestrator.main pid $!" >> "$ORCH_LOG_DIR/keepalive.log"
