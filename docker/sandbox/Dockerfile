# Disposable sandbox: one container per run. See issues #2 and #5.
#
# Holds the tooling a triage job needs and nothing else. The repo clone, the
# Claude credential, and any job-scoped secrets are mounted or injected at run
# time, never baked in.
FROM node:22-bookworm-slim

# Pinned deliberately. Provisioning the host taught us that an unpinned install
# fails open: on Node 20, `npm install -g` printed an EBADENGINE warning and
# silently installed the last compatible release, stranding the host on an old
# CLI that could not reach current models. Bumping this is a one-line, reviewed
# change rather than a surprise on the next rebuild.
ARG CLAUDE_CODE_VERSION=2.1.220

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg git jq less procps ripgrep unzip \
        python3 python3-venv python3-pip \
    && rm -rf /var/lib/apt/lists/*

# GitHub CLI: the fix stage (#7) opens PRs with it.
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        -o /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends gh \
    && rm -rf /var/lib/apt/lists/*

# Docker CLI only, no daemon. feliu-dev's suite drives testcontainers and PIC's
# commands run through compose, so a job needs to talk to a Docker daemon. The
# socket is a *run time* decision (see docs/runbook.md); the client belongs here.
RUN install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg \
        -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}"

# uid 1000 matches the host's `wanderindev`, so a bind-mounted workspace stays
# writable without chown dances. The base image ships its own uid-1000 user.
RUN userdel -r node \
    && useradd --create-home --uid 1000 --shell /bin/bash agent \
    && install -d -o agent -g agent /workspace /home/agent/.claude

COPY entrypoint.sh /usr/local/bin/run-agent
RUN chmod 0755 /usr/local/bin/run-agent

# Fail the build rather than ship a sandbox that cannot run the CLI.
RUN test "$(node --version | sed 's/^v\([0-9]*\).*/\1/')" -ge 22 \
    && claude --version \
    && git --version \
    && gh --version | head -1 \
    && docker --version

USER agent
WORKDIR /workspace

LABEL org.opencontainers.image.source="https://github.com/wanderindev/managed-agents" \
      org.opencontainers.image.description="Disposable Claude Code sandbox for the managed-agents orchestrator"

ENTRYPOINT ["/usr/local/bin/run-agent"]
