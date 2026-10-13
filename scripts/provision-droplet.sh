#!/usr/bin/env bash
# Provision the orchestrator host. See issue #2.
#
#   scp scripts/provision-droplet.sh root@<host>:/root/
#   ssh root@<host> 'bash /root/provision-droplet.sh'
#
# Idempotent: safe to re-run after a partial failure or to pick up a newer
# Claude Code release.
set -euo pipefail

# Claude Code requires Node >= 22. On Node 20 npm silently installs the last
# 20-compatible release instead of failing, which strands the host on an old
# CLI that cannot reach current models. This bit is load-bearing.
NODE_MAJOR=22

echo "=== waiting for cloud-init's apt to finish ==="
# A freshly created droplet holds the dpkg lock for the first minute or two.
# Without this the very first run dies on "could not get lock".
for _ in $(seq 1 60); do
    fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 || break
    sleep 10
done

echo "=== swap ==="
# 4GB of RAM, and a sandbox running a test suite with its own Postgres
# container spikes hard. Swap is cheaper than debugging an OOM kill.
if [ ! -f /swapfile ]; then
    fallocate -l 2G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile >/dev/null
    swapon /swapfile
    grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

echo "=== base packages ==="
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq \
    ca-certificates curl gnupg git jq unzip \
    python3 python3-venv python3-pip \
    postgresql-client >/dev/null

echo "=== docker ==="
if ! command -v docker >/dev/null; then
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    . /etc/os-release
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
        > /etc/apt/sources.list.d/docker.list
    apt-get update -qq
    apt-get install -y -qq \
        docker-ce docker-ce-cli containerd.io \
        docker-buildx-plugin docker-compose-plugin >/dev/null
fi
systemctl enable --now docker >/dev/null

echo "=== node ${NODE_MAJOR} ==="
current_major="$(node --version 2>/dev/null | sed 's/^v\([0-9]*\).*/\1/')"
if [ "${current_major:-0}" -lt "$NODE_MAJOR" ]; then
    curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | bash - >/dev/null
    apt-get install -y -qq nodejs >/dev/null
fi

echo "=== claude code ==="
# Deliberately not silenced. An EBADENGINE warning here is the difference
# between the latest CLI and a stale one, and it is worth seeing.
npm install -g @anthropic-ai/claude-code@latest

echo "=== operator user ==="
if ! id wanderindev >/dev/null 2>&1; then
    adduser --disabled-password --gecos "" wanderindev >/dev/null
    usermod -aG docker wanderindev
    mkdir -p /home/wanderindev/.ssh
    cp /root/.ssh/authorized_keys /home/wanderindev/.ssh/authorized_keys
    chown -R wanderindev:wanderindev /home/wanderindev/.ssh
    chmod 700 /home/wanderindev/.ssh
    chmod 600 /home/wanderindev/.ssh/authorized_keys
fi

echo "=== directories ==="
install -d -o wanderindev -g wanderindev /srv/repos /srv/worktrees /srv/orchestrator

echo
echo "=== versions ==="
docker --version
docker compose version
node --version
npm --version
python3 --version
git --version
claude --version

node_major="$(node --version | sed 's/^v\([0-9]*\).*/\1/')"
if [ "$node_major" -lt "$NODE_MAJOR" ]; then
    echo "FAIL: node ${node_major} is below the required ${NODE_MAJOR}" >&2
    exit 1
fi
echo
echo "OK. Remaining manual step: run 'claude' once as wanderindev to complete"
echo "the interactive OAuth login. It cannot be automated."
