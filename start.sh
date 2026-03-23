#!/bin/bash
# start.sh — RunPod container startup script
# 1. Injects SSH public key from RunPod env var
# 2. Starts sshd
# 3. Launches 3 independent pod_worker processes (each with own GPU context)

set -e

# --- SSH setup ---
mkdir -p /root/.ssh
chmod 700 /root/.ssh

if [[ -n "$PUBLIC_KEY" ]]; then
    echo "$PUBLIC_KEY" >> /root/.ssh/authorized_keys
    chmod 600 /root/.ssh/authorized_keys
    echo "[start.sh] SSH public key injected."
else
    echo "[start.sh] WARNING: PUBLIC_KEY not set — SSH key auth disabled."
fi

if [[ ! -f /etc/ssh/ssh_host_rsa_key ]]; then
    ssh-keygen -A
fi

service ssh start || /usr/sbin/sshd
echo "[start.sh] sshd started."

# --- Launch 3 independent workers (nohup — survive SSH disconnect) ---
echo "[start.sh] Starting 3 independent worker processes..."

NUM_WORKERS=1 WORKER_ID=${WORKER_ID:-pod}-a \
    nohup python3 /workspace/pod_worker.py > /tmp/worker-a.log 2>&1 &

NUM_WORKERS=1 WORKER_ID=${WORKER_ID:-pod}-b \
    nohup python3 /workspace/pod_worker.py > /tmp/worker-b.log 2>&1 &

NUM_WORKERS=1 WORKER_ID=${WORKER_ID:-pod}-c \
    nohup python3 /workspace/pod_worker.py > /tmp/worker-c.log 2>&1 &

echo "[start.sh] All 3 workers launched. Waiting..."
wait
