#!/bin/bash
# start.sh — RunPod container startup script
# 1. Injects SSH public key from RunPod env var
# 2. Starts sshd
# 3. Launches NUM_WORKERS independent pod_worker processes (each with own GPU context)
#    NUM_WORKERS comes from RunPod template env var (default: 3)
#    Each process loads its own WhisperModel — avoids CUDA shared-memory issues.

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

# --- Launch independent workers ---
N=${NUM_WORKERS:-3}
echo "[start.sh] Starting ${N} independent worker processes (NUM_WORKERS=${N})..."

for i in $(seq 1 $N); do
    LABEL=$(printf "%02d" $i)
    NUM_WORKERS=1 WORKER_ID=${WORKER_ID:-pod}-w${LABEL} \
        nohup python3 /workspace/pod_worker.py > /tmp/worker-${LABEL}.log 2>&1 &
    echo "[start.sh] Launched worker-${LABEL} (PID $!)"
done

echo "[start.sh] All ${N} workers launched. Waiting..."
wait
