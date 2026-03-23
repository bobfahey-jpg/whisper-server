#!/bin/bash
# start.sh — RunPod container startup script
# 1. Injects SSH public key from RunPod env var (enables SSH access)
# 2. Starts sshd
# 3. Launches pod_worker.py (multi-threaded)

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

# Generate host keys if missing (first boot)
if [[ ! -f /etc/ssh/ssh_host_rsa_key ]]; then
    ssh-keygen -A
    echo "[start.sh] SSH host keys generated."
fi

# Start sshd in background
service ssh start || /usr/sbin/sshd
echo "[start.sh] sshd started."

# --- Worker ---
echo "[start.sh] Starting pod_worker.py with NUM_WORKERS=${NUM_WORKERS:-3}"
exec python3 /workspace/pod_worker.py
