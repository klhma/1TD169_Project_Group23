#!/usr/bin/env bash
# deploy.sh
# Helper script to copy project code to the Spark cluster master node.
# Edit the variables below to match your cluster configuration.

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration — edit these variables before running
# ---------------------------------------------------------------------------
MASTER_HOST="spark-master"          # Hostname or IP of the Spark master node
REMOTE_USER="ubuntu"                # SSH user on the master node
REMOTE_DIR="/home/${REMOTE_USER}/project"  # Target directory on the master
SSH_KEY="~/.ssh/my-keypair.pem"     # Path to your SSH private key

# ---------------------------------------------------------------------------
# Source files/directories to copy
# ---------------------------------------------------------------------------
SOURCE_DIRS=("src" "scripts" "data")

# ---------------------------------------------------------------------------
# Deploy
# ---------------------------------------------------------------------------
echo "Deploying code to ${REMOTE_USER}@${MASTER_HOST}:${REMOTE_DIR} ..."

# Ensure the remote directory exists
ssh -i "${SSH_KEY}" "${REMOTE_USER}@${MASTER_HOST}" "mkdir -p ${REMOTE_DIR}"

for DIR in "${SOURCE_DIRS[@]}"; do
    echo "  Copying ${DIR}/ ..."
    rsync -avz --exclude '__pycache__' --exclude '*.pyc' \
        -e "ssh -i ${SSH_KEY}" \
        "${DIR}/" "${REMOTE_USER}@${MASTER_HOST}:${REMOTE_DIR}/${DIR}/"
done

echo "Deploy complete."
echo "You can now SSH into the master and run:"
echo "  spark-submit ${REMOTE_DIR}/src/etl_job.py"
echo "  spark-submit ${REMOTE_DIR}/src/analysis_job.py"
