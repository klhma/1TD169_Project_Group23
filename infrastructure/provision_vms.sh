#!/usr/bin/env bash
# provision_vms.sh
# Script to provision VMs for the Spark cluster.
# Adjust the variables below to match your cloud provider / environment.

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration — edit these variables before running
# ---------------------------------------------------------------------------
NUM_WORKERS=3
MASTER_NAME="spark-master"
WORKER_PREFIX="spark-worker"
INSTANCE_TYPE="t3.medium"   # AWS example; change for GCP/Azure/OpenStack
IMAGE_ID="ami-0abcdef1234567890"  # Update with your AMI / image ID
KEY_NAME="my-keypair"
SECURITY_GROUP="spark-sg"

# ---------------------------------------------------------------------------
# Provision master node
# ---------------------------------------------------------------------------
echo "Provisioning master node: ${MASTER_NAME} ..."
# aws ec2 run-instances \
#   --image-id "${IMAGE_ID}" \
#   --instance-type "${INSTANCE_TYPE}" \
#   --key-name "${KEY_NAME}" \
#   --security-groups "${SECURITY_GROUP}" \
#   --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${MASTER_NAME}}]"
echo "  [TODO] Uncomment and configure the cloud CLI command above."

# ---------------------------------------------------------------------------
# Provision worker nodes
# ---------------------------------------------------------------------------
for i in $(seq 1 "${NUM_WORKERS}"); do
    WORKER_NAME="${WORKER_PREFIX}-${i}"
    echo "Provisioning worker node: ${WORKER_NAME} ..."
    # aws ec2 run-instances \
    #   --image-id "${IMAGE_ID}" \
    #   --instance-type "${INSTANCE_TYPE}" \
    #   --key-name "${KEY_NAME}" \
    #   --security-groups "${SECURITY_GROUP}" \
    #   --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${WORKER_NAME}}]"
    echo "  [TODO] Uncomment and configure the cloud CLI command above."
done

echo "Done. Remember to note the IP addresses and update src/config.py."
