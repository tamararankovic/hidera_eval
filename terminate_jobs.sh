#!/usr/bin/env bash

REMOTE_HOST="nova_cluster"

# --- Local cleanup section ---
echo
echo "Cleaning up local metadata and latency files..."

# Remove latency files
if [[ -d "latency" ]]; then
  echo "Removing latency/*.txt files..."
  rm -f latency/*.txt 2>/dev/null
fi

# Remove job metadata logs and backups
echo "Removing job metadata files..."
rm -f jobs_created*.json 2>/dev/null
rm -f jobs_created*.bak 2>/dev/null
rm -f jobs_created_*_*.bak 2>/dev/null

echo "Fetching active OAR jobs from $REMOTE_HOST..."

# Step 1: Get all job IDs from nova_cluster (but do NOT run anything there yet)
JOBS=$(ssh "$REMOTE_HOST" "oarstat -u \$USER 2>/dev/null | awk 'NR>1 && \$1 ~ /^[0-9]+\$/ {print \$1}'")

if [[ -z "$JOBS" ]]; then
  echo "No active OAR jobs found."
  exit 0
fi

echo "The following OAR jobs will be terminated:"
echo "$JOBS"
echo

# Step 2: For each job, bring down the network locally and then delete it remotely
COUNT=0
export FRONTEND_HOSTNAME=nova_cluster
export HOSTNAME=tamara
for job in $JOBS; do
  echo "----------------------------------------"
  echo "Processing job $job..."

  export OAR_JOB_ID="$job"

  echo "Bringing down local P2P network for job $job..."
  oar-p2p net down

  echo "Deleting OAR job $job on $REMOTE_HOST..."
  ssh "$REMOTE_HOST" "oardel '$job'" 2>/dev/null || echo "Failed to delete job $job remotely"

  ((COUNT++))
done

echo "----------------------------------------"
echo "$COUNT OAR job(s) processed."

echo "Done."
