#!/usr/bin/env bash

INPUT_FILE="$1"
VALID_PROTOCOLS=("hi" "fu" "ep" "rr" "dd")
REMOTE_HOST="nova_cluster"
JOBS_LOG="jobs_created.json"
DEFAULT_LOSS="20"

if [[ -z "$INPUT_FILE" || ! -f "$INPUT_FILE" ]]; then
  echo "Usage: $0 <json_file>"
  exit 1
fi

# --- Initialize JSON log file ---
if [[ -f "$JOBS_LOG" ]]; then
  mv "$JOBS_LOG" "${JOBS_LOG%.json}_$(date +%Y%m%d_%H%M%S).bak"
fi
echo "[]" > "$JOBS_LOG"

# --- Helper: validate protocol ---
is_valid_protocol() {
  local proto="$1"
  for p in "${VALID_PROTOCOLS[@]}"; do
    [[ "$p" == "$proto" ]] && return 0
  done
  return 1
}

# --- Helper: submit job remotely ---
submit_job() {
  local session_name="$1"
  local exp_name="$2"
  local protocol="$3"
  local nodes_count="$4"
  local latency="$5"
  local repeat="$6"
  local stabilization_wait="$7"
  local event_wait="$8"
  local event="$9"
  local end_wait="${10}"
  local loss="${11:-$DEFAULT_LOSS}"

  echo "  → Creating job '$session_name' on $REMOTE_HOST..."

  # Run job submission and capture job ID
  local JOB_ID
  JOB_ID=$(ssh "$REMOTE_HOST" "export LC_ALL=C LANG=C;
    oarsub -l \"{cluster='moltres'}/nodes=1,walltime=12:00\" \
           --project ${session_name} 'sleep 43200'" 2>/dev/null | grep -Eo '[0-9]+' | tail -n1)

  if [[ -z "$JOB_ID" ]]; then
    echo " Failed to create job '$session_name' on $REMOTE_HOST."
    ./terminate_jobs.sh
    exit 1
  fi

  echo "   Job '$session_name' created with ID $JOB_ID"

  # Append metadata to JSON log
  jq --arg id "$JOB_ID" \
     --arg exp "$exp_name" \
     --arg proto "$protocol" \
     --argjson nodes "$nodes_count" \
     --argjson lat "$latency" \
     --argjson rep "$repeat" \
     --argjson stab "$stabilization_wait" \
     --argjson evwait "$event_wait" \
     --arg ev "$event" \
     --argjson endw "$end_wait" \
     --argjson loss "$loss" \
     '. += [{"job_id": $id, "exp_name": $exp, "protocol": $proto, "nodes_count": $nodes, "latency": $lat, "repeat": $rep, "stabilization_wait": $stab, "event_wait": $evwait, "event": $ev, "end_wait": $endw, "loss": $loss}]' \
     "$JOBS_LOG" > "${JOBS_LOG}.tmp" && mv "${JOBS_LOG}.tmp" "$JOBS_LOG"

  return 0
}

# --- Read and process JSON input ---
if ! command -v jq >/dev/null 2>&1; then
  echo "Error: jq is required to parse JSON. Please install it (e.g., sudo apt install jq)."
  exit 1
fi

mapfile -t experiments < <(jq -c '.[]' "$INPUT_FILE")

for exp in "${experiments[@]}"; do
  protocol=$(echo "$exp" | jq -r '.protocol')
  exp_name=$(echo "$exp" | jq -r '.exp_name')
  nodes_count=$(echo "$exp" | jq -r '.nodes_count')
  latency=$(echo "$exp" | jq -r '.latency')
  repeat=$(echo "$exp" | jq -r '.repeat')
  stabilization_wait=$(echo "$exp" | jq -r '.stabilization_wait')
  event_wait=$(echo "$exp" | jq -r '.event_wait')
  event=$(echo "$exp" | jq -r '.event')
  end_wait=$(echo "$exp" | jq -r '.end_wait')
  loss=$(echo "$exp" | jq -r '.loss // empty')

  [[ -z "$protocol" || "$protocol" == "null" ]] && continue

  echo "Processing experiment: $exp_name ($protocol)"

  if [[ "$protocol" == "all" ]]; then
    echo "→ 'all' detected — submitting jobs for all protocols..."
    for proto in "${VALID_PROTOCOLS[@]}"; do
      SESSION_NAME="${exp_name}_${proto}"
      submit_job "$SESSION_NAME" "$exp_name" "$proto" "$nodes_count" "$latency" \
            "$repeat" "$stabilization_wait" "$event_wait" "$event" "$end_wait" "$loss" || continue
    done
    echo "------------------------------------"
    continue
  fi

  if ! is_valid_protocol "$protocol"; then
    echo "Invalid protocol '$protocol'. Allowed: ${VALID_PROTOCOLS[*]} or 'all'."
    continue
  fi

  SESSION_NAME="${exp_name}_${protocol}"
  submit_job "$SESSION_NAME" "$exp_name" "$protocol" "$nodes_count" "$latency" \
            "$repeat" "$stabilization_wait" "$event_wait" "$event" "$end_wait" "$loss" || continue
  echo "------------------------------------"

done

echo
echo " All OAR jobs processed for $REMOTE_HOST."
echo "  Job metadata saved to $JOBS_LOG."

# --- Wait for jobs to start ---
echo "Waiting for all OAR jobs on $REMOTE_HOST to start (state = R)..."

while true; do
  JOBS=$(ssh "$REMOTE_HOST" "export LC_ALL=C LANG=C; oarstat -u \$USER 2>/dev/null | awk 'NR>2 {print \$2}'")
  [[ -z "$JOBS" ]] && { echo "No active OAR jobs found."; break; }

  TOTAL=$(echo "$JOBS" | wc -l | tr -d ' ')
  RUNNING=$(echo "$JOBS" | grep -c '^R$' || true)

  echo "  → $RUNNING / $TOTAL jobs are running..."

  [[ "$RUNNING" -eq "$TOTAL" ]] && { echo " All $TOTAL OAR jobs are running."; break; }

  sleep 5
done

# --- Setup P2P networks ---
echo
echo "----------------------------------------"
echo "Setting up P2P network and applying packet loss on each job host..."

export FRONTEND_HOSTNAME="$REMOTE_HOST"
export HOSTNAME="tamara"

COUNT=0
mapfile -t logged_jobs < <(jq -c '.[]' "$JOBS_LOG")

for job_entry in "${logged_jobs[@]}"; do
  job_id=$(echo "$job_entry" | jq -r '.job_id')
  nodes_count=$(echo "$job_entry" | jq -r '.nodes_count')
  latency=$(echo "$job_entry" | jq -r '.latency')
  loss=$(echo "$job_entry" | jq -r '.loss // 0')

  export OAR_JOB_ID="$job_id"

  # --- Resolve the compute host for this job (JSON-safe parsing) ---
  HOST=$(
    ssh "$REMOTE_HOST" "export LC_ALL=C LANG=C;
      oarstat -J -fj ${job_id} 2>/dev/null \
      | grep -A1 '\"assigned_network_address\"' \
      | tail -n1 \
      | grep -oE '\"[^\"]+\"' \
      | tr -d '\" ,'
    " 2>/dev/null
  )

  if [[ -z "$HOST" || "$HOST" == "null" ]]; then
    echo "Could not determine compute host for job $job_id"
    ssh "$REMOTE_HOST" "oarstat -J -fj ${job_id} | jq '.'" || true
    continue
  fi

  echo "Resolved compute host for job $job_id → $HOST"

  echo "=== Bringing up P2P network for job $job_id ==="
  go run latency/main.go "$nodes_count" "$latency" > "latency/latency_${job_id}.txt"
  LAT_FILE="latency/latency_${job_id}.txt"

  if oar-p2p net up --addresses "$nodes_count" --latency-matrix "$LAT_FILE"; then
    echo "   P2P network created for job $job_id."
  else
    echo "   Failed to create P2P network for job $job_id."
    continue
  fi

  # --- Apply packet loss on that host ---
  echo "=== Applying loss (${loss}%) on $HOST for job $job_id ==="
  ssh "$REMOTE_HOST" "ssh -o StrictHostKeyChecking=no $HOST 'bash -s'" <<EOF
docker run --rm --net=host --privileged local/oar-p2p-networking bash -lc '
set -e
for IF in bond0 lo; do
  while read -r line; do
    if [[ "\$line" =~ qdisc[[:space:]]netem[[:space:]]([0-9]+):.*delay[[:space:]]([0-9]+)ms ]]; then
      handle="\${BASH_REMATCH[1]}"; latency_val="\${BASH_REMATCH[2]}"
      classid=\$((handle - 1))
      echo "[\$IF] handle \$handle delay \${latency_val}ms → adding loss ${loss}%"
      tc qdisc change dev "\$IF" parent 1:\$classid handle \$handle: netem delay \${latency_val}ms loss ${loss}%
    fi
  done < <(tc qdisc show dev "\$IF")
done
'
EOF

  ((COUNT++))
done

echo "----------------------------------------"
echo "Configured P2P + loss on $COUNT job host(s)."
