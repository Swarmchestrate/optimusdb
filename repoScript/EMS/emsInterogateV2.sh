#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------
# EMS <-> OptimusDB ad-hoc query helper
# - Auto-detects optimusdb-0 Pod IP (namespace default)
# - Or use --ip <POD_IP> or --lb <HOST:PORT> (e.g., 172.31.26.15:18001)
# - Endpoints used:
#   * /swarmkb/ems/sql     (POST {"sql":"..."})
#   * /swarmkb/ems/events  (GET ?limit=&since_min=)
#   * /swarmkb/ems/logs    (GET ?limit=&since_min=&level=)
# ------------------------------------------------------------

#
# sudo ./emsInterogateV2.sh --sql "SELECT id,received_at,topic,substr(raw_json,1,200) AS raw FROM ems_events ORDER BY id;"  >> EMSTopics.txt
#


NS="default"
POD="optimusdb-0"
POD_IP=""
LB_HOSTPORT=""
CONTEXT="swarmkb"
PORT="8089"

# default actions
DO_TABLES=0
DO_COUNT=0
DO_EVENTS=0;  EVENTS_LIMIT=50; EVENTS_SINCE_MIN=0
DO_LOGS=0;    LOGS_LIMIT=50;  LOGS_SINCE_MIN=0; LOGS_LEVEL=""
DO_SQL=0;     SQL_QUERY=""

usage() {
  cat <<EOF
Usage: $0 [options] [actions]

Target selection (one of these; default is auto-resolve Pod IP):
  --ns <namespace>         (default: ${NS})
  --pod <name>             (default: ${POD})
  --ip  <pod_ip>           Use a specific Pod IP (skips kubectl)
  --lb  <host:port>        Use the LoadBalancer (e.g., 172.31.26.15:18001)

Actions:
  --tables                 List tables in logger DB
  --count                  Count rows in ems_events
  --events [N]             Show last N events (default 50)
  --since <minutes>        With --events/--logs, filter by last <minutes>
  --logs [N]               Show last N EMS log lines (default 50)
  --level <LEVEL>          With --logs, filter by INFO|WARN|ERROR|DEBUG
  --sql "<QUERY>"          Run ad-hoc SQL against logger DB

Examples:
  $0 --tables
  $0 --events 100
  $0 --logs 100 --since 60 --level ERROR
  $0 --sql "SELECT id,received_at,topic,substr(raw_json,1,200) FROM ems_events ORDER BY id DESC LIMIT 20;"
  $0 --lb 172.31.26.15:18001 --events 50
EOF
}

need_bin() { command -v "$1" >/dev/null 2>&1 || { echo "Missing: $1" >&2; exit 1; }; }

# ---------- parse args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --ns)    NS="$2"; shift 2 ;;
    --pod)   POD="$2"; shift 2 ;;
    --ip)    POD_IP="$2"; shift 2 ;;
    --lb)    LB_HOSTPORT="$2"; shift 2 ;;
    --tables) DO_TABLES=1; shift ;;
    --count)  DO_COUNT=1;  shift ;;
    --events) DO_EVENTS=1; EVENTS_LIMIT="${2:-50}"; [[ "$2" =~ ^[0-9]+$ ]] && shift 2 || shift 1 ;;
    --since)  EVENTS_SINCE_MIN="$2"; LOGS_SINCE_MIN="$2"; shift 2 ;;
    --logs)   DO_LOGS=1; LOGS_LIMIT="${2:-50}"; [[ "$2" =~ ^[0-9]+$ ]] && shift 2 || shift 1 ;;
    --level)  LOGS_LEVEL="$2"; shift 2 ;;
    --sql)    DO_SQL=1; SQL_QUERY="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

need_bin curl
need_bin jq

# resolve BASE URL
BASE=""
if [[ -n "$LB_HOSTPORT" ]]; then
  BASE="http://${LB_HOSTPORT}/${CONTEXT}"
elif [[ -n "$POD_IP" ]]; then
  BASE="http://${POD_IP}:${PORT}/${CONTEXT}"
else
  need_bin kubectl
  POD_IP="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.podIP}')"
  if [[ -z "$POD_IP" ]]; then
    echo "Could not resolve Pod IP for ${POD} in ns=${NS}" >&2
    exit 1
  fi
  BASE="http://${POD_IP}:${PORT}/${CONTEXT}"
fi

echo ">>> Using BASE: $BASE"

# ---------- helpers ----------
post_sql() {
  local sql="$1"
  # Build JSON body safely with jq
  local body; body="$(jq -n --arg sql "$sql" '{sql:$sql}')"
  curl -s -X POST -H "Content-Type: application/json" \
    --data "$body" \
    "$BASE/ems/sql" | jq .
}

get_events() {
  local url="$BASE/ems/events?limit=${EVENTS_LIMIT}"
  if [[ "$EVENTS_SINCE_MIN" -gt 0 ]]; then
    url="${url}&since_min=${EVENTS_SINCE_MIN}"
  fi
  curl -s "$url" | jq .
}

get_logs() {
  local url="$BASE/ems/logs?limit=${LOGS_LIMIT}"
  if [[ "$LOGS_SINCE_MIN" -gt 0 ]]; then
    url="${url}&since_min=${LOGS_SINCE_MIN}"
  fi
  if [[ -n "$LOGS_LEVEL" ]]; then
    url="${url}&level=${LOGS_LEVEL}"
  fi
  curl -s "$url" | jq .
}

# ---------- run actions ----------
RAN=0
if [[ "$DO_TABLES" -eq 1 ]]; then
  echo "== Tables in logger DB =="
  post_sql "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
  RAN=1
fi

if [[ "$DO_COUNT" -eq 1 ]]; then
  echo "== ems_events count =="
  post_sql "SELECT COUNT(*) AS n FROM ems_events;"
  RAN=1
fi

if [[ "$DO_EVENTS" -eq 1 ]]; then
  echo "== Recent EMS events =="
  get_events
  RAN=1
fi

if [[ "$DO_LOGS" -eq 1 ]]; then
  echo "== EMS log lines =="
  get_logs
  RAN=1
fi

if [[ "$DO_SQL" -eq 1 ]]; then
  echo "== Ad-hoc SQL =="
  post_sql "$SQL_QUERY"
  RAN=1
fi

# default behavior if no action specified
if [[ "$RAN" -eq 0 ]]; then
  echo "No action given; running a quick status:"
  curl -s "$BASE/ems" | jq .
  echo
  echo "-- tables --"
  post_sql "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
  echo
  echo "-- last 10 events --"
  EVENTS_LIMIT=10 get_events
  echo
  echo "-- last 10 logs --"
  LOGS_LIMIT=10 get_logs
fi
