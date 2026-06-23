#!/usr/bin/env bash
set -uo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./run_stbg_2025_pages.sh [--background] [page ...]

Examples:
  ./run_stbg_2025_pages.sh
  ./run_stbg_2025_pages.sh --background
  ./run_stbg_2025_pages.sh --background 6 5 4 3 2 1

Runs stbg_2025.py once per page. If no pages are supplied, it runs pages:
6 5 4 3 2 1

Environment overrides:
  PYTHON_BIN              Python interpreter to use.
  STBG_WRITE_UPDATE_LOG   Defaults to auto.
  STBG_BROWSER_WARMUP     Defaults to 0.
  STBG_LOG_DIR            Defaults to ./logs.
EOF
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
background=0
pages=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -b|--background)
      background=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      pages+=("$@")
      break
      ;;
    *)
      pages+=("$1")
      shift
      ;;
  esac
done

if [[ ${#pages[@]} -eq 0 ]]; then
  pages=(6 5 4 3 2 1)
fi

for page in "${pages[@]}"; do
  if [[ ! "$page" =~ ^[0-9]+$ ]]; then
    echo "Invalid page number: $page" >&2
    exit 2
  fi
done

log_dir="${STBG_LOG_DIR:-$script_dir/logs}"
mkdir -p "$log_dir"

pages_label="$(printf '%s_' "${pages[@]}")"
pages_label="${pages_label%_}"
run_ts="${STBG_RUN_TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
run_name="stbg_2025_pages_${pages_label}_${run_ts}"
log_file="$log_dir/${run_name}.log"
status_file="$log_dir/${run_name}.status"
pid_file="$log_dir/${run_name}.pid"
lock_file="$log_dir/stbg_2025_pages.lock"

if [[ "$background" -eq 1 ]]; then
  export STBG_RUN_TS="$run_ts"
  nohup "$0" "${pages[@]}" >/dev/null 2>&1 &
  launcher_pid=$!
  echo "Started stbg_2025 page runner."
  echo "PID=$launcher_pid"
  echo "LOG=$log_file"
  echo "STATUS=$status_file"
  echo "PID_FILE=$pid_file"
  exit 0
fi

exec 9>"$lock_file"
if ! flock -n 9; then
  echo "Another stbg_2025 page runner is already active. Lock: $lock_file" >&2
  exit 75
fi

echo "$$" > "$pid_file"

finalize() {
  local rc=$?
  echo "FINAL_EXIT=$rc"
  echo "$rc" > "$status_file"
  rm -f "$pid_file"
}
trap finalize EXIT

exec > >(tee -a "$log_file") 2>&1

cd "$script_dir" || exit 1

if [[ -n "${PYTHON_BIN:-}" ]]; then
  python_bin="$PYTHON_BIN"
elif [[ -x "$script_dir/../venv/bin/python" ]]; then
  python_bin="$script_dir/../venv/bin/python"
elif [[ -x "/root/deal_viewer/ABSDaily/ABS/venv/bin/python" ]]; then
  python_bin="/root/deal_viewer/ABSDaily/ABS/venv/bin/python"
else
  python_bin="python"
fi

echo "Started at $(date '+%F %T %z')"
echo "Working directory: $script_dir"
echo "Python: $python_bin"
echo "Pages: ${pages[*]}"
echo "Log: $log_file"
echo "Status: $status_file"
echo "PID file: $pid_file"

active_stbg="$(pgrep -af 'stbg_2025.py' || true)"
if [[ -n "$active_stbg" ]]; then
  echo "Another stbg_2025.py process is already active:"
  echo "$active_stbg"
  exit 75
fi

rc=0
for page in "${pages[@]}"; do
  echo "===== $(date '+%F %T %z') running pageNum=$page ====="
  STBG_PAGE_NUM="$page" \
    STBG_WRITE_UPDATE_LOG="${STBG_WRITE_UPDATE_LOG:-auto}" \
    STBG_BROWSER_WARMUP="${STBG_BROWSER_WARMUP:-0}" \
    PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}" \
    "$python_bin" -u stbg_2025.py
  rc=$?
  echo "===== $(date '+%F %T %z') pageNum=$page exit=$rc ====="
  if [[ "$rc" -ne 0 ]]; then
    break
  fi
done

echo "Completed at $(date '+%F %T %z')"
exit "$rc"
