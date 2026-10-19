#!/usr/bin/env bash
# Exit when any selected worker's raw.log has been quiet.
# Usage: watch-any.sh [--scratch DIR] [NAME...]
set -euo pipefail

usage() { echo "usage: $0 [--scratch DIR] [NAME...]" >&2; }
scratch=${PTY_AGENTS_DIR:-${TMPDIR:-/tmp}/pty-subagents}
while (($#)); do
  case $1 in
    --scratch) (($# >= 2)) || { usage; exit 2; }; scratch=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    --) shift; break ;;
    *) break ;;
  esac
done
quiet=${QUIET:-30}
maxwait=${MAXWAIT:-1800}
interval=${INTERVAL:-5}
[[ $quiet =~ ^[0-9]+$ && $maxwait =~ ^[0-9]+$ && $interval =~ ^[1-9][0-9]*$ ]] || {
  echo "watch-any.sh: QUIET, MAXWAIT, and INTERVAL must be non-negative/positive integers" >&2
  exit 2
}
[[ -d $scratch ]] || { echo "watch-any.sh: scratch directory does not exist: $scratch" >&2; exit 1; }
command -v find >/dev/null 2>&1 || { echo "watch-any.sh: missing required command 'find'" >&2; exit 2; }
command -v sleep >/dev/null 2>&1 || { echo "watch-any.sh: missing required command 'sleep'" >&2; exit 2; }
command -v date >/dev/null 2>&1 || { echo "watch-any.sh: missing required command 'date'" >&2; exit 2; }
command -v wc >/dev/null 2>&1 || { echo "watch-any.sh: missing required command 'wc'" >&2; exit 2; }

declare -a files=()
selected=0
if (($#)); then
  selected=1
  for name; do
    [[ -n $name && $name != . && $name != .. && $name != */* ]] || { echo "watch-any.sh: invalid NAME: $name" >&2; exit 2; }
    files+=("$scratch/$name/raw.log")
  done
fi
declare -A last_size last_change
start=$(date +%s)
while :; do
  now=$(date +%s)
  if ((selected == 0)); then
    files=()
    while IFS= read -r -d '' file; do files+=("$file"); done < <(find "$scratch" -mindepth 2 -maxdepth 2 -type f -name raw.log -print0)
  fi
  for file in "${files[@]}"; do
    [[ -f $file ]] || continue
    key=$file
    size=$(wc -c < "$file")
    if [[ ${last_size[$key]+set} != set || ${last_size[$key]} != "$size" ]]; then
      last_size[$key]=$size
      last_change[$key]=$now
      continue
    fi
    if (( now - ${last_change[$key]:-$now} >= quiet )); then
      worker=${file%/raw.log}; worker=${worker##*/}
      echo "QUIET: $worker after $((now - start))s (raw.log=$size)"
      exit 0
    fi
  done
  if (( now - start >= maxwait )); then
    echo "MAXWAIT reached"
    exit 0
  fi
  sleep "$interval"
done
