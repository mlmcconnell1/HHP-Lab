#!/usr/bin/env bash
# Launch one interactive agent with isolated terminal/log files.
# Usage: launch-agent.sh [--scratch DIR] [--agent BIN] NAME WORKDIR PROMPTFILE|resume:SESSION_ID
set -euo pipefail

usage() {
  echo "usage: $0 [--scratch DIR] [--agent BIN] NAME WORKDIR PROMPTFILE|resume:SESSION_ID" >&2
  echo "       PTY_AGENTS_DIR may provide scratch (default: \${TMPDIR:-/tmp}/pty-subagents)" >&2
}

scratch=${PTY_AGENTS_DIR:-${TMPDIR:-/tmp}/pty-subagents}
agent_bin=${CODEX_BIN:-codex}
while (($#)); do
  case $1 in
    --scratch)
      (($# >= 2)) || { usage; exit 2; }
      scratch=$2; shift 2
      ;;
    --agent)
      (($# >= 2)) || { usage; exit 2; }
      agent_bin=$2; shift 2
      ;;
    --help|-h) usage; exit 0 ;;
    --) shift; break ;;
    *) break ;;
  esac
done
(( $# == 3 )) || { usage; exit 2; }

name=$1
workdir=$2
request=$3
[[ -n $name && $name != . && $name != .. && $name != */* ]] || {
  echo "launch-agent.sh: NAME must be a non-empty directory name without '/'" >&2
  exit 2
}
[[ -d $workdir ]] || { echo "launch-agent.sh: workdir does not exist: $workdir" >&2; exit 2; }
workdir=$(CDPATH= cd -- "$workdir" && pwd)
if [[ $request != resume:* ]]; then
  [[ -f $request && -r $request ]] || {
    echo "launch-agent.sh: prompt file is not readable: $request" >&2
    exit 2
  }
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ptycrunch=$script_dir/ptycrunch
for dependency in script tail tee mkfifo stty perl setsid; do
  command -v "$dependency" >/dev/null 2>&1 || {
    echo "launch-agent.sh: missing required command '$dependency'" >&2
    exit 2
  }
done
[[ -x $ptycrunch ]] || { echo "launch-agent.sh: not executable: $ptycrunch" >&2; exit 2; }
if [[ $agent_bin == */* ]]; then
  [[ -x $agent_bin ]] || { echo "launch-agent.sh: agent is not executable: $agent_bin" >&2; exit 2; }
else
  agent_bin=$(command -v -- "$agent_bin") || {
    echo "launch-agent.sh: agent command not found: $agent_bin" >&2
    exit 2
  }
fi
if [[ $agent_bin == */* ]]; then
  agent_bin=$(CDPATH= cd -- "$(dirname -- "$agent_bin")" && pwd)/$(basename -- "$agent_bin")
fi
scratch=$(mkdir -p -- "$scratch" && CDPATH= cd -- "$scratch" && pwd)

bundle=$scratch/$name
mkdir -- "$bundle" 2>/dev/null || {
  echo "launch-agent.sh: worker directory already exists; refusing to reuse logs: $bundle" >&2
  exit 2
}
input=$bundle/in.stream
raw=$bundle/raw.log
clean=$bundle/clean.log
touch -- "$input" "$raw" "$clean"
fifo=$bundle/agent.stdin
mkfifo -- "$fifo"
output_fifo=$bundle/agent.stdout
mkfifo -- "$output_fifo"

# script(1) accepts a command string. Quote each fixed argument before passing
# it to script so spaces and apostrophes in configured paths remain data, not
# shell syntax. Subsequent input is sent through the PTY input stream.
shell_quote() {
  local value=$1
  value=${value//\'/\'\\\'\'}
  printf "'%s'" "$value"
}

if [[ $request == resume:* ]]; then
  session=${request#resume:}
  [[ -n $session ]] || { echo "launch-agent.sh: empty resume session id" >&2; exit 2; }
  command_string="$(shell_quote stty) cols 200 rows 50; $(shell_quote "$agent_bin") resume $(shell_quote "$session")"
else
  prompt=$(<"$request")
  command_string="$(shell_quote stty) cols 200 rows 50; $(shell_quote "$agent_bin") $(shell_quote "$prompt")"
fi

cd -- "$workdir"
tail_pid=''
filter_pid=''
agent_pid=''
cleanup_children() {
  if [[ -n $tail_pid ]]; then
    kill "$tail_pid" 2>/dev/null || true
    wait "$tail_pid" 2>/dev/null || true
    tail_pid=''
  fi
  if [[ -n $agent_pid ]]; then
    kill -TERM "$agent_pid" 2>/dev/null || true
    kill -TERM -- "-$agent_pid" 2>/dev/null || true
  fi
  if [[ -n $filter_pid ]]; then
    kill "$filter_pid" 2>/dev/null || true
  fi
  if [[ -n $agent_pid ]]; then
    wait "$agent_pid" 2>/dev/null || true
    agent_pid=''
  fi
  if [[ -n $filter_pid ]]; then
    wait "$filter_pid" 2>/dev/null || true
    filter_pid=''
  fi
}
cancel() {
  trap - HUP INT TERM
  cleanup_children
  exit 143
}
trap cancel HUP INT TERM
trap cleanup_children EXIT

# Keep feeder, filter, and agent separate so cancellation can terminate and
# reap each process. FIFOs are intentionally retained with the worker scratch
# bundle; reruns use a fresh worker name rather than reusing logs.
tail -f -c +0 -- "$input" > "$fifo" &
tail_pid=$!
(set -o pipefail; tee -a -- "$raw" < "$output_fifo" | "$ptycrunch" >> "$clean") &
filter_pid=$!
setsid script -qefc "$command_string" /dev/null < "$fifo" > "$output_fifo" &
agent_pid=$!

set +e
wait "$agent_pid"
agent_status=$?
agent_pid=''
set -e
kill "$tail_pid" 2>/dev/null || true
wait "$tail_pid" 2>/dev/null || true
tail_pid=''
set +e
wait "$filter_pid"
filter_status=$?
set -e
filter_pid=''
trap - EXIT HUP INT TERM
if (( agent_status != 0 )); then
  exit "$agent_status"
else
  exit "$filter_status"
fi
