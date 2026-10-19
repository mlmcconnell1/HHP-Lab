#!/usr/bin/env bash
# Send keystrokes to one launched worker.
# Usage: send.sh [--scratch DIR] NAME [--enter|--esc|TEXT...]
set -euo pipefail

usage() { echo "usage: $0 [--scratch DIR] NAME [--enter|--esc|TEXT...]" >&2; }
scratch=${PTY_AGENTS_DIR:-${TMPDIR:-/tmp}/pty-subagents}
while (($#)); do
  case $1 in
    --scratch) (($# >= 2)) || { usage; exit 2; }; scratch=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    --) shift; break ;;
    *) break ;;
  esac
done
(( $# >= 1 )) || { usage; exit 2; }
name=$1; shift
[[ -n $name && $name != . && $name != .. && $name != */* ]] || { echo "send.sh: invalid NAME" >&2; exit 2; }
input=$scratch/$name/in.stream
[[ -f $input && -w $input ]] || { echo "send.sh: input stream is not writable: $input" >&2; exit 1; }
(( $# >= 1 )) || { usage; exit 2; }

case $1 in
  --enter)
    (($# == 1)) || { usage; exit 2; }
    printf '\r' >> "$input"
    ;;
  --esc)
    (($# == 1)) || { usage; exit 2; }
    printf '\033' >> "$input"
    ;;
  *)
    text=$1; shift
    while (($#)); do text+=" $1"; shift; done
    printf '%s' "$text" >> "$input"
    sleep "${SEND_DELAY:-0.5}"
    printf '\r' >> "$input"
    ;;
esac
