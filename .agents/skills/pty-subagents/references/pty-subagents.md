# Driving interactive CLI agents as sub-agents

A from-scratch guide for an AI agent (Claude Code or similar) that needs to launch,
supervise, and integrate the work of other agent CLIs (codex, gemini, etc.) by
driving their own interfaces — appearing to them as a human operator at a terminal.
Everything here was developed and battle-tested driving OpenAI codex sessions
against a real Rust repository.

## 0. First: check whether you need any of this

Before building terminal puppetry, check the target CLI for a headless mode —
nearly every agent CLI has one, and it is always cheaper and more reliable:

- `claude -p "prompt"` (+ `--resume <id>`, `--output-format json`)
- `codex exec "prompt"` (+ `codex exec resume --last`, `--json`)
- `gemini -p "prompt"` (+ `--approval-mode`)

One process per turn, prompt via argv/stdin, result on stdout, resume flag for
multi-turn. If that fits your task, use it and skip this document.

Use the PTY technique below when you need the *interactive* agent: its TUI-only
features, its native approval prompts (so you can gatekeep each action the way a
human would), or a CLI with no headless mode.

## 1. Architecture

Per sub-agent, one background pipeline:

```
append-only file (in.stream)          <- you "type" by appending bytes
  | tail -f -c +0
  | script -qfc 'stty cols 200 rows 50; <agent-cmd>' /dev/null   <- the PTY
  | tee -a raw.log                    <- ground truth byte stream
  | ptycrunch >> clean.log            <- stripped/deduped digest
```

Plus, on demand (not streaming):

- `render.py raw.log` — replays the raw bytes through a `pyte` virtual terminal
  and prints the final rendered screen. This is the *only* reliable way to read
  a full-screen TUI's state; it is `tmux capture-pane` rebuilt without tmux.
- a **quiescence watcher** — a shell loop that exits when raw.log stops growing
  for N seconds. Run it as a harness background task so its exit *wakes you*.

The division of labor is the core idea:

- **Mechanical layer** (free): detects *change* — output flowing or quiet.
  It is allowed to be dumb and false-positive.
- **You** (expensive): interpret *meaning* only when woken — done vs. still
  working vs. asking a question vs. wedged — by rendering the screen once.

Token economics: the PTY is cheap external storage, your context is the scarce
resource, and the shell between them is a free filter. Read little, read late,
read rendered screens rather than streams. Never poll on a timer.

### Harness-provided PTYs do not replace the screen reader

Some agent harnesses expose a persistent PTY directly, with a session ID plus
operations for writing stdin and reading new output. This is useful for process
lifecycle and input control, but it does **not** necessarily render the terminal
screen. In particular, Codex harness PTY reads may return incremental raw terminal
bytes containing ANSI cursor movement, spinner updates, status-line repainting,
and repeated fragments. `--no-alt-screen` preserves scrollback but does not stop
those rewrites. The repaint traffic does not prevent reliable input, but reading
it frequently is noisy, can be heavily truncated, and wastes orchestrator context.

Treat a direct PTY as transport unless its API explicitly promises a composed
screen snapshot. For sustained supervision, still route the byte stream through
an append-only raw log and `ptycrunch`, use `render.py`/`pyte` to interpret the
current screen, and use raw-log growth for quiescence detection. If the harness
does not let you attach those components to an existing PTY, launch (or resume)
the agent through the pipeline described below rather than polling raw PTY output.

## 2. The components

The packaged helpers in `../scripts/` are the source of truth. Install `pyte`
in the Python environment used by `render.py` (`python3 -m venv venv &&
venv/bin/pip install pyte`). The shell helpers require Bash, Perl, util-linux
`script`, `tail`, `tee`, `mkfifo`, `stty`, `find`, `sleep`, `date`, and `wc`;
`codex` (or an explicitly
configured agent command) is required only by `launch-agent.sh`. Missing
dependencies produce an actionable error naming the command or Python package.

Set `PTY_AGENTS_DIR` or pass `--scratch DIR` to choose any scratch directory;
each worker gets `DIR/<name>/in.stream`, `raw.log`, and `clean.log` (plus an
internal input FIFO). Paths may
contain spaces or apostrophes. The helpers refuse to reuse an existing worker
directory, so logs are never silently truncated.

The reusable interfaces are:

- `scripts/ptycrunch`: `pty-source | scripts/ptycrunch >> clean.log`.
  It consumes 4 KiB chunks, retains at most `PTYCRUNCH_MAX_PENDING` visible
  bytes (default 65536), and carries ANSI state across chunks. A newline-free
  stream is emitted as bounded records; this is the defined behavior for long
  output, not an unbounded line wait. `PTYCRUNCH_WINDOW` controls deduplication.
- `scripts/render.py RAW_LOG [COLS] [ROWS]`: replay the raw bytes through
  `pyte` and print the final screen. This is the reliable full-screen TUI
  reader.
- `scripts/launch-agent.sh [--scratch DIR] [--agent BIN] NAME WORKDIR
  PROMPTFILE|resume:SESSION_ID`: launch one agent and create fresh per-worker
  files. Prompt-file content is passed as the initial agent argument; a
  `resume:` request runs the agent's resume command. The helper sets a 200x50
  PTY, uses shell-quoted fixed arguments, explicitly stops its input feeder,
  and returns the agent status.
- `scripts/send.sh [--scratch DIR] NAME [--enter|--esc|TEXT...]`: append
  keystrokes to a worker's input stream. Text receives a short configurable
  `SEND_DELAY` before Enter.
- `scripts/watch-any.sh [--scratch DIR] [NAME...]`: wake when a selected raw
  log (or any worker when no names are supplied) has been unchanged for
  `QUIET` seconds, or when `MAXWAIT` is reached. `INTERVAL` controls polling.

Run launch and watcher as harness background tasks (Claude Code:
`run_in_background`), one per agent/watcher. When the pipeline exits (agent
quit or crashed), the harness wakes you.

The `stty cols 200 rows 50` is **mandatory**: with no real terminal behind
`script`, the PTY comes up with a degenerate size (observed: one column wide,
every glyph wrapping to its own row — unreadable even through a renderer).

Enter is `\r` (carriage return), not `\n`. Sending text and Enter with a short
gap between them is deliberate — some TUIs debounce paste-like input. Watch
**raw.log, not clean.log**:
a working TUI repaints a timer every second (raw bytes flow) but may produce
nothing *new* after dedup, so clean.log quiescence fires while the agent is
mid-thought. Watch only agents you expect activity from — an idle agent at its
prompt is legitimately quiet and will false-fire the watcher instantly.

### Selecting a model and reasoning effort

Codex accepts `--model MODEL -c 'model_reasoning_effort="high"'`. To use these
settings with `launch-agent.sh`, put them in an executable wrapper: `--agent`
accepts an executable path (or a command name on `PATH`), **not a command string
with arguments**. Extra Codex flags are not launcher arguments.

Create `/path/to/codex-high` with the following content, replacing `MODEL` with
the desired model ID and `high` with a reasoning effort supported by that model:

```sh
#!/bin/sh
exec codex --model MODEL -c 'model_reasoning_effort="high"' "$@"
```

Make it executable, then launch it using the helper path resolved from the skill
directory:

```sh
chmod +x /path/to/codex-high
scripts/launch-agent.sh --agent /path/to/codex-high \
  worker-high /path/to/repo /path/to/prompt.txt
```

The quoted `"$@"` forwards the launcher's prompt or `resume SESSION_ID` arguments
unchanged. Use the same wrapper when resuming to apply the chosen settings.

## 3. Operating procedure

**Launch.** Start the pipeline task; after ~10s render the screen. Expect and
handle startup dialogs by judgment (update prompts, folder-trust prompts —
answer them via send.sh like a human would). Then arm the watcher and *stop* —
do nothing until woken. Every wake: render the screen once, classify, act:

- **Approval prompt** → see gatekeeping below.
- **Still working** (false wake) → re-arm the watcher, done.
- **Asking a question / wedged / erroring** → answer or redirect via send.sh.
- **Finished** → verify its claims independently (see integration).

**Gatekeeping approvals.** The sub-agent's permission prompts are your control
point — the reason to drive the TUI at all. In the shared-main workflow,
workers must not stage or commit: hold Git mutation requests and report them to
the orchestrator. A separate worktree may allow worker staging/commits only
after explicit discussion and agreement between the human user and
orchestrator(s) on that permission and its scope; inspect the footprint
(`git status --short`) and keep pushes/publishing with the orchestrator.

**Crash recovery.** Agent CLIs persist session state continuously (codex:
`~/.codex/sessions/<date>/rollout-*.jsonl`). If a session dies, resume it:
`codex resume <session-id>` in the same workdir restores full context; then
send "please continue — the interruption was accidental" and tell it to re-run
any verification whose background processes died with the crash. Map session
IDs to workdirs by grepping the rollout files for the workdir path — with
multiple concurrent sessions, `resume --last` is ambiguous; always use IDs.

**Parallelism.** Fan out only what the dependency graph actually allows. The
default is a shared working tree on `main`: the orchestrator assigns exclusive
file/package ownership, coordinates cross-owner edits and interface changes,
and keeps workers from touching shared trackers. Workers make edits, run
focused checks, and hand off without staging or committing. Pause concurrent
writes for a stable final combined-verification window before the orchestrator
reviews and commits.

Separate worktrees are an exception, never an automatic parallelism mechanism.
Use one only after explicit discussion and agreement between the human user and
orchestrator(s) on its scope, worker permissions, and integration
responsibility. If agreed, the
orchestrator owns creating/integrating it and the worker follows the agreed
cleanup and deletion restrictions. With several orchestrators, designate one
owner of shared Git operations.

**Integration (the orchestrator's job, never delegated).**
1. Review each worker's handoff and inspect the shared-tree diff, checking that
   ownership boundaries and requested scope were respected.
2. Resolve overlapping edits semantically with the workers before the final
   verification window; preserve compatible changes and make interfaces
   coherent.
3. Pause worker writes and run the full verification suite on the combined
   result. Worker self-reports are useful evidence but do not replace this
   check.
4. Treat any final resolution as code: conformance tests may assert exact
   phrases or interfaces. If integration tests fail, inspect the combined
   edits and interface assumptions before attributing the failure elsewhere.
5. Send a precise failure back to the owning worker when its session is alive,
   then repeat the stable verification window after the fix.
6. The orchestrator owns shared-main staging, commits, ticket updates, and
   pushes. A worker may stage/commit in a separate worktree only under the
   explicit discussion-and-agreement exception above; the orchestrator still
   owns integration, shared tracker updates, pushes, and cleanup under the
   agreed deletion restrictions.

## 4. Pitfalls (each of these cost us a debugging cycle)

- **Degenerate PTY size.** Without `stty cols/rows` before the agent starts,
  the TUI renders into a 1-column terminal. Unreadable; set it always.
- **Spinner-only output.** "Thinking" agents may emit output only in the OSC
  window title (a spinner). Stripped streams go quiet; raw bytes keep flowing.
  Hence: watch raw.log.
- **Frozen pipeline ≠ idle agent.** If raw.log stops at a suspiciously round
  size (pipe-buffer boundary), your tee/filter died (SIGPIPE cascade) while
  the agent runs on, blind. Check the pipeline processes (`ps -o stat,wchan`),
  then recycle the agent via session-resume with a fresh pipeline. ptycrunch's
  incremental bounded records prevent a newline-free run from accumulating
  indefinitely in the filter.
- **pkill self-match.** `pkill -f 'some pattern'` kills your *own* shell if
  the pattern appears in your own command line (it does — you just typed it).
  Use a character class to break self-match: `pkill -f 'scrip[t] -qfc'`.
- **Shell environment landmines.** Your background shells may run the user's
  shell init. We hit: an aliased `tmux` that didn't exist non-interactively,
  and an ssh-agent block that spawned agents and yanked sockets from every
  parallel launcher. Prefer absolute binary paths; treat login-shell side
  effects as hostile.
- **External kill sweeps.** The harness can kill all your background tasks at
  once for reasons outside your control (we traced one to an expired ssh key
  cascading into a connection drop; another to an exhausted spend limit).
  Design for it: pipelines are disposable, session state is durable, and
  recovery is always resume + "please continue" + re-verify.
- **Watcher lifecycle.** Arm watchers via the harness's background-task
  mechanism so their exit wakes you — a plain shell `&` completes silently
  and you sleep forever. Re-arm after every wake you don't act on; kill and
  re-scope watchers when the fleet changes (idle agents false-fire).
- **Sandbox quirks.** Snap/containerized CLIs may silently drop output
  (`--version` printing nothing) or be unable to write where you expect;
  errors may only appear in their own log files, not stderr.

## 5. What this technique is and isn't

Through the PTY the sub-agent genuinely cannot distinguish you from a human
operator: same keystrokes, same approval dialogs, same screen. What you gain
over headless modes is judgment-gated approvals, mid-flight redirection, and
compatibility with anything that runs in a terminal. What you pay is the
machinery above and the obligation to actually supervise: render screens at
wakes, gate approvals on evidence, and verify everything independently at
integration. The sub-agents' claims are inputs to your verification, never
substitutes for it.
