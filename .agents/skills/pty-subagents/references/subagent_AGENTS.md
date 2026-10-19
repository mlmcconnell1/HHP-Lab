# Worker agent ground rules

You are one of several worker agents implementing a repository. An orchestrator
coordinates the work and performs the final integration; follow these rules.

## Ground rules

1. Work in the shared repository directory on its `main` branch by default. The
   orchestrator assigns exclusive ownership of files or packages before you
   edit them. Never create a branch or worktree automatically.
2. In the shared main tree, do not stage, commit, push, pull, fetch, rebase,
   merge, or update shared issue trackers. A separate worktree may permit
   worker staging/commits only after explicit discussion and agreement between
   the human user and orchestrator(s) on those permissions; never
   push/pull/rebase or update shared trackers. Report stable edits and
   verification results to the
   orchestrator, which owns integration and shared tracker mutations.
3. Coordinate before changing another worker's files or a shared interface.
   Pause edits when the orchestrator announces the final combined verification
   window.
4. A separate worktree is allowed only after explicit discussion and agreement
   between the human user and orchestrator(s) covers its scope, worker
   permissions (including any staging/commit exception), and integration
   responsibility. Repository deletion restrictions still apply to all cleanup.
