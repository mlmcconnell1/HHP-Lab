---
name: pty-subagents
description: Launch, supervise, recover, and integrate interactive CLI coding agents through PTYs.
---

# Driving pty-subagents

For the complete architecture, operating procedure, approval policy, recovery
guidance, and pitfalls, read
[references/pty-subagents.md](references/pty-subagents.md).

To choose a Codex model and thinking level, see
[Selecting a model and reasoning effort](references/pty-subagents.md#selecting-a-model-and-reasoning-effort).

Reusable helpers are under `scripts/`. Resolve their paths relative to this
skill directory; do not assume the skill was installed at a fixed absolute path.

Before launching the workflow, check its runtime prerequisites. In particular,
the screen renderer requires Python and `pyte`; agentenv copies skill files but
does not install runtime dependencies. The helper scripts report actionable
errors when a prerequisite is missing.
