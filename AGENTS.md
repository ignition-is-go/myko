# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync --flush-only  # Save beads to JSONL (local only)
```

## Beads Workflow Rules

- **No auto-push**: Only sync with remote when the user explicitly asks
- **Local-only by default**: Use `bd sync --flush-only` to save changes without git operations
- **Task-scoped commits**: Only commit files changed for the current task
- **Separate concerns**: Commit code changes separately from beads changes

## Completing Work

When finishing a task:

```bash
git add <only-task-files>           # Stage only files for this task
git commit -m "feat(scope): ..."    # Commit code changes
bd sync --flush-only                # Export beads to JSONL (no commit/push)
```

**Do NOT automatically push to remote.** Only run `bd sync` (full sync) or `git push` when the user explicitly requests it.

## Quality Gates (if code changed)

Run applicable checks before committing:
- `cargo check` - Type checking
- `cargo clippy` - Lints
- `cargo test` - Tests
- `pnpm build` - TypeScript builds

## Issue Management

1. **Create issues for follow-up work** - Use `bd create` for anything needing future attention
2. **Update status** - Close finished work with `bd close <id>`
3. **Flush changes** - Run `bd sync --flush-only` to persist to JSONL
