---
description: "Structured code review for the torrent-rs codebase. Use when: reviewing a PR, diff, commit, or current file. Focuses on correctness, safety (unsafe, error handling, edge cases), and architecture (crate boundaries, module design, async/sync layering). Produces prioritized findings with P0/P1/P2 severity."
argument-hint: "[PR number, commit hash, branch, or 'current file']"
---

Review the provided code change. Use the project context below to guide the review.

## Mode

If the user says **"quick"** or the change is small (<50 lines), run **Quick Review**: check only P0 items (correctness, safety, protocol violations). Skip architecture and style unless something is obviously broken. Mark the review as "Quick Review" in the output.

Otherwise, run **Full Review**: all sections below.

## Review Scope

If the input is a **GitHub PR**, use `github-pull-request_currentActivePullRequest` or `github-pull-request_pullRequestInViewport` to get the diff and changed files. If a **commit/branch** or **current file**, read the relevant files directly.

## What to Check

### Correctness & Safety (P0–P1)

- **Error handling**: Are all `Result`/`Option` values handled? No silent `let _ = e` without justification? No bare `unwrap()` or `expect()` without documented invariants?
- **unsafe usage**: Is every `unsafe` block minimal, well-documented with a `// SAFETY:` comment, and behind a safe abstraction?
- **Protocol compliance**: Does the code follow the relevant BEP spec? Check message format, handshake order, timing requirements.
- **Edge cases**: Empty input, N=0, timeout, channel closure, EOF, peer disconnect — are these handled?
- **Resource management**: Are file handles, sockets, and memory properly released? Any unbounded channels or unbounded growth (Vec, HashMap)?
- **Concurrency**: Are `Mutex`/`RwLock` held across `.await`? Are race conditions possible? Is `Send + Sync` correctly derived?

### Architecture & Design (P1–P2)

- **Crate boundary (Hard Rule 2)**: `torrent-core` must NOT depend on tokio. No async I/O types in core.
- **Type placement (Hard Rule 3)**: Pure data/parsing → `torrent-core`. Async I/O → `torrent`.
- **Re-exports (Hard Rule 4)**: Are all public `torrent-core` types needed by `torrent`'s API re-exported?
- **Event-driven patterns**: New tasks via `tokio::spawn`? New channels? New `select!` branches? Follow the event-driven-design conventions.
- **Module cohesion**: Does the change touch >3 modules for a single logical change? If so, is there a coupling issue?
- **Trait design**: Are new traits in `torrent-core`? Are they object-safe if needed?

### Documentation & Style (P2)

- **Doc comments**: Every `pub` item must have `///` documentation.
- **BEP references**: Protocol-related code should reference BEP numbers.
- **Trait derivation**: `Debug`, `Clone`, `PartialEq`, `Eq` where appropriate. No `Clone`/`Eq` on resource-owning types.

## Output Format

Produce a review table ordered by severity:

| #   | Severity | Category    | File:Line   | Issue | Suggestion |
| --- | -------- | ----------- | ----------- | ----- | ---------- |
| 1   | P0       | Correctness | `foo.rs:42` | ...   | ...        |

Where:

- **P0**: Must fix before merge — crash, data loss, protocol violation, safety bug
- **P1**: Should fix — performance issue, missing error handling, architectural violation
- **P2**: Nice to fix — style, documentation, minor refactoring

After the table, add a **Summary** section:

```
## Summary

- P0: N issues (blocking)
- P1: N issues (should fix)
- P2: N issues (nice to fix)

**Verdict**: [Approve | Approve with comments | Request changes]
```

## Integration with Project Skills

For deeper analysis, reference these project skills when relevant:

- `bt-protocol` — Wire format, BEP compliance, message types
- `event-driven-design` — Concurrency patterns, channels, select! usage
- `documentation-writing` — Rustdoc and Markdown conventions
- `logging-conventions` — tracing macro usage, log levels

## First-Principles Lens

When a design decision is non-obvious, apply these checks before flagging it:

1. **Is this driven by analogy?** "X does it this way" without matching constraints → question it
2. **Is this driven by convention?** "Best practice" without rationale → ask what problem it solves here
3. **Is the complexity necessary?** What's the simplest thing that satisfies the constraint?

If you have the `first-principles` skill installed, invoke `/first-principles` for deeper analysis of specific architectural decisions.
