---
description: Fetch PR review comments, prioritize them, and address each one
allowed-tools: Bash, Read, Edit, Write, Grep, Glob
---

Address all review comments on the current branch's PR.

## Step 1: Fetch Comments

1. Get PR info: `gh pr view --json number,title,url,state,reviews,comments`
2. Get inline comments: `gh api repos/{owner}/{repo}/pulls/{number}/comments`

Stop if no PR exists or it's closed/merged.

## Step 2: List & Prioritize

List each comment with: reviewer, file/line (if inline), and the comment text.

Categorize each:
- **BLOCKER** — requests a required change, flags a bug, or flags an AGENTS.md Invariant / data-loss / security issue
- **QUESTION** — asks for clarification (needs a reply)
- **SUGGESTION** — optional improvement (may ignore or adopt)
- **NITPICK** — minor style issue (fix quickly or ignore)

Address BLOCKERs first, then QUESTIONs.

Treat comments that touch these as BLOCKER by default (see `AGENTS.md`):
- LSN ack before durable persist
- Client-originated shape predicates
- Non-idempotent mutation apply
- Snapshot/stream overlap or gap
- Continuing after schema drift
- Unbounded replication slot hold
- Blocking the WAL reader on a slow client
- Untested WAL/offset/snapshot handoff changes (need real Postgres integration coverage)

## Step 3: Address Each

For each comment:
1. Read the relevant code for context
2. Either make the fix, or draft a reply if it needs discussion
3. **For BLOCKERs: show the user what you plan to change before editing**
4. When a fix touches WAL decoding, offsets, or snapshot handoff, plan/`make test-integration` (with `make db-up` + `TETHER_TEST_DSN`) — unit fakes alone are not enough
5. Do not "fix" flaky convergence by adding retries to `internal/e2e` — if the change makes it flake, the change is wrong

Summarize what you changed and any replies needed.

$ARGUMENTS
