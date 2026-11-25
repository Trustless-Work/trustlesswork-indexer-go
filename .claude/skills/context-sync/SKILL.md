---
name: context-sync
description: Mandatory reconciliation of every merge into main into Trustless-Work/trustlesswork-context, so the company context always reflects the current state of the Indexer. Use whenever a hook prints "CONTEXT SYNC REQUIRED", as soon as you learn a PR was merged into main (the user says so, you see it in git or gh output), or when asked to update or sync the context repo.
---

# Context sync — every merge into main lands in the context repo

`Trustless-Work/trustlesswork-context` is the company's long-term memory. This repo is
implementation truth. After every merge into `main`, the context must be reconciled
with what changed. Hooks in `.claude/settings.json` detect the pending range and
refuse to let the turn end until this skill closes it; the rule itself is in
`CLAUDE.md` and applies to any agent, with or without hooks.

**Standing rule.** Do not ask before starting, before pushing the docs branch, before
opening the PR in the context repo, or before merging that PR. The rule covers exactly
that: a branch, a PR and its merge in `trustlesswork-context`. It does not cover
pushing straight to any `main`, merging anything else, or changing this repo.

The helper is `.claude/skills/context-sync/scripts/context-sync.sh`; a pending notice
prints its absolute path. Call it `$CS` below. The last reconciled commit is kept per
machine in `~/.config/trustlesswork/context-sync/`; on a machine's first run the
current tip of `main` becomes the baseline.

## 1. Establish the range

Run from this repo:

```bash
sh "$CS" status
```

It fetches, then prints `base`, `head`, the first-parent log and the diff stat. If it
says `IN SYNC`, report that and stop. Several merges pending means one reconciliation
covering the whole range.

Read the range for real: `git log --first-parent base..head`, the PR bodies
(`gh pr view <n>`), and the diff of whatever looks structural. Read the repo's
`CLAUDE.md` and docs touched in the range; they usually state the new truth already.
Never read or copy `.env` values, keys or any secret.

## 2. Decide what the context must now say

List, in implementation terms, what is true at `head` that was not true at `base`.
These always need reconciling:

- envelope shape, schema version, routing keys or exchange: the contract with Core API v2
- what the Indexer detects or publishes: new fact types, events, deposits, withdrawals
- how escrows are discovered: approved WASM hashes, contract generations, factories
- delivery, deduplication, cursor, watchlist, reconciliation or gap semantics
- new chain, network or environment; RPC, deployment or infra change
- health, readiness, status or metrics surfaces operators rely on
- security fix or incident; a change to the threat model
- architecture migration; experiment promoted to production; repo role or status change

Then find every claim in the context that the range makes **stale, incomplete or
wrong**, even when the change itself looks small. Stale context is the failure this
skill exists to prevent.

Changes to this repo's own tooling, agent rules or CI are not context: they change how
we work, not what the Indexer is. See "What never goes in" in `destinations.md`.

## 3. Prepare the context repo

```bash
CTX="${CONTEXT_REPO_DIR:-$HOME/Developer/TrustlessWork/Code/trustlesswork-context}"
[ -d "$CTX/.git" ] || gh repo clone Trustless-Work/trustlesswork-context "$CTX"
git -C "$CTX" fetch origin
git -C "$CTX" switch -c docs/indexer-<head7> origin/main
```

If the clone has uncommitted work or the branch exists, do not discard anything: use
`git worktree add` for a clean checkout instead.

Read, from `origin/main`, before editing anything:

- `destinations.md` in this skill's directory: where each kind of change goes, the
  vocabulary and the form an update must keep
- `AGENTS.md`, `meta/context-map.md`, `repositories/documentation-agent.md`
- every destination file `destinations.md` names for this change, whole, so you know
  how the section you will edit already names and frames things
- `grep -rn -i 'indexer'` across the repo, to catch other claims the range
  makes stale

Check `gh pr list -R Trustless-Work/trustlesswork-context --state open`. If an open PR
already edits the same lines, do not push to its branch; write your change against
`main` and name the overlap in your PR body.

## 4. Write the update

The update must read as if the people who maintain the context repo wrote it. Follow
`destinations.md`, and the context repo's own rules, which win when they are stricter.

- **Edit the section that already owns the topic.** Use the destinations table. No
  new sections, no new files to hold repository facts, no parallel summaries, no
  per-commit changelog, no source mirror. A new file only for a dated `updates/` entry
  or a decision log, and only when `AGENTS.md` file-creation rules justify it.
- **Keep the vocabulary.** "Indexer", "Core API v2", "read model" and the other
  standard terms, exactly as `destinations.md` lists them and as the target section
  already uses them. Do not coin names.
- **Keep the nature.** Durable interpretation of what exists, what owns what and how
  it connects. No operating status, no tooling notes, no implementation detail that
  belongs in this repo.
- **Keep the form** of the file you edit: its header, its diagrams, its list style,
  its way of marking resolved questions.
- Preserve conflicts and open questions explicitly instead of picking a side.
- Respect `meta/sensitivity-model.md`. No secrets, no credentials, no unnecessary PII.
- English. No AI attribution anywhere.

Before committing, re-read your diff against the surrounding text. If a reader could
tell which paragraphs were added by this sync, rewrite them.

## 5. Commit, push, open the PR and merge it

Match the context repo's history, as `destinations.md` ("Commit and pull request")
describes: subject "Record the …" or "Update the …" without a conventional prefix,
small commits, branch `docs/indexer-<head7>`.

```bash
git -C "$CTX" add -A && git -C "$CTX" commit -m "<subject>"
git -C "$CTX" push -u origin docs/indexer-<head7>
gh pr create -R Trustless-Work/trustlesswork-context --base main \
  --head docs/indexer-<head7> --title "<subject>" --body "<body>"
```

PR body: one bullet per file changed, then `### Environment variables`, then
`### Source` with the compare link of the range.

Then merge it, with a merge commit as the context repo's history does:

```bash
gh pr merge <n> -R Trustless-Work/trustlesswork-context --merge
```

The PR stays as the record of the sync. If the merge is refused (conflict, required
review, failing check), do not force it and do not push to `main`: leave the PR open
and tell the user why.

## 6. Close the loop

Only after the PR exists (merged, or left open for a reason you report):

```bash
sh "$CS" mark <head> pr https://github.com/Trustless-Work/trustlesswork-context/pull/<n>
```

Then report to the user, briefly: range reconciled, files changed, PR link and whether
it was merged, remaining
gaps or contradictions.

### The only two other ways to close

- **Nothing to reconcile.** Allowed only when the range is purely internal (refactor,
  tests, formatting, dependency bumps with no behaviour change, or this repo's own
  tooling, agent rules and CI) **and** you grepped the
  context and found no claim it makes stale. Then
  `sh "$CS" mark <head> no-change "<what the range was and what you checked>"` and tell
  the user exactly that, naming the commits. When in doubt, it is not this case: write
  the update.
- **Skipped.** Only when the user says, in this conversation, to skip this sync:
  `sh "$CS" mark <head> skipped "<their reason>"`. Never on your own judgment, and
  never because the sync is inconvenient, slow or blocked.

If something blocks the sync (no network, no push access, context clone in a state you
cannot safely use), do not mark anything. Say what blocked it; the range stays pending
and the next session picks it up.
