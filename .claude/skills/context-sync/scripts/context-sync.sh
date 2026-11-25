#!/bin/sh
# context-sync.sh — keeps Trustless-Work/trustlesswork-context in step with merges.
#
# This repo is "pending" while origin/main has commits that were not yet
# reconciled into the context repo. The marker (last reconciled sha) and a ledger
# live outside git, per machine, keyed by the origin slug.
#
# Hook modes (read the hook JSON on stdin):
#   check-session   SessionStart      fetch, print a notice when pending
#   check-prompt    UserPromptSubmit  fetch when the prompt talks about a merge
#   check-bash      PostToolUse/Bash  after gh pr merge / git pull|fetch|merge
#   check-stop      Stop              refuse to stop while pending
# Manual modes (run by the context-sync skill):
#   status
#   mark <sha> pr <context-pr-url>
#   mark <sha> no-change "<reason>"
#   mark <sha> skipped "<reason>"      only when the user says to skip
set -u

SELF_DIR=$(cd "$(dirname "$0")" && pwd)
SELF="$SELF_DIR/$(basename "$0")"
SOURCE_SLUG="${CONTEXT_SYNC_SOURCE:-Trustless-Work/trustlesswork-indexer-go}"
STATE_DIR="${CONTEXT_SYNC_STATE_DIR:-$HOME/.config/trustlesswork/context-sync}"
CONTEXT_SLUG="Trustless-Work/trustlesswork-context"

mode="${1:-status}"

json_field() { # json_field <json> <dotted.path>
  printf '%s' "$1" | node -e '
    let s = "";
    process.stdin.on("data", d => (s += d)).on("end", () => {
      try {
        let v = JSON.parse(s);
        for (const k of process.argv[1].split(".")) v = v == null ? v : v[k];
        process.stdout.write(v == null ? "" : String(v));
      } catch {}
    });' "$2" 2>/dev/null
}

input=""
case "$mode" in
  check-*)
    input=$(cat)
    cwd=$(json_field "$input" cwd)
    [ -n "$cwd" ] && [ -d "$cwd" ] && cd "$cwd"
    ;;
esac

# --- repo identity -----------------------------------------------------------
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || exit 0
url=$(git remote get-url origin 2>/dev/null) || exit 0
slug=$(printf '%s' "$url" | sed -E 's#^(git@|ssh://git@|https://)([^/:]+)[/:]##; s#\.git$##')
# Forks and renamed remotes are not this repo's main: do nothing there.
[ "$(printf '%s' "$slug" | tr 'A-Z' 'a-z')" = "$(printf '%s' "$SOURCE_SLUG" | tr 'A-Z' 'a-z')" ] || {
  case "$mode" in check-*) exit 0 ;; *) echo "origin is $slug, not $SOURCE_SLUG"; exit 1 ;; esac
}

# Only merges into main count, whatever the repo's default branch is.
branch=main
key=$(printf '%s' "$slug" | tr '/' '_' | tr 'A-Z' 'a-z')
marker="$STATE_DIR/$key.synced"
ledger="$STATE_DIR/$key.log"

fetch() {
  GIT_TERMINAL_PROMPT=0 GIT_SSH_COMMAND="ssh -o BatchMode=yes -o ConnectTimeout=5" \
    git -c http.lowSpeedLimit=1000 -c http.lowSpeedTime=5 \
    fetch --quiet origin "$branch" >/dev/null 2>&1 || true
}

head_sha() { git rev-parse --verify --quiet "refs/remotes/origin/$branch"; }

# Sets $base, $head, $count. Returns 0 when pending.
pending() {
  head=$(head_sha) || return 1
  if [ ! -f "$marker" ]; then
    # First run on this machine: start tracking from the current tip.
    mkdir -p "$STATE_DIR"
    printf '%s\n' "$head" >"$marker"
    printf '%s %s baseline first-run\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$head" >>"$ledger"
    return 1
  fi
  base=$(cat "$marker")
  [ "$base" = "$head" ] && return 1
  count=$(git rev-list --count "$base..$head" 2>/dev/null) || count="?"
  [ "$count" = "0" ] && return 1
  return 0
}

notice() {
  short_base=$(printf '%s' "$base" | cut -c1-7)
  short_head=$(printf '%s' "$head" | cut -c1-7)
  cat <<EOF
CONTEXT SYNC REQUIRED — $slug
origin/$branch has $count commit(s) not yet reconciled into $CONTEXT_SLUG
($short_base..$short_head).

This is mandatory: before ending this turn, follow the context-sync skill
(.claude/skills/context-sync/SKILL.md) and complete it. If the current request is
unrelated, finish it first, then run the skill in the same turn. If the skill is
already running, keep going. Do not ask for permission to start; it is a standing
rule of this repo (CLAUDE.md > Context sync) that every merge into main is
reconciled into the context repo.

Range:   git log --first-parent $base..$head
Helper:  sh "$SELF" status
Close:   sh "$SELF" mark $head pr <context-pr-url>
EOF
}

case "$mode" in
  check-session)
    fetch
    pending && notice
    exit 0
    ;;

  check-prompt)
    prompt=$(json_field "$input" prompt)
    if printf '%s' "$prompt" | grep -Eqi 'merge|mergi|mergea|fusion'; then fetch; fi
    pending && notice
    exit 0
    ;;

  check-bash)
    cmd=$(json_field "$input" tool_input.command)
    printf '%s' "$cmd" | grep -Eq 'gh pr merge|git (pull|fetch|merge|remote update)' || exit 0
    # The skill's own bookkeeping must not re-trigger the notice.
    printf '%s' "$cmd" | grep -q 'context-sync.sh' && exit 0
    printf '%s' "$cmd" | grep -q 'gh pr merge' && fetch
    if pending; then notice >&2; exit 2; fi
    exit 0
    ;;

  check-stop)
    [ "$(json_field "$input" stop_hook_active)" = "true" ] && exit 0
    if pending; then notice >&2; exit 2; fi
    exit 0
    ;;

  status)
    fetch
    if pending; then
      echo "PENDING $slug origin/$branch"
      echo "base=$base"
      echo "head=$head"
      echo "commits=$count"
      echo "--- first-parent log"
      git log --first-parent --format='%h %ad %an  %s' --date=short "$base..$head"
      echo "--- files"
      git diff --stat "$base..$head" | tail -40
    else
      echo "IN SYNC $slug origin/$branch at $(head_sha)"
    fi
    [ -f "$ledger" ] && { echo "--- ledger (last 5)"; tail -5 "$ledger"; }
    exit 0
    ;;

  mark)
    sha="${2:-}"; outcome="${3:-}"; detail="${4:-}"
    full=$(git rev-parse --verify --quiet "$sha^{commit}") || { echo "unknown commit: $sha" >&2; exit 1; }
    tip=$(head_sha) || { echo "origin/$branch not found" >&2; exit 1; }
    git merge-base --is-ancestor "$full" "$tip" || { echo "$sha is not on origin/$branch" >&2; exit 1; }
    case "$outcome" in
      pr)
        printf '%s' "$detail" | grep -Eq "^https://github\.com/$CONTEXT_SLUG/pull/[0-9]+$" || {
          echo "pr outcome needs the context PR url: https://github.com/$CONTEXT_SLUG/pull/<n>" >&2; exit 1; }
        ;;
      no-change|skipped)
        [ -n "$detail" ] || { echo "$outcome needs a reason" >&2; exit 1; }
        ;;
      *) echo "usage: mark <sha> pr <url> | no-change <reason> | skipped <reason>" >&2; exit 1 ;;
    esac
    mkdir -p "$STATE_DIR"
    printf '%s\n' "$full" >"$marker"
    printf '%s %s %s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$full" "$outcome" "$detail" >>"$ledger"
    echo "marked $slug synced at $full ($outcome)"
    exit 0
    ;;

  *) echo "unknown mode: $mode" >&2; exit 1 ;;
esac
