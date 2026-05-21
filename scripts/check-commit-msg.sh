#!/usr/bin/env bash
# check-commit-msg.sh — guard against workflow vocabulary in commit messages.
#
# The project convention forbids workflow tokens ("Round N", "Cycle N",
# "Phase N", "Bundle X", "ultrathink", "ISSUE-N", references to .md
# filenames) in commit messages. Two published SA commits leaked the
# pattern through review:
#
#   * 33d1d96 ("Rewrite _FORCE_CLOSE_TAIL_EXCEPTIONS header for both
#     do_close arms") — body opens with "Round 7 (commit 028af11)";
#     the durable referent is the commit hash, the workflow token is
#     ornamental.
#   * 2be34aef ("Anchor force-close-transport comment by symbol rather
#     than line range") — body contains "sibling commits in this round",
#     an indirect "this round" reference.
#
# Amending the published commits would rewrite SHAs the sibling
# packages (dbapi / client / wire) may reference; this script is the
# forward-looking guard so future SA commits do not regress. Mirrors
# the same-named script in ``python-dqlite-wire/scripts/`` so the
# workspace has a single shared rule.
#
# Usage:
#   # Lint a single message file (e.g. as a commit-msg hook):
#   scripts/check-commit-msg.sh .git/COMMIT_EDITMSG
#
#   # Lint the most recent commit body:
#   git log -1 --format=%B | scripts/check-commit-msg.sh -
#
#   # Lint a range of commits (e.g. everything since main diverged):
#   scripts/check-commit-msg.sh --range origin/main..HEAD
#
# Install as a git hook:
#   ln -s ../../scripts/check-commit-msg.sh .git/hooks/commit-msg
#
# Exit codes:
#   0 — clean
#   1 — workflow vocabulary detected (with file:line context)
#   2 — usage error
#
# Regex notes: a naive ``\b(round|cycle|phase|bundle)\s+\d+\b`` also
# flags legitimate prose like "phase 1 of two" or "cycle 3 of the
# test". The reviewer pitfall on the SA workflow-leak issue calls
# this out explicitly. We anchor on ``\s+\d+\b`` (digits-only,
# whitespace-required, word-boundary) so "phaseout 2024" and
# "round-trip" are not falsely accused; case-insensitive so the
# lowercase variants ("round 29") that slipped past review are
# still caught.
#
# Also flags ``ISSUE-<token>`` references and ``done/<file>.md``
# filename mentions — both classes the reviewer flagged as belonging
# in the same rejected set as the Round/Cycle/Phase/Bundle tokens.

set -euo pipefail

# Capital-leading-or-lowercase workflow tokens with a numeric counter.
WORKFLOW_RE='\b(round|cycle|phase|bundle)\s+[0-9]+\b'

# ISSUE-N tokens (e.g. ISSUE-T4, ISSUE-42) and done/ filename refs.
ISSUE_RE='\bISSUE-[A-Z0-9]+\b'
DONE_RE='\bdone/[A-Za-z0-9._-]+\.md\b'

# "ultrathink" / "this round" / "next round" prose markers.
PROSE_RE='\b(ultrathink|this round|next round|prior round|earlier round)\b'

scan_text() {
    # $1 — label (file path or commit SHA) for the report.
    # stdin — message body to scan.
    local label="$1"
    local body
    body="$(cat)"
    local rc=0
    # Strip comment lines (git commit message convention: '#' at column 0).
    local stripped
    stripped="$(printf '%s\n' "$body" | grep -v '^#' || true)"

    if printf '%s\n' "$stripped" | grep -inE "$WORKFLOW_RE" >/dev/null; then
        printf 'check-commit-msg: %s: workflow token (Round/Cycle/Phase/Bundle N) detected\n' "$label" >&2
        printf '%s\n' "$stripped" | grep -inE "$WORKFLOW_RE" >&2 || true
        rc=1
    fi
    if printf '%s\n' "$stripped" | grep -inE "$ISSUE_RE" >/dev/null; then
        printf 'check-commit-msg: %s: ISSUE-N token detected\n' "$label" >&2
        printf '%s\n' "$stripped" | grep -inE "$ISSUE_RE" >&2 || true
        rc=1
    fi
    if printf '%s\n' "$stripped" | grep -inE "$DONE_RE" >/dev/null; then
        printf 'check-commit-msg: %s: done/ filename reference detected\n' "$label" >&2
        printf '%s\n' "$stripped" | grep -inE "$DONE_RE" >&2 || true
        rc=1
    fi
    if printf '%s\n' "$stripped" | grep -inE "$PROSE_RE" >/dev/null; then
        printf 'check-commit-msg: %s: workflow prose marker detected\n' "$label" >&2
        printf '%s\n' "$stripped" | grep -inE "$PROSE_RE" >&2 || true
        rc=1
    fi
    return $rc
}

usage() {
    cat >&2 <<'EOF'
Usage:
  check-commit-msg.sh <file>           lint a single commit-message file
  check-commit-msg.sh -                lint message read from stdin
  check-commit-msg.sh --range <rev>    lint every commit in the rev range
EOF
    exit 2
}

if [ $# -lt 1 ]; then
    usage
fi

case "$1" in
    --range)
        if [ $# -ne 2 ]; then
            usage
        fi
        rc=0
        for sha in $(git rev-list "$2"); do
            if ! git log -1 --format=%B "$sha" | scan_text "$sha"; then
                rc=1
            fi
        done
        exit $rc
        ;;
    -)
        scan_text "<stdin>"
        ;;
    -h|--help)
        usage
        ;;
    *)
        if [ ! -f "$1" ]; then
            printf 'check-commit-msg: %s: not a file\n' "$1" >&2
            exit 2
        fi
        scan_text "$1" < "$1"
        ;;
esac
