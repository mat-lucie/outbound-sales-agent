#!/bin/bash
# Sync repo skill definitions to a supported agent's skills directory.
#
# Per §0 invariant #6: no cron, no launchd, no scheduled jobs. Every
# Outbound Agent workflow runs as an operator-invoked slash command. Default target
# remains Claude Code; --codex targets Codex. Optional names limit which
# skills are replaced.
#
# Idempotent: re-running overwrites the destination files.
#
# Replaces scripts/install-cron.sh — there is no production cron.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SKILLS_SRC="$REPO_ROOT/skills"
if [[ "${1:-}" == "--codex" ]]; then
    SKILLS_DST="$HOME/.agents/skills"
    shift
else
    SKILLS_DST="$HOME/.claude/skills"
fi

SKILLS_DST="${OUTBOUND_SKILLS_DIR:-$SKILLS_DST}"

if [[ ! -d "$SKILLS_SRC" ]]; then
    echo "error: skills source not found: $SKILLS_SRC" >&2
    exit 1
fi

mkdir -p "$SKILLS_DST"

if [[ "$#" -gt 0 ]]; then
    skill_dirs=()
    for skill_name in "$@"; do
        if [[ ! "$skill_name" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
            echo "error: invalid skill name: $skill_name" >&2
            exit 2
        fi
        if [[ ! -f "$SKILLS_SRC/$skill_name/SKILL.md" ]]; then
            echo "error: skill not found: $skill_name" >&2
            exit 2
        fi
        skill_dirs+=("$SKILLS_SRC/$skill_name/")
    done
else
    skill_dirs=("$SKILLS_SRC"/*/)
fi

count=0
for skill_dir in "${skill_dirs[@]}"; do
    skill_name="$(basename "$skill_dir")"
    src="$skill_dir/SKILL.md"
    dst="$SKILLS_DST/$skill_name"

    if [[ ! -f "$src" ]]; then
        echo "warning: $skill_name has no SKILL.md; skipping" >&2
        continue
    fi

    mkdir -p "$dst"
    # Copy the whole skill dir: SKILL.md points at references/ files that
    # must travel with it, or the installed copy has dangling pointers.
    cp -Rf "$skill_dir"/. "$dst"/
    echo "synced: /$skill_name -> $dst/"
    count=$((count + 1))
done

echo ""
echo "synced $count skill(s) to $SKILLS_DST"
echo "verify the installed skill in the selected agent before a live run"
echo ""
echo "If you previously installed the Outbound Agent cron, remove it:"
echo "  crontab -e    # delete the MAILTO + Outbound Agent lines (daily, weekly)"
echo "  crontab -l    # verify they are gone"
