#!/usr/bin/env bash
# Points this repository's git at the committed hooks. Run once per clone.
#
# core.hooksPath rather than copying into .git/hooks: hooks live in the
# repository, are reviewed like code, and cannot drift per machine.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
git config core.hooksPath githooks
chmod +x githooks/* 2>/dev/null || true
echo "hooks active: $(git config core.hooksPath)"
command -v gitleaks >/dev/null || echo "note: brew install gitleaks for the second layer"
