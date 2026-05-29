"""Shipped source must not reference the internal issue-tracking workflow.

Published library code should not point at the project's ``issues/`` /
``done/`` / ``wont-fix/`` folders, issue IDs, or development-phase
labels — those are meaningless (and can dangle) for an external reader
of the package. This test scans the shipped ``src`` tree and fails if
any such reference is reintroduced.
"""

from __future__ import annotations

import pathlib
import re

import sqlalchemydqlite

# Patterns that denote the internal development workflow. ``Phase`` is
# matched only with a capital ``P`` so legitimate lowercase
# interpreter-shutdown "phase N" references are NOT flagged.
_WORKFLOW_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?:done|wont-fix|issues)/[A-Za-z0-9_.-]+"),
    re.compile(r"ISSUE-\d+"),
    re.compile(r"\b[Cc]ycle \d+"),
    re.compile(r"\bround \d+"),
    re.compile(r"\b[Bb]undle [A-Z]\b"),
    re.compile(r"\bPhase \d+\b"),
)


def _shipped_source_files() -> list[pathlib.Path]:
    package_dir = pathlib.Path(sqlalchemydqlite.__file__).parent
    return sorted(package_dir.rglob("*.py"))


def test_no_workflow_references_in_shipped_source() -> None:
    offenders: list[str] = []
    for path in _shipped_source_files():
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for pattern in _WORKFLOW_PATTERNS:
                match = pattern.search(line)
                if match is not None:
                    offenders.append(f"{path.name}:{lineno}: {match.group(0)!r}")
    assert not offenders, "workflow references found in shipped source:\n" + "\n".join(offenders)
