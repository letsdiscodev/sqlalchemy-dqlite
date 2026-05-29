"""Shipped source must not reference the internal issue-tracking workflow
(issue folders/IDs, dev-phase labels) — meaningless to external readers."""

from __future__ import annotations

import pathlib
import re

import sqlalchemydqlite

# Dev-workflow markers. ``Phase`` capital-P only, so lowercase
# interpreter-shutdown "phase N" references are not flagged.
_WORKFLOW_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?:done|wont-fix|issues)/[A-Za-z0-9_.-]+"),
    re.compile(r"ISSUE-\d+"),
    re.compile(r"\b[Cc]ycle \d+"),
    re.compile(r"\bround \d+"),
    # "round" in the dev-episode sense; anchored to qualifiers so technical
    # uses ("round-trip", "a round of retries") are not flagged.
    re.compile(r"\b(?:prior|previous|earlier|widening|hardening|audit) round\b"),
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
