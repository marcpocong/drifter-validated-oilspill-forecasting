import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DRAFT_WORD = "Draft"
LOWER_DRAFT_WORD = "draft"
TARGET_VERSION = "2" + "2"
REDACTED_MANUSCRIPT_LABEL = "[REDACTED_MANUSCRIPT_LABEL]"
FORBIDDEN_MANUSCRIPT_PATTERN = re.compile(
    "".join(("Dra", "ft")) + r"[\s_-]*" + str(20 + 8),
    re.IGNORECASE,
)

SCAN_TARGETS = (
    REPO_ROOT / "ui",
    REPO_ROOT / "README.md",
    REPO_ROOT / "PANEL_QUICK_START.md",
    REPO_ROOT / "docs",
    REPO_ROOT / "output" / "Phase 3B March13-14 Final Output",
    REPO_ROOT / "output" / "Phase 3C DWH Final Output",
    REPO_ROOT / "output" / "figure_package_publication",
)

TEXT_SUFFIXES = {
    ".csv",
    ".css",
    ".html",
    ".json",
    ".md",
    ".ps1",
    ".py",
    ".txt",
    ".yaml",
    ".yml",
}

DISALLOWED_PATTERNS = (
    (re.compile(DRAFT_WORD + r"\s*" + TARGET_VERSION, re.IGNORECASE), "versioned draft label"),
    (re.compile(LOWER_DRAFT_WORD + TARGET_VERSION, re.IGNORECASE), "compacted versioned draft label"),
    (re.compile(DRAFT_WORD + r"\s+[0-9]+", re.IGNORECASE), "draft followed by an integer"),
    (re.compile(LOWER_DRAFT_WORD + r"[ _-]?[0-9]+", re.IGNORECASE), "draft filename/token with a number"),
)


def _iter_scanned_files() -> list[Path]:
    files: list[Path] = []
    for target in SCAN_TARGETS:
        if target.is_file():
            files.append(target)
        elif target.is_dir():
            files.extend(path for path in target.rglob("*") if path.is_file())
    return sorted(files)


def _find_disallowed(text: str) -> str:
    for pattern, reason in DISALLOWED_PATTERNS:
        match = pattern.search(text)
        if match:
            matched_text = FORBIDDEN_MANUSCRIPT_PATTERN.sub(
                REDACTED_MANUSCRIPT_LABEL,
                match.group(0),
            )
            return f"{reason}: {matched_text!r}"
    return ""


def test_panel_facing_surfaces_do_not_expose_draft_version_labels():
    violations: list[str] = []

    for path in _iter_scanned_files():
        rel_path = path.relative_to(REPO_ROOT).as_posix()
        path_violation = _find_disallowed(rel_path)
        if path_violation:
            violations.append(f"{rel_path} path contains {path_violation}")
            continue

        if path.suffix.lower() not in TEXT_SUFFIXES:
            continue

        text = path.read_text(encoding="utf-8", errors="replace")
        text_violation = _find_disallowed(text)
        if text_violation:
            violations.append(f"{rel_path} content contains {text_violation}")

    assert not violations, "\n".join(violations)
