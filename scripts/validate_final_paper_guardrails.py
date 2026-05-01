"""Cheap final-paper consistency guardrails.

This validator audits repository metadata, docs, and text-like tracked files
only. It does not run models, fetch data, launch Docker, or rewrite scientific
outputs.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import validate_archive_registry as archive_validator
from scripts import validate_data_sources_registry as data_sources_validator
from scripts import validate_paper_to_output_registry as paper_registry_validator

REDACTED_MANUSCRIPT_LABEL = "[REDACTED_MANUSCRIPT_LABEL]"

TEXT_SUFFIXES = {
    ".bat",
    ".cfg",
    ".cmd",
    ".csv",
    ".css",
    ".geojson",
    ".html",
    ".ini",
    ".ipynb",
    ".json",
    ".md",
    ".ps1",
    ".py",
    ".rst",
    ".sh",
    ".toml",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
}

LAUNCHER_REQUIRED_FIELDS = {
    "entry_id",
    "label",
    "description",
    "category_id",
    "thesis_role",
    "manuscript_section",
    "claim_boundary",
    "run_kind",
    "safe_default",
    "rerun_cost",
}

FINAL_PRIMARY_EVIDENCE_ENTRY_IDS = {
    "phase1_mindoro_focus_provenance",
    "mindoro_phase3b_primary_public_validation",
    "dwh_reportable_bundle",
    "mindoro_reportable_core",
}

ARCHIVE_ROLES = {"archive_provenance", "legacy_support"}
ARCHIVE_VISIBILITIES = {"visible_archive", "hidden_alias", "hidden_experimental", "read_only_only"}

CLAIM_BOUNDARY_PHRASES = {
    "B1 coastal-neighborhood usefulness": ("b1", "coastal-neighborhood usefulness"),
    "no exact 1 km overlap": ("not exact 1 km overlap",),
    "PyGNOME comparator-only": ("pygnome is comparator-only",),
    "DWH external transfer only": ("dwh is external transfer validation only",),
    "oil-type support/context only": ("oil-type", "support/context only"),
    "secondary 2016 support only": ("secondary 2016", "support only", "not public-spill validation"),
}

FINAL_ALIGNMENT_SNIPPETS = {
    "focused Phase 1 design": (
        "- Workflow mode: `phase1_mindoro_focus_pre_spill_2016_2023`",
        "- Historical window: `2016-01-01` to `2023-03-02`",
        "- Focused box: `[118.751, 124.305, 10.620, 16.026]`",
        "- Full strict accepted segments: `65`",
        "- February-April ranked subset: `19`",
        "- Selected recipe: `cmems_gfs`",
        "| `cmems_gfs` | `4.5886` | `4.6305` |",
        "| `cmems_era5` | `4.6237` | `4.5916` |",
        "| `hycom_gfs` | `4.7027` | `4.9263` |",
        "| `hycom_era5` | `4.7561` | `5.0106` |",
    ),
    "Mindoro B1 primary validation": (
        "- March 13 public NOAA/NESDIS observation extent is the seed; March 14 public NOAA/NESDIS observation extent is the target.",
        "- March 13 and March 14 are independent day-specific public-observation products.",
        "Mindoro B1 is the March 13-14 primary public-observation validation row.",
        "It is interpreted as coastal-neighborhood usefulness, not exact 1 km overlap.",
        "| FSS 1 km | `0.0000` |",
        "| FSS 3 km | `0.0441` |",
        "| FSS 5 km | `0.1371` |",
        "| FSS 10 km | `0.2490` |",
        "| Mean FSS | `0.1075` |",
        "| Forecast cells | `5` |",
        "| Observed cells | `22` |",
        "| Nearest distance | `1414.21 m` |",
        "| Centroid distance | `7358.16 m` |",
        "| IoU | `0.0` |",
        "| Dice | `0.0` |",
    ),
    "Mindoro same-case comparator": (
        "| OpenDrift promoted p50 branch | `5` | `1414.21 m` | `0.0000 / 0.0441 / 0.1371 / 0.2490` | `0.1075` |",
        "| PyGNOME deterministic comparator | `6` | `6082.76 m` | `0.0000 / 0.0000 / 0.0000 / 0.0244` | `0.0061` |",
        "PyGNOME remains comparator-only in this package. It is never observation truth.",
    ),
    "DWH external transfer": (
        "- Case ID: `CASE_DWH_RETRO_2010_72H`",
        "- Forcing stack: `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes`",
        "- Interpretation: external transfer validation only, not Mindoro recalibration.",
        "| Deterministic | `0.5568` |",
        "| Ensemble p50 | `0.5389` |",
        "| Ensemble p90 | `0.4966` |",
        "| PyGNOME comparator | `0.3612` |",
    ),
    "probability semantics": (
        "- `mask_p50` means `P >= 0.50`; it is the preferred probabilistic footprint.",
        "- `mask_p90` means `P >= 0.90`; it is a conservative support/comparison product only.",
        "- Do not label `mask_p90` as a broad envelope.",
    ),
    "oil-type support": (
        "These values are support/context only, not primary validation.",
        "| Light oil | `0.02%` | `4 h` | `11` | pass |",
        "| Fixed-base medium-heavy proxy | `0.61%` | `4 h` | `10` | flagged |",
        "| Heavier oil | `0.63%` | `4 h` | `11` | pass |",
    ),
    "secondary 2016 support": (
        "The 2016 material provides direct drifter-track and legacy OpenDrift-PyGNOME FSS support only.",
        "It is not public-spill validation and is not a replacement for Mindoro B1 or DWH.",
    ),
}


def _forbidden_label_pattern() -> re.Pattern[str]:
    word = "".join(("Dra", "ft"))
    number = str(20 + 8)
    return re.compile(re.escape(word) + r"[\s_-]*" + re.escape(number), re.IGNORECASE)


def _redact_forbidden_label(value: str) -> str:
    return _forbidden_label_pattern().sub(REDACTED_MANUSCRIPT_LABEL, value)


def _tracked_files(root: Path = REPO_ROOT) -> list[str]:
    output = subprocess.check_output(
        ["git", "ls-files", "-z"],
        cwd=root,
        encoding="utf-8",
        errors="replace",
    )
    return [item for item in output.split("\0") if item]


def _is_text_like(path: Path) -> bool:
    return path.suffix.lower() in TEXT_SUFFIXES


def find_forbidden_manuscript_label(root: Path = REPO_ROOT) -> list[str]:
    """Return redacted tracked-file locations containing the forbidden label."""

    pattern = _forbidden_label_pattern()
    violations: list[str] = []

    for rel_path in _tracked_files(root):
        redacted_path = _redact_forbidden_label(rel_path)
        if pattern.search(rel_path):
            violations.append(f"{redacted_path}: path")
            continue

        path = root / rel_path
        if not _is_text_like(path):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line_number, line in enumerate(text.splitlines(), start=1):
            if pattern.search(line):
                violations.append(f"{redacted_path}:{line_number}")
                break

    return sorted(violations)


def _load_launcher_matrix(root: Path = REPO_ROOT) -> dict[str, Any]:
    path = root / "config" / "launcher_matrix.json"
    return json.loads(path.read_text(encoding="utf-8"))


def launcher_matrix_schema_issues(root: Path = REPO_ROOT) -> list[str]:
    matrix = _load_launcher_matrix(root)
    entries = matrix.get("entries")
    if not isinstance(entries, list) or not entries:
        return ["config/launcher_matrix.json must contain a non-empty entries list"]

    issues: list[str] = []
    entry_ids = {str(entry.get("entry_id")) for entry in entries if isinstance(entry, dict)}
    for index, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            issues.append(f"entry {index}: must be an object")
            continue
        entry_id = str(entry.get("entry_id") or f"<entry-{index}>")

        for field in sorted(LAUNCHER_REQUIRED_FIELDS):
            if field not in entry:
                issues.append(f"{entry_id}: missing required field `{field}`")
            elif entry.get(field) in ("", None, []):
                issues.append(f"{entry_id}: required field `{field}` is empty")

        if not isinstance(entry.get("safe_default"), bool):
            issues.append(f"{entry_id}: safe_default must be boolean")

        steps = entry.get("steps")
        alias_of = str(entry.get("alias_of") or "").strip()
        if not steps and not alias_of:
            issues.append(f"{entry_id}: must define steps or alias_of")
        if steps is not None and (not isinstance(steps, list) or not steps):
            issues.append(f"{entry_id}: steps must be a non-empty list when present")
        if alias_of and alias_of not in entry_ids:
            issues.append(f"{entry_id}: alias_of points to missing launcher entry `{alias_of}`")

    return issues


def launcher_role_issues(root: Path = REPO_ROOT) -> list[str]:
    entries = _load_launcher_matrix(root).get("entries") or []
    issues: list[str] = []
    primary_ids = {
        str(entry.get("entry_id"))
        for entry in entries
        if isinstance(entry, dict) and entry.get("thesis_role") == "primary_evidence"
    }

    extra_primary = sorted(primary_ids - FINAL_PRIMARY_EVIDENCE_ENTRY_IDS)
    missing_primary = sorted(FINAL_PRIMARY_EVIDENCE_ENTRY_IDS - primary_ids)
    if extra_primary:
        issues.append("primary_evidence contains non-final-paper launcher entries: " + ", ".join(extra_primary))
    if missing_primary:
        issues.append("primary_evidence is missing final-paper launcher entries: " + ", ".join(missing_primary))

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        entry_id = str(entry.get("entry_id") or "<missing>")
        thesis_role = str(entry.get("thesis_role") or "")
        archive_status = str(entry.get("archive_status") or "")
        launcher_visibility = str(entry.get("launcher_visibility") or "")
        archived = bool(archive_status) or thesis_role in ARCHIVE_ROLES or launcher_visibility in ARCHIVE_VISIBILITIES
        experimental = (
            entry.get("experimental_only") is True
            or archive_status == "experimental_only"
            or launcher_visibility == "hidden_experimental"
        )

        if entry_id in primary_ids:
            if entry.get("thesis_facing") is not True or entry.get("reportable") is not True:
                issues.append(f"{entry_id}: primary_evidence entries must be thesis_facing=true and reportable=true")

        if archived:
            if entry.get("thesis_facing") is True:
                issues.append(f"{entry_id}: archive/legacy launcher entry must not be thesis_facing=true")
            if entry.get("reportable") is True:
                issues.append(f"{entry_id}: archive/legacy launcher entry must not be reportable=true")

        if experimental:
            if entry.get("menu_hidden") is not True:
                issues.append(f"{entry_id}: hidden experimental entry must stay menu_hidden=true")
            if entry.get("thesis_facing") is not False:
                issues.append(f"{entry_id}: hidden experimental entry must set thesis_facing=false")
            if entry.get("reportable") is not False:
                issues.append(f"{entry_id}: hidden experimental entry must set reportable=false")

    return issues


def _read_docs_and_config_text(root: Path = REPO_ROOT) -> str:
    paths = [root / "README.md", root / "PANEL_QUICK_START.md"]
    paths.extend(sorted((root / "docs").glob("*.md")))
    for suffix in ("*.json", "*.yaml", "*.yml", "*.csv"):
        paths.extend(sorted((root / "config").glob(suffix)))

    parts: list[str] = []
    for path in paths:
        if path.exists():
            parts.append(path.read_text(encoding="utf-8", errors="replace"))
    return "\n".join(parts).lower()


def claim_boundary_issues(root: Path = REPO_ROOT) -> list[str]:
    combined = _read_docs_and_config_text(root)
    issues: list[str] = []
    for label, phrases in CLAIM_BOUNDARY_PHRASES.items():
        if not all(phrase in combined for phrase in phrases):
            issues.append(f"docs/config are missing bounded claim phrase group: {label}")
    return issues


def final_alignment_fact_issues(root: Path = REPO_ROOT) -> list[str]:
    alignment_path = root / "docs" / "FINAL_PAPER_ALIGNMENT.md"
    if not alignment_path.exists():
        return ["docs/FINAL_PAPER_ALIGNMENT.md is missing"]

    text = alignment_path.read_text(encoding="utf-8", errors="replace")
    issues: list[str] = []
    for label, snippets in FINAL_ALIGNMENT_SNIPPETS.items():
        missing = [snippet for snippet in snippets if snippet not in text]
        if missing:
            issues.append(f"docs/FINAL_PAPER_ALIGNMENT.md is missing final-paper fact group `{label}`")
    return issues


def probability_semantics_issues(root: Path = REPO_ROOT) -> list[str]:
    scan_roots = [root / "src", root / "ui", root / "docs", root / "config"]
    scan_files = [root / "README.md", root / "PANEL_QUICK_START.md"]
    for scan_root in scan_roots:
        if scan_root.exists():
            scan_files.extend(path for path in scan_root.rglob("*") if path.is_file() and _is_text_like(path))

    positive_p90_envelope = re.compile(r"\bp90\b.{0,100}\bbroad(?:er|est)?\b.{0,100}\benvelope\b", re.IGNORECASE)
    negation = re.compile(r"\b(no|not|never|do not|do n't|should not|must not)\b", re.IGNORECASE)
    issues: list[str] = []
    for path in sorted(set(scan_files)):
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line_number, line in enumerate(lines, start=1):
            if positive_p90_envelope.search(line) and not negation.search(line):
                rel_path = path.relative_to(root).as_posix()
                issues.append(f"{rel_path}:{line_number}: p90 must be support/comparison only, not an envelope claim")
    return issues


def archive_registry_issues(root: Path = REPO_ROOT) -> list[str]:
    issues = list(archive_validator.validate_archive_registry())
    try:
        registry = archive_validator._load_archive_registry(root / "config" / "archive_registry.yaml")
    except Exception as exc:
        return [*issues, f"archive registry could not be parsed for field audit: {exc}"]

    launcher_entries = _load_launcher_matrix(root).get("entries", [])
    launcher_ids = {
        str(entry.get("entry_id"))
        for entry in _load_launcher_matrix(root).get("entries", [])
        if isinstance(entry, dict)
    }
    if not launcher_entries:
        issues.append("launcher matrix has no entries for archive cross-check")

    for item in registry.get("archive_items") or []:
        archive_id = str(item.get("archive_id") or "<missing>")
        if not str(item.get("claim_boundary") or "").strip():
            issues.append(f"{archive_id}: missing claim_boundary")
        if not str(item.get("why_archived") or "").strip():
            issues.append(f"{archive_id}: missing why_archived")
        launcher_entry_id = item.get("launcher_entry_id")
        if launcher_entry_id and str(launcher_entry_id) not in launcher_ids:
            issues.append(f"{archive_id}: launcher_entry_id is not in launcher_matrix")
        if not launcher_entry_id and item.get("path_only") is not True:
            issues.append(f"{archive_id}: missing launcher_entry_id requires path_only=true")
    return issues


def paper_to_output_registry_issues() -> list[str]:
    registry = paper_registry_validator._load_registry(paper_registry_validator.REGISTRY_PATH)
    entries = registry.get("entries")
    if not isinstance(entries, list) or not entries:
        return ["paper-to-output registry must contain a non-empty entries list"]

    issues: list[str] = []
    for raw_entry in entries:
        if not isinstance(raw_entry, dict):
            issues.append("paper-to-output registry entry is not a mapping")
            continue
        report = paper_registry_validator._validate_entry(raw_entry)
        for error in report["errors"]:
            issues.append(f"{report['paper_item_id']}: {error}")
        for warning in report["warnings"]:
            issues.append(f"{report['paper_item_id']}: optional/missing output status is not marked honestly: {warning}")
    return issues


def data_sources_registry_issues() -> list[str]:
    issues = list(data_sources_validator.validate_registry())
    try:
        payload, _raw_text = data_sources_validator._load_registry()
        entries = data_sources_validator._source_entries(payload)
    except Exception as exc:
        return [*issues, f"data-source registry could not be parsed for explicit-role audit: {exc}"]

    for entry_key, entry in entries.items():
        role = str(entry.get("role") or "").strip()
        if not role:
            issues.append(f"{entry_key}: source role is empty")
        if not isinstance(entry.get("secrets_required"), bool):
            issues.append(f"{entry_key}: secrets_required must be explicitly true or false")

        role_text = role.lower()
        identity_text = " ".join(
            str(entry.get(field) or "").lower()
            for field in ("label", "provider", "product_or_layer")
        )
        combined = " ".join(
            str(entry.get(field) or "").lower()
            for field in ("label", "provider", "product_or_layer", "evidence_boundary")
        )
        model_like = any(term in identity_text for term in ("opendrift", "openoil", "pygnome", "gnome", "model output"))
        if model_like and role_text == "observation validation mask":
            issues.append(f"{entry_key}: model/tool source is marked as observation truth")
        direct_bad = (
            "model output is observation truth",
            "model output as observation truth",
            "pygnome is observation truth",
            "opendrift is observation truth",
        )
        if any(phrase in combined for phrase in direct_bad):
            issues.append(f"{entry_key}: model output is described as observation truth")

    return issues


def run_all_guardrails(root: Path = REPO_ROOT) -> dict[str, list[str]]:
    return {
        "forbidden_manuscript_label": find_forbidden_manuscript_label(root),
        "launcher_matrix_schema": launcher_matrix_schema_issues(root),
        "launcher_roles": launcher_role_issues(root),
        "claim_boundaries": claim_boundary_issues(root),
        "final_alignment_facts": final_alignment_fact_issues(root),
        "probability_semantics": probability_semantics_issues(root),
        "archive_registry": archive_registry_issues(root),
        "paper_to_output_registry": paper_to_output_registry_issues(),
        "data_sources_registry": data_sources_registry_issues(),
    }


def main() -> int:
    results = run_all_guardrails(REPO_ROOT)
    issue_count = sum(len(issues) for issues in results.values())
    if issue_count:
        print("Final-paper guardrail validation: FAIL")
        for group, issues in results.items():
            if not issues:
                continue
            print(f"- {group}:")
            for issue in issues:
                print(f"  - {_redact_forbidden_label(issue)}")
        return 1

    print("Final-paper guardrail validation: PASS")
    print("Checked tracked text labels, launcher routing, claim boundaries, exact alignment facts, registries, and p90 semantics.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
