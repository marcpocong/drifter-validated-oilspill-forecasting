from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_REGISTRY_PATH = REPO_ROOT / "config" / "archive_registry.yaml"
LAUNCHER_MATRIX_PATH = REPO_ROOT / "config" / "launcher_matrix.json"

REQUIRED_ARCHIVE_IDS = {
    "phase1_regional_reference_rerun",
    "mindoro_march13_14_phase1_focus_trial",
    "mindoro_march6_recovery_sensitivity",
    "mindoro_march23_extended_public_stress_test",
    "phase3b_mindoro_march3_4_philsa_5000_experiment",
    "mindoro_mar09_12_multisource_experiment",
    "phase3b_mindoro_march13_14_reinit_5000_experiment",
    "prototype_legacy_final_figures",
    "prototype_2021_bundle",
    "prototype_legacy_bundle",
    "mindoro_march3_6_base_case_archive",
    "prototype_2016_support_surfaces",
    "root_debug_artifacts_prompt1",
}
REQUIRED_ITEM_FIELDS = {
    "archive_id",
    "path_patterns",
    "status",
    "thesis_facing",
    "reportable",
    "paper_reflected_as",
    "claim_boundary",
    "why_archived",
    "rerun_allowed",
    "launcher_visibility",
    "protected_outputs",
    "notes",
}
ALLOWED_STATUSES = {
    "archive_provenance",
    "legacy_support",
    "experimental_only",
    "repository_only_development",
}
ALLOWED_VISIBILITIES = {
    "visible_archive",
    "hidden_alias",
    "hidden_experimental",
    "read_only_only",
}
PRIMARY_EVIDENCE_ENTRY_IDS = {
    "phase1_mindoro_focus_provenance",
    "mindoro_phase3b_primary_public_validation",
    "dwh_reportable_bundle",
    "mindoro_reportable_core",
}


def _parse_scalar(raw_value: str) -> Any:
    value = raw_value.strip()
    if value == "":
        return ""
    if value in {"null", "Null", "NULL", "~"}:
        return None
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        return value


def _split_key_value(payload: str, line_number: int) -> tuple[str, Any]:
    if ":" not in payload:
        raise ValueError(f"Line {line_number}: expected key/value pair")
    key, raw_value = payload.split(":", 1)
    key = key.strip()
    if not key:
        raise ValueError(f"Line {line_number}: empty key")
    return key, _parse_scalar(raw_value)


def _load_archive_registry(path: Path) -> dict[str, Any]:
    registry: dict[str, Any] = {}
    items: list[dict[str, Any]] = []
    current_item: dict[str, Any] | None = None
    current_list_key: str | None = None
    in_items = False

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        stripped = raw_line.strip()

        if indent == 0:
            current_item = None
            current_list_key = None
            if stripped == "archive_items:":
                registry["archive_items"] = items
                in_items = True
                continue
            key, value = _split_key_value(stripped, line_number)
            registry[key] = value
            continue

        if not in_items:
            raise ValueError(f"Line {line_number}: nested content before archive_items")

        if indent == 2 and stripped.startswith("- "):
            current_item = {}
            items.append(current_item)
            current_list_key = None
            payload = stripped[2:].strip()
            if payload:
                key, value = _split_key_value(payload, line_number)
                current_item[key] = value
            continue

        if current_item is None:
            raise ValueError(f"Line {line_number}: archive item field without an item")

        if indent == 4:
            key, value = _split_key_value(stripped, line_number)
            if value == "":
                current_item[key] = []
                current_list_key = key
            else:
                current_item[key] = value
                current_list_key = None
            continue

        if indent == 6 and stripped.startswith("- "):
            if current_list_key is None:
                raise ValueError(f"Line {line_number}: list value without a list field")
            current_item.setdefault(current_list_key, []).append(_parse_scalar(stripped[2:].strip()))
            continue

        raise ValueError(f"Line {line_number}: unsupported YAML shape")

    return registry


def _load_launcher_matrix(path: Path) -> dict[str, dict[str, Any]]:
    matrix = json.loads(path.read_text(encoding="utf-8"))
    return {entry["entry_id"]: entry for entry in matrix.get("entries", [])}


def validate_archive_registry() -> list[str]:
    issues: list[str] = []
    if not ARCHIVE_REGISTRY_PATH.exists():
        return [f"Missing archive registry: {ARCHIVE_REGISTRY_PATH.relative_to(REPO_ROOT)}"]
    if not LAUNCHER_MATRIX_PATH.exists():
        return [f"Missing launcher matrix: {LAUNCHER_MATRIX_PATH.relative_to(REPO_ROOT)}"]

    registry = _load_archive_registry(ARCHIVE_REGISTRY_PATH)
    entries = _load_launcher_matrix(LAUNCHER_MATRIX_PATH)
    items = registry.get("archive_items")
    if not isinstance(items, list) or not items:
        return ["archive_registry.yaml must contain a non-empty archive_items list"]

    seen_ids: set[str] = set()
    for index, item in enumerate(items, start=1):
        archive_id = str(item.get("archive_id") or f"<item-{index}>")
        if archive_id in seen_ids:
            issues.append(f"{archive_id}: duplicate archive_id")
        seen_ids.add(archive_id)

        missing = sorted(REQUIRED_ITEM_FIELDS - set(item))
        if missing:
            issues.append(f"{archive_id}: missing fields: {', '.join(missing)}")

        if item.get("status") not in ALLOWED_STATUSES:
            issues.append(f"{archive_id}: invalid status {item.get('status')!r}")
        if item.get("launcher_visibility") not in ALLOWED_VISIBILITIES:
            issues.append(f"{archive_id}: invalid launcher_visibility {item.get('launcher_visibility')!r}")
        if item.get("thesis_facing") is not False:
            issues.append(f"{archive_id}: archive registry items must set thesis_facing=false")
        if not isinstance(item.get("reportable"), bool):
            issues.append(f"{archive_id}: reportable must be boolean")
        if not isinstance(item.get("rerun_allowed"), bool):
            issues.append(f"{archive_id}: rerun_allowed must be boolean")
        if not isinstance(item.get("path_patterns"), list) or not item.get("path_patterns"):
            issues.append(f"{archive_id}: path_patterns must be a non-empty list")
        if not isinstance(item.get("protected_outputs"), list):
            issues.append(f"{archive_id}: protected_outputs must be a list")

        launcher_entry_id = item.get("launcher_entry_id")
        path_only = bool(item.get("path_only"))
        if launcher_entry_id:
            entry = entries.get(str(launcher_entry_id))
            if entry is None:
                issues.append(f"{archive_id}: launcher_entry_id {launcher_entry_id!r} is not in launcher_matrix")
                continue
            if entry.get("thesis_facing") is True:
                issues.append(f"{archive_id}: launcher entry must not be thesis_facing=true")
            if item.get("reportable") is False and entry.get("reportable") is True:
                issues.append(f"{archive_id}: launcher entry must not be reportable=true")
            if entry.get("archive_registry_id") and entry.get("archive_registry_id") != archive_id:
                issues.append(
                    f"{archive_id}: launcher archive_registry_id is {entry.get('archive_registry_id')!r}"
                )
            if item.get("status") == "experimental_only":
                if entry.get("experimental_only") is not True:
                    issues.append(f"{archive_id}: experimental launcher entry must set experimental_only=true")
                if entry.get("menu_hidden") is not True:
                    issues.append(f"{archive_id}: experimental launcher entry must be menu_hidden=true")
        elif not path_only:
            issues.append(f"{archive_id}: missing launcher_entry_id requires path_only=true")

    missing_required = sorted(REQUIRED_ARCHIVE_IDS - seen_ids)
    if missing_required:
        issues.append("archive registry missing required archive IDs: " + ", ".join(missing_required))

    registry_ids = seen_ids
    for entry_id, entry in sorted(entries.items()):
        if entry.get("archive_status"):
            registry_id = entry.get("archive_registry_id")
            if not registry_id:
                issues.append(f"{entry_id}: archive_status entry is missing archive_registry_id")
            elif registry_id not in registry_ids:
                issues.append(f"{entry_id}: archive_registry_id {registry_id!r} is not in archive registry")
            if entry.get("thesis_facing") is True:
                issues.append(f"{entry_id}: archived launcher entry cannot be thesis_facing=true")

        if entry.get("experimental_only") is True or entry.get("launcher_visibility") == "hidden_experimental":
            if entry.get("menu_hidden") is not True:
                issues.append(f"{entry_id}: hidden experimental entry must be menu_hidden=true")
            if entry.get("thesis_role") == "primary_evidence":
                issues.append(f"{entry_id}: hidden experimental entry cannot be primary_evidence")
            if entry.get("thesis_facing") is not False:
                issues.append(f"{entry_id}: hidden experimental entry must set thesis_facing=false")
            if entry.get("reportable") is not False:
                issues.append(f"{entry_id}: hidden experimental entry must set reportable=false")
            if entry.get("archive_status") != "experimental_only":
                issues.append(f"{entry_id}: hidden experimental entry must set archive_status=experimental_only")

        if entry.get("thesis_role") == "primary_evidence" and entry_id not in PRIMARY_EVIDENCE_ENTRY_IDS:
            issues.append(f"{entry_id}: primary_evidence is limited to final manuscript evidence entries")

    return issues


def main() -> int:
    try:
        issues = validate_archive_registry()
    except Exception as exc:
        print(f"Archive registry validation failed to run: {exc}", file=sys.stderr)
        return 2

    if issues:
        print("Archive registry validation: FAIL")
        for issue in issues:
            print(f"  - {issue}")
        return 1

    print("Archive registry validation: PASS")
    print(f"Checked {ARCHIVE_REGISTRY_PATH.relative_to(REPO_ROOT)} against {LAUNCHER_MATRIX_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
