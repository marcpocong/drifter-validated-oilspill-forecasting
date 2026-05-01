"""Read-only panel verification of manuscript-facing stored results."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPO_ROOT / "config" / "panel_paper_expected_values.yaml"
OUTPUT_DIR = REPO_ROOT / "output" / "panel_review_check"
CSV_OUTPUT_PATH = OUTPUT_DIR / "panel_results_match_check.csv"
JSON_OUTPUT_PATH = OUTPUT_DIR / "panel_results_match_check.json"
MARKDOWN_OUTPUT_PATH = OUTPUT_DIR / "panel_results_match_check.md"
MANIFEST_OUTPUT_PATH = OUTPUT_DIR / "panel_review_manifest.json"
OPTIONAL_WORKBOOK_OUTPUT_PATH = OUTPUT_DIR / "panel_results_match_check.xlsx"

OPTIONAL_MISSING_MARKERS = ("optional_missing:", "missing_optional:")
TEXT_COMPARISON_MODES = {"text", "string"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _normalize_text(value: Any) -> str:
    return str(value if value is not None else "").strip()


def _normalize_key(value: Any) -> str:
    return _normalize_text(value).lower()


def _to_number(value: Any, comparison_mode: str) -> int | float:
    text = _normalize_text(value)
    if comparison_mode == "integer":
        return int(round(float(text)))
    return float(text)


def _coerce_expected_value(value: Any, comparison_mode: str) -> Any:
    if comparison_mode in TEXT_COMPARISON_MODES:
        return _normalize_text(value)
    return _to_number(value, comparison_mode)


def _parse_source_candidate(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {
            "path": _normalize_text(value.get("path")),
            "optional_missing": bool(value.get("optional_missing")),
            "reason": _normalize_text(value.get("reason")),
        }

    text = _normalize_text(value)
    lowered = text.lower()
    for marker in OPTIONAL_MISSING_MARKERS:
        if lowered.startswith(marker):
            remainder = text.split(":", 1)[1].strip()
            path_text, separator, reason = remainder.partition(" - ")
            return {
                "path": path_text.strip(),
                "optional_missing": True,
                "reason": reason.strip() if separator else "",
            }

    return {"path": text, "optional_missing": False, "reason": ""}


def _source_candidates(values: list[Any]) -> list[dict[str, Any]]:
    return [candidate for candidate in (_parse_source_candidate(value) for value in values) if candidate["path"]]


def _match_row(rows: list[dict[str, str]], match: dict[str, Any]) -> dict[str, str] | None:
    for row in rows:
        if all(_normalize_text(row.get(key)) == _normalize_text(value) for key, value in (match or {}).items()):
            return row
    return None


def _match_rows(rows: list[dict[str, str]], match: dict[str, Any]) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if all(_normalize_text(row.get(key)) == _normalize_text(value) for key, value in (match or {}).items())
    ]


def _apply_value_map(raw_value: Any, lookup: dict[str, Any]) -> Any:
    value_map = lookup.get("value_map")
    if not isinstance(value_map, dict):
        return raw_value

    raw_text = _normalize_text(raw_value)
    if raw_text in value_map:
        return value_map[raw_text]

    lowered_map = {_normalize_key(key): mapped for key, mapped in value_map.items()}
    return lowered_map.get(_normalize_key(raw_text), raw_value)


def _aggregate_values(rows: list[dict[str, str]], value_column: str, aggregation: str) -> float:
    values = []
    for row in rows:
        raw_value = _normalize_text(row.get(value_column))
        if raw_value:
            values.append(float(raw_value))

    if not values:
        raise ValueError("no numeric values available for aggregation")

    normalized = aggregation.strip().lower()
    if normalized in {"mean", "average"}:
        return sum(values) / len(values)
    if normalized == "sum":
        return sum(values)
    if normalized == "count":
        return float(len(values))
    raise ValueError(f"unsupported aggregation: {aggregation}")


def _compare(expected_value: Any, actual_value: Any, tolerance: float, comparison_mode: str) -> tuple[bool, float]:
    if comparison_mode in TEXT_COMPARISON_MODES:
        passed = _normalize_key(actual_value) == _normalize_key(expected_value)
        return passed, 0.0 if passed else 1.0
    if comparison_mode == "integer":
        difference = abs(int(actual_value) - int(expected_value))
        return difference == 0, float(difference)
    difference = abs(float(actual_value) - float(expected_value))
    return difference <= float(tolerance), difference


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _display_value(value: Any, comparison_mode: str, units: str) -> str:
    if value in ("", None):
        return ""
    if comparison_mode in TEXT_COMPARISON_MODES:
        return str(value)
    if comparison_mode == "integer":
        return str(int(value))
    if units in {"m", "hours"}:
        return f"{float(value):.2f}"
    if units == "percentage points":
        return f"{float(value):.2f}"
    return f"{float(value):.4f}"


def _build_result(entry: dict[str, Any]) -> dict[str, Any]:
    candidate_specs = _source_candidates(list(entry.get("source_output_path_candidates") or []))
    candidates = [str(spec["path"]) for spec in candidate_specs]
    lookup = dict(entry.get("source_lookup") or {})
    match = dict(lookup.get("match") or {})
    value_column = str(lookup.get("value_column") or "").strip()
    comparison_mode = str(entry.get("comparison_mode") or "float").strip().lower()
    expected_value = _coerce_expected_value(entry.get("expected_value"), comparison_mode)
    tolerance = float(entry.get("tolerance", 0))

    result: dict[str, Any] = {
        "paper_table": entry.get("paper_table", ""),
        "paper_section": entry.get("paper_section", ""),
        "metric": entry.get("metric", ""),
        "expected_value": expected_value,
        "actual_value": "",
        "display_expected_value": _display_value(expected_value, comparison_mode, str(entry.get("units", ""))),
        "display_actual_value": "",
        "tolerance": tolerance,
        "comparison_mode": comparison_mode,
        "units": entry.get("units", ""),
        "status": "",
        "absolute_difference": "",
        "source_path": "",
        "missing_path": candidates[0] if candidates else "",
        "source_match": match,
        "source_value_column": value_column,
        "interpretation_role": entry.get("interpretation_role", ""),
        "thesis_facing": bool(entry.get("thesis_facing", False)),
        "notes": entry.get("notes", ""),
    }

    existing_candidate_specs = [
        {**spec, "resolved_path": REPO_ROOT / spec["path"]}
        for spec in candidate_specs
        if (REPO_ROOT / spec["path"]).exists()
    ]
    if not existing_candidate_specs:
        optional_notes = [
            f"{spec['path']} ({spec['reason'] or 'optional/provenance fallback'})"
            for spec in candidate_specs
            if spec["optional_missing"]
        ]
        result["status"] = "MISSING_SOURCE"
        optional_text = f" Optional fallback candidates: {'; '.join(optional_notes)}." if optional_notes else ""
        result["notes"] = f"{result['notes']} Missing source file. Tried: {', '.join(candidates)}.{optional_text}".strip()
        return result

    lookup_errors: list[str] = []
    for source_spec in existing_candidate_specs:
        source_path = source_spec["resolved_path"]
        result["source_path"] = str(source_path.relative_to(REPO_ROOT))
        rows = _read_csv_rows(source_path)

        aggregation = _normalize_text(lookup.get("aggregation"))
        if aggregation:
            matched_rows = _match_rows(rows, match)
            if not matched_rows:
                lookup_errors.append(
                    f"{source_path.relative_to(REPO_ROOT)}: no rows matched selector {json.dumps(match, sort_keys=True)}"
                )
                continue
            if not all(value_column in row for row in matched_rows):
                lookup_errors.append(f"{source_path.relative_to(REPO_ROOT)}: missing column '{value_column}'")
                continue
            try:
                actual_value = _aggregate_values(matched_rows, value_column, aggregation)
            except ValueError as exc:
                lookup_errors.append(f"{source_path.relative_to(REPO_ROOT)}: {exc}")
                continue
            passed, difference = _compare(expected_value, actual_value, tolerance, comparison_mode)
            result["actual_value"] = actual_value
            result["display_actual_value"] = _display_value(actual_value, comparison_mode, str(entry.get("units", "")))
            result["absolute_difference"] = difference
            result["status"] = "PASS" if passed else "FAIL"
            return result

        matched_row = _match_row(rows, match)
        if matched_row is None:
            lookup_errors.append(
                f"{source_path.relative_to(REPO_ROOT)}: no row matched selector {json.dumps(match, sort_keys=True)}"
            )
            continue

        if value_column not in matched_row:
            lookup_errors.append(f"{source_path.relative_to(REPO_ROOT)}: missing column '{value_column}'")
            continue

        raw_actual = _apply_value_map(matched_row.get(value_column, ""), lookup)
        if _normalize_text(raw_actual) == "":
            lookup_errors.append(f"{source_path.relative_to(REPO_ROOT)}: source value is blank")
            continue

        actual_value = (
            _normalize_text(raw_actual)
            if comparison_mode in TEXT_COMPARISON_MODES
            else _to_number(raw_actual, comparison_mode)
        )
        passed, difference = _compare(expected_value, actual_value, tolerance, comparison_mode)
        result["actual_value"] = actual_value
        result["display_actual_value"] = _display_value(actual_value, comparison_mode, str(entry.get("units", "")))
        result["absolute_difference"] = difference
        result["status"] = "PASS" if passed else "FAIL"
        return result

    result["status"] = "LOOKUP_ERROR"
    if lookup_errors:
        result["notes"] = f"{result['notes']} Tried existing sources but could not resolve the configured value: {'; '.join(lookup_errors)}.".strip()
    return result


def _write_csv(results: list[dict[str, Any]]) -> None:
    fieldnames = [
        "paper_table",
        "paper_section",
        "metric",
        "status",
        "expected_value",
        "actual_value",
        "display_expected_value",
        "display_actual_value",
        "tolerance",
        "absolute_difference",
        "comparison_mode",
        "units",
        "source_path",
        "missing_path",
        "source_value_column",
        "source_match",
        "interpretation_role",
        "thesis_facing",
        "notes",
    ]
    with CSV_OUTPUT_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            payload = dict(row)
            payload["source_match"] = json.dumps(payload.get("source_match") or {}, sort_keys=True)
            writer.writerow(payload)


def _write_json(summary: dict[str, Any], results: list[dict[str, Any]]) -> None:
    payload = {
        "generated_at_utc": _utc_now(),
        "repo_root": str(REPO_ROOT),
        "config_path": str(CONFIG_PATH.relative_to(REPO_ROOT)),
        "output_dir": str(OUTPUT_DIR.relative_to(REPO_ROOT)),
        "summary": summary,
        "results": results,
    }
    JSON_OUTPUT_PATH.write_text(json.dumps(payload, indent=2, default=_json_default), encoding="utf-8")


def _write_markdown(summary: dict[str, Any], results: list[dict[str, Any]]) -> None:
    lines = [
        "# Panel Review Check",
        "",
        "This panel mode verifies the stored thesis-facing outputs against the manuscript.",
        "It does not rerun expensive scientific simulations by default.",
        f"It reads `{CONFIG_PATH.relative_to(REPO_ROOT)}` and writes only to `{OUTPUT_DIR.relative_to(REPO_ROOT)}`.",
        "",
        "## Summary",
        "",
        f"- Total checks: {summary['total_checks']}",
        f"- PASS: {summary['pass_count']}",
        f"- FAIL: {summary['fail_count']}",
        f"- MISSING_SOURCE: {summary['missing_source_count']}",
        f"- LOOKUP_ERROR: {summary['lookup_error_count']}",
        "",
        "## Results",
        "",
        "| Status | Paper ref | Metric | Expected | Actual | Tolerance | Source |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in results:
        paper_ref = f"{row['paper_table']} / {row['paper_section']}"
        source_path = row["source_path"] or row["missing_path"] or "(not found)"
        lines.append(
            "| {status} | {paper_ref} | {metric} | {expected} | {actual} | {tolerance} | `{source}` |".format(
                status=row["status"],
                paper_ref=paper_ref.replace("|", "/"),
                metric=str(row["metric"]).replace("|", "/"),
                expected=row["display_expected_value"] or row["expected_value"],
                actual=row["display_actual_value"] or row["actual_value"] or "",
                tolerance=_display_value(row["tolerance"], "float", str(row["units"])),
                source=source_path.replace("`", "'"),
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation notes",
            "",
            "- `PASS` means the stored source value matches the expected manuscript value within the configured tolerance.",
            "- `FAIL` means the stored source value was found but does not match the configured manuscript value within tolerance.",
            "- `MISSING_SOURCE` means the configured source file was not present, so no value was fabricated.",
            "- `LOOKUP_ERROR` means the file existed but the configured row selector or value column could not be resolved.",
            "",
        ]
    )
    MARKDOWN_OUTPUT_PATH.write_text("\n".join(lines), encoding="utf-8")


def _write_manifest(summary: dict[str, Any]) -> None:
    output_files = [
        CSV_OUTPUT_PATH,
        JSON_OUTPUT_PATH,
        MARKDOWN_OUTPUT_PATH,
    ]
    payload = {
        "generated_at_utc": _utc_now(),
        "script_path": str(Path(__file__).resolve().relative_to(REPO_ROOT)),
        "config_path": str(CONFIG_PATH.relative_to(REPO_ROOT)),
        "config_sha256": _file_sha256(CONFIG_PATH),
        "output_dir": str(OUTPUT_DIR.relative_to(REPO_ROOT)),
        "outputs": [
            {
                "path": str(path.relative_to(REPO_ROOT)),
                "sha256": _file_sha256(path),
                "size_bytes": path.stat().st_size,
            }
            for path in output_files
        ],
        "summary": summary,
        "guardrails": {
            "read_only_source_check": True,
            "writes_limited_to_output_panel_review_check": True,
            "reruns_expensive_science": False,
            "modifies_canonical_scientific_outputs": False,
        },
    }
    MANIFEST_OUTPUT_PATH.write_text(json.dumps(payload, indent=2, default=_json_default), encoding="utf-8")


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OPTIONAL_WORKBOOK_OUTPUT_PATH.unlink(missing_ok=True)
    config = _read_yaml(CONFIG_PATH)
    entries = list(config.get("panel_review_expected_values") or [])
    results = [_build_result(entry) for entry in entries]

    summary = {
        "total_checks": len(results),
        "pass_count": sum(1 for row in results if row["status"] == "PASS"),
        "fail_count": sum(1 for row in results if row["status"] == "FAIL"),
        "missing_source_count": sum(1 for row in results if row["status"] == "MISSING_SOURCE"),
        "lookup_error_count": sum(1 for row in results if row["status"] == "LOOKUP_ERROR"),
    }

    _write_csv(results)
    _write_json(summary, results)
    _write_markdown(summary, results)
    _write_manifest(summary)

    print("Panel review verification complete.")
    print(f"CSV: {CSV_OUTPUT_PATH.relative_to(REPO_ROOT)}")
    print(f"JSON: {JSON_OUTPUT_PATH.relative_to(REPO_ROOT)}")
    print(f"MD: {MARKDOWN_OUTPUT_PATH.relative_to(REPO_ROOT)}")
    print(f"Manifest: {MANIFEST_OUTPUT_PATH.relative_to(REPO_ROOT)}")
    print(
        "Summary: PASS={pass_count} FAIL={fail_count} MISSING_SOURCE={missing_source_count} LOOKUP_ERROR={lookup_error_count}".format(
            **summary
        )
    )
    return 0 if summary["fail_count"] == 0 and summary["missing_source_count"] == 0 and summary["lookup_error_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
