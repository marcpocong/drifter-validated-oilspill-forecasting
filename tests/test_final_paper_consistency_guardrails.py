from __future__ import annotations

from scripts import validate_final_paper_guardrails as guardrails


def _format_issues(issues: list[str]) -> str:
    return "\n".join(issues)


def test_no_forbidden_manuscript_label():
    violations = guardrails.find_forbidden_manuscript_label()

    assert not violations, (
        f"{guardrails.REDACTED_MANUSCRIPT_LABEL} found in tracked text-like files:\n"
        + _format_issues(violations)
    )


def test_launcher_matrix_schema():
    issues = guardrails.launcher_matrix_schema_issues()

    assert not issues, _format_issues(issues)


def test_launcher_roles():
    issues = guardrails.launcher_role_issues()

    assert not issues, _format_issues(issues)


def test_claim_boundaries():
    issues = guardrails.claim_boundary_issues()

    assert not issues, _format_issues(issues)


def test_probability_semantics():
    issues = guardrails.probability_semantics_issues()

    assert not issues, _format_issues(issues)


def test_archive_registry():
    issues = guardrails.archive_registry_issues()

    assert not issues, _format_issues(issues)


def test_paper_to_output_registry():
    issues = guardrails.paper_to_output_registry_issues()

    assert not issues, _format_issues(issues)


def test_data_sources_registry():
    issues = guardrails.data_sources_registry_issues()

    assert not issues, _format_issues(issues)
