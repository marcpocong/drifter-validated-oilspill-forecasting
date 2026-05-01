# Reproducibility Bundle Guide

This guide describes the final review/reproducibility package. It is a governance and traceability layer over stored outputs, not a claim of universal operational accuracy.

## Required Launcher Commands

```powershell
.\start.ps1 -ValidateMatrix -NoPause
.\start.ps1 -List -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause
.\start.ps1 -Explain dwh_reportable_bundle -NoPause
```

## Panel-Safe Commands

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1 -Entry phase1_audit -NoPause
.\start.ps1 -Entry phase2_audit -NoPause
.\start.ps1 -Entry b1_drifter_context_panel -NoPause
.\start.ps1 -Entry final_validation_package -NoPause
.\start.ps1 -Entry phase5_sync -NoPause
.\start.ps1 -Entry figure_package_publication -NoPause
```

These commands are read-only or packaging-only. They are the preferred defense and review surface because they use stored outputs.

## Main Evidence Rerun Commands

Use only when an intentional scientific rerun is desired:

```powershell
.\start.ps1 -Entry phase1_mindoro_focus_provenance -DryRun -NoPause
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation -DryRun -NoPause
.\start.ps1 -Entry dwh_reportable_bundle -DryRun -NoPause
.\start.ps1 -Entry mindoro_reportable_core -DryRun -NoPause
```

Remove `-DryRun` only after confirming runtime cost, Docker/model availability, and credential requirements.

## Support And Context Commands

```powershell
.\start.ps1 -Entry mindoro_phase4_only -DryRun -NoPause
.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle -DryRun -NoPause
.\start.ps1 -Entry prototype_legacy_final_figures -NoPause
```

Mindoro oil-type and shoreline outputs are support/context only, not primary validation. PyGNOME branches remain comparator-only.

## Archive And Provenance Commands

```powershell
.\start.ps1 -ListRole archive_provenance -NoPause
.\start.ps1 -Entry phase1_regional_reference_rerun -DryRun -NoPause
.\start.ps1 -Entry mindoro_march6_recovery_sensitivity -DryRun -NoPause
.\start.ps1 -Entry mindoro_march23_extended_public_stress_test -DryRun -NoPause
```

Archive, experimental, legacy, and repository-development files are preserved for audit. They are not promoted into the main paper claim.

## Provenance Files

- Data-source provenance: `docs/DATA_SOURCES.md` and `config/data_sources.yaml`
- Paper-to-output crosswalk: `docs/PAPER_TO_REPO_CROSSWALK.md` and `config/paper_to_output_registry.yaml`
- Archive governance: `docs/ARCHIVE_GOVERNANCE.md` and `config/archive_registry.yaml`
- Launcher matrix: `config/launcher_matrix.json` and `docs/COMMAND_MATRIX.md`
- Final paper alignment: `docs/FINAL_PAPER_ALIGNMENT.md`
- Development artifact archive: `archive/development_artifacts/README.md`

## Validation Commands

```powershell
python -m pytest tests
python scripts\validate_final_paper_guardrails.py
python scripts\validate_archive_registry.py
python scripts\validate_paper_to_output_registry.py
python scripts\validate_data_sources_registry.py
.\start.ps1 -ValidateMatrix -NoPause
.\start.ps1 -List -NoPause
```

The full test suite may require the scientific runtime stack, including packages such as OpenDrift, Cartopy, ERDDAP tooling, GeoPandas, Rasterio, NumPy, Pandas, and Xarray. The cheap guardrail tests and validators do not require Docker or external credentials.

## Known Limitations And Credentials

- Copernicus Marine and CDS/ERA5 access can require user credentials.
- Docker and the model runtime are required for intentional scientific reruns.
- PyGNOME is comparator-only and never observation truth.
- DWH is external transfer validation only, not Mindoro recalibration.
- Mindoro B1 supports coastal-neighborhood usefulness, not exact 1 km overlap and not universal operational accuracy.
- `mask_p90` is a conservative support/comparison product only.

## Submission Manifest

The machine-readable submission manifest is:

```text
output/final_reproducibility_package/repo_submission_manifest.json
```

It records the source commit at manifest generation time, key config/docs/output paths, validation commands, validation status, small-file checksums, and the no-secrets policy.
