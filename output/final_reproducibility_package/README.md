# Final Reproducibility Package

This directory is the review and reproducibility bundle for the final paper-aligned repository state. It indexes stored outputs, launcher routes, validation commands, and governance files without rerunning expensive science by default.

Use this package with:

- `docs/REPRODUCIBILITY_BUNDLE_GUIDE.md`
- `docs/FINAL_PAPER_ALIGNMENT.md`
- `docs/PAPER_TO_REPO_CROSSWALK.md`
- `config/paper_to_output_registry.yaml`
- `config/archive_registry.yaml`
- `config/data_sources.yaml`
- `output/final_reproducibility_package/repo_submission_manifest.json`

The bundle is not a claim of universal operational accuracy. It is a reproducibility and review layer over stored Mindoro, DWH, oil-type/shoreline, secondary 2016, archive, and governance artifacts.

Panel-safe entry points:

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1 -ValidateMatrix -NoPause
.\start.ps1 -List -NoPause
```

Scientific reruns are intentionally routed through explicit launcher entries and may require Docker, external data access, model dependencies, and credentials.
