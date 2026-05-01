# Panel Quick Start

This is the short path for panel review. It uses stored outputs and packaging-safe launcher entries by default.

## 1. Open Panel Mode

```powershell
.\panel.ps1
```

or:

```powershell
.\start.ps1 -Panel
```

On macOS or Linux with PowerShell 7:

```bash
pwsh ./panel.ps1
pwsh ./start.ps1 -Panel
```

## 2. Use The Panel Menu

Panel mode is read-only and packaging-safe. It can open the dashboard, verify stored paper numbers, rebuild publication figures from stored outputs, refresh review packages, and show provenance registries.

Panel-safe options:

1. Open read-only dashboard
2. Verify paper numbers against stored scorecards
3. Rebuild publication figures from stored outputs
4. Refresh final validation package from stored outputs
5. Refresh final reproducibility package / command documentation
6. Show paper-to-output registry
7. View B1 drifter provenance/context
8. View data sources and provenance registry

Suggested review path:

1. Open the read-only dashboard.
2. Verify manuscript numbers against stored scorecards.
3. Inspect [docs/FINAL_PAPER_ALIGNMENT.md](docs/FINAL_PAPER_ALIGNMENT.md).
4. Inspect [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md).
5. Inspect [docs/ARCHIVE_GOVERNANCE.md](docs/ARCHIVE_GOVERNANCE.md).

## 3. Keep The Evidence Boundaries

- Mindoro B1 supports coastal-neighborhood usefulness, not exact 1 km overlap.
- Mindoro Track A and PyGNOME are comparator-only.
- DWH is external transfer validation only.
- Mindoro oil-type and shoreline outputs are support/context only.
- Secondary 2016 outputs are legacy support only.

## 4. When A Rerun Is Needed

Expensive science does not run from the default panel path. Choose the full launcher only when an explicit research or audit rerun is intended:

```powershell
.\start.ps1
.\start.ps1 -List -NoPause
.\start.ps1 -ListRole primary_evidence -NoPause
.\start.ps1 -ListRole archive_provenance -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation -DryRun -NoPause
```

Use dry runs and exported plans before running any full scientific entry. Explain and dry-run previews show the requested ID, canonical ID, claim boundary, run kind, rerun cost, `safe_default`, and role flags before any run confirmation.
