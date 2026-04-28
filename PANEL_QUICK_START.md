# Panel / Defense Quick Start

For defense or panel inspection, start here:

```powershell
.\panel.ps1
```

or:

```powershell
.\start.ps1 -Panel
```

This opens the panel-safe review menu instead of the full research launcher.

## What Panel Mode Is For

Panel mode is the recommended first path for:

1. opening the read-only dashboard
2. verifying that stored manuscript numbers match stored scorecards
3. rebuilding publication figures from stored outputs only
4. inspecting the drifter provenance / transport context behind the March 13 -> March 14 `B1` recipe
5. refreshing the final validation package from stored outputs only
6. refreshing the reproducibility / docs package from stored outputs only
7. opening the paper-to-output registry

## What Panel Mode Does Not Do By Default

- It does not rerun expensive science.
- It does not promote appendix, archive, or personal-experiment outputs into thesis-facing results.
- It does not treat the full research launcher as the default defense path.

## Draft 20 Evidence Boundaries

- `B1` is the only main-text primary Mindoro validation row.
- The March 13 -> March 14 `B1` pair keeps the shared-imagery caveat explicit.
- `Track A` is same-case comparator support only.
- `PyGNOME` is comparator-only, never observational truth.
- `DWH` is external transfer validation, not Mindoro recalibration.
- Mindoro oil-type and shoreline outputs are support/context only.
- `prototype_2016` is legacy/archive support only.

## Inspect Drifter Provenance Behind `B1`

Use either:

```powershell
.\start.ps1 -Entry b1_drifter_context_panel
```

or panel option `7`.

This opens the panel-safe `B1 Drifter Provenance` surface. It shows the historical focused Phase 1 drifter records behind the selected B1 recipe. It does not create a new validation claim: `B1` remains public-observation validation, and if no direct March 13-14 2023 accepted drifter records are stored, the page says so explicitly.

## If You Intentionally Need The Full Launcher

Choose `Advanced` from the panel menu, or run:

```powershell
.\start.ps1
```

Use the full launcher only when you intentionally want researcher or audit reruns.
