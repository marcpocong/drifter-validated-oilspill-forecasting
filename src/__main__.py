"""
Main entry point for the Drifter-Validated Oil Spill Forecasting System.

Container routing via the PIPELINE_PHASE environment variable:
  PIPELINE_PHASE=prep            -> Pipeline-only input preparation and manifests
  PIPELINE_PHASE=1_2  (default) -> Prototype: Phase 1 validation + Phase 2 ensemble
                                    Official: frozen-baseline recipe + deterministic control + Phase 2 ensemble
  PIPELINE_PHASE=official_phase3b -> Official minimal path: deterministic control + ensemble + Phase 3B
  PIPELINE_PHASE=recipe_sensitivity -> Official event-scale Phase 3B recipe sensitivities
  PIPELINE_PHASE=public_obs_appendix -> Official appendix-only public observation expansion
  PIPELINE_PHASE=3               -> Phase 3 (oil weathering & PyGNOME comparison)
  PIPELINE_PHASE=benchmark       -> Phase 3A cross-model benchmark

docker-compose exec pipeline runs Phase 1 + 2.
docker-compose exec gnome   runs Phase 3.
"""

import os
import sys
from pathlib import Path


def print_workflow_context():
    from src.core.case_context import get_case_log_lines

    print("Workflow Context:")
    for line in get_case_log_lines():
        print(f"  {line}")


def _prep_command_hint() -> str:
    workflow_mode = os.environ.get("WORKFLOW_MODE", "prototype_2016")
    return (
        "docker-compose -f docker-compose.yml exec -T "
        f"-e WORKFLOW_MODE={workflow_mode} -e PIPELINE_PHASE=prep pipeline python -m src"
    )


def ensure_prepared_inputs(
    run_name: str,
    recipe_name: str | None = None,
    require_drifter: bool = False,
    include_all_transport_forcing: bool = False,
    phase_label: str = "requested phase",
):
    from src.utils.io import find_missing_prepared_inputs

    missing_specs = find_missing_prepared_inputs(
        recipe_name=recipe_name,
        require_drifter=require_drifter,
        include_all_transport_forcing=include_all_transport_forcing,
        run_name=run_name,
    )
    if not missing_specs:
        return

    print(f"Missing prepared inputs for {phase_label}.")
    print("This phase is read-only and will not download or preprocess data.")
    print("Missing files:")
    for spec in missing_specs:
        print(f"  - {spec['label']}: {spec['path']}")
        print(f"    source: {spec['source']}")
    print("Run the pipeline-only prep stage first:")
    print(f"  {_prep_command_hint()}")
    sys.exit(1)


def ensure_phase3b_forecast_outputs(
    run_name: str,
    recipe_name: str,
    phase_label: str = "Phase 3B",
):
    from src.core.case_context import get_case_context
    from src.utils.io import find_missing_phase3b_forecast_outputs

    case = get_case_context()
    missing_specs = find_missing_phase3b_forecast_outputs(recipe_name=recipe_name, run_name=run_name)
    if not missing_specs:
        return

    print(f"Missing spill forecast outputs for {phase_label}.")
    print("Phase 3B requires deterministic/ensemble spill outputs but does not depend on benchmark or weathering.")
    print("Missing files:")
    for spec in missing_specs:
        print(f"  - {spec['label']}: {spec['path']}")
        print(f"    source: {spec['source']}")
    if case.is_official:
        print("Run the official minimal forecast path after prep:")
        print(
            "  docker-compose -f docker-compose.yml exec -T "
            f"-e WORKFLOW_MODE={os.environ.get('WORKFLOW_MODE', 'mindoro_retro_2023')} "
            "-e PIPELINE_PHASE=official_phase3b pipeline python -m src"
        )
    else:
        print("Run Phase 1 + 2 first to generate deterministic/ensemble forecast outputs.")
    sys.exit(1)


def ensure_data_exists(run_name: str, require_drifter: bool = True):
    from src.core.case_context import get_case_context
    from src.services.ingestion import DataIngestionService
    from src.utils.io import resolve_initialization_polygon_path, resolve_validation_polygon_path

    drifter_path = Path(f"data/drifters/{run_name}/drifters_noaa.csv")
    case = get_case_context()
    init_polygon_path = resolve_initialization_polygon_path(run_name)
    validation_polygon_path = resolve_validation_polygon_path(run_name)

    missing = (
        not init_polygon_path.exists()
        or not validation_polygon_path.exists()
        or (require_drifter and not drifter_path.exists())
    )
    if missing:
        print("Required data not found. Initiating ingestion service...")
        DataIngestionService().run()

        still_missing = (
            not init_polygon_path.exists()
            or not validation_polygon_path.exists()
            or (require_drifter and not drifter_path.exists())
        )
        if still_missing:
            print("Ingestion failed to produce required data. Exiting.")
            sys.exit(1)


def run_prep():
    from src.core.case_context import get_case_context
    from src.core.constants import RUN_NAME
    from src.services.ingestion import DataIngestionService

    pipeline_role = os.environ.get("PIPELINE_ROLE", "").strip().lower()
    if pipeline_role and pipeline_role != "pipeline":
        print("The prep stage is only available in the pipeline container.")
        print(f"Run it with: {_prep_command_hint()}")
        sys.exit(1)

    case = get_case_context()

    print("Starting pipeline-only input preparation...")
    print_workflow_context()

    result = DataIngestionService().run() or {}
    ensure_prepared_inputs(
        RUN_NAME,
        require_drifter=case.drifter_required,
        include_all_transport_forcing=True,
        phase_label="pipeline prep stage",
    )

    print("\nPreparation complete.")
    if result.get("download_manifest"):
        print(f"Download manifest: {result['download_manifest']}")
    if result.get("prepared_input_manifest"):
        print(f"Prepared-input manifest: {result['prepared_input_manifest']}")


def print_phase2_outputs(results):
    from src.core.case_context import get_case_context
    from src.core.constants import BASE_OUTPUT_DIR

    case = get_case_context()
    print("\nEnsemble generation complete.")
    print(f"Outputs saved to: {results['output']}")
    if case.is_official:
        print("Official Phase 2 forecast products:")
        print(f"  - {BASE_OUTPUT_DIR}/forecast/forecast_manifest.json")
        print(f"  - {BASE_OUTPUT_DIR}/forecast/control_footprint_mask_2023-03-06T09-59-00Z.tif")
        print(f"  - {BASE_OUTPUT_DIR}/forecast/control_density_norm_2023-03-06T09-59-00Z.tif")
        print(f"  - {BASE_OUTPUT_DIR}/forecast/phase2_loading_audit.json")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/ensemble_manifest.json")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/prob_presence_2023-03-06T09-59-00Z.tif")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/mask_p50_2023-03-06T09-59-00Z.tif")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/mask_p90_2023-03-06T09-59-00Z.tif")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/mask_p50_2023-03-06_datecomposite.tif")
    else:
        print("Phase 2 probability outputs:")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_24h.png")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_48h.png")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_72h.png")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_24h.nc")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_48h.nc")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_72h.nc")
    if results.get("manifest"):
        print(f"  - {results['manifest']} (ensemble manifest)")
    if results.get("written_files") and not case.is_official:
        alias_path = next((p for p in results["written_files"] if p.endswith("probability_cone.png")), None)
        if alias_path:
            print(f"  - {alias_path} (legacy alias)")


def print_recipe_selection(selection, label: str = "Recipe selection"):
    print(f"{label}: {selection.recipe}")
    print(f"Selection source: {selection.source_kind}")
    print(
        f"Selection status: {selection.status_flag} "
        f"(valid={selection.valid}, provisional={selection.provisional}, rerun_required={selection.rerun_required})"
    )
    if selection.source_path:
        print(f"Selection artifact: {selection.source_path}")
    if selection.note:
        print(f"Selection note: {selection.note}")


def run_phase1_and_2():
    from src.core.case_context import get_case_context
    from src.core.constants import RUN_NAME
    from src.services.ensemble import run_ensemble, run_official_spill_forecast
    from src.utils.io import resolve_best_recipe, resolve_recipe_selection, resolve_spill_origin

    case = get_case_context()

    if case.is_official:
        print("Starting official spill-case workflow...")
        print_workflow_context()

        print("Historical Phase 1 drifter validation is disabled in official mode.")
        selection = resolve_recipe_selection()
        best_recipe = selection.recipe
        ensure_prepared_inputs(
            RUN_NAME,
            recipe_name=best_recipe,
            require_drifter=case.drifter_required,
            phase_label="official spill-case forecast",
        )
        print_recipe_selection(selection, label="Baseline recipe")

        start_lat, start_lon, start_time_str = resolve_spill_origin()
        print(f"Origin: {start_lat:.4f}, {start_lon:.4f} at {start_time_str}")

        results = run_official_spill_forecast(
            selection=selection,
            start_time=start_time_str,
            start_lat=start_lat,
            start_lon=start_lon,
        )
        print_phase2_outputs(results)
        if results.get("deterministic_control"):
            print(f"Deterministic control: {results['deterministic_control']}")
        if results.get("forecast_manifest"):
            print(f"Forecast manifest: {results['forecast_manifest']}")
        print("\nOfficial spill-case pipeline completed successfully.")
        return

    from src.services.validation import TransportValidationService
    from src.utils.io import load_drifter_data

    print("Starting Phase 1: Transport Validation Pipeline...")
    print_workflow_context()

    ensure_data_exists(RUN_NAME, require_drifter=True)
    drifter_path = Path(f"data/drifters/{RUN_NAME}/drifters_noaa.csv")

    print(f"Loading drifter data from {drifter_path}...")
    drifter_df = load_drifter_data(drifter_path)

    service = TransportValidationService()
    print("Running validation service across all recipes...")
    rankings = service.run_validation(drifter_df)
    print("\nFinal rankings (lower NCS is better):")
    print(rankings)

    print("\nInitiating Phase 2: Ensemble Uncertainty Quantification...")

    best_recipe = rankings.iloc[0]["recipe"]
    print(f"Winning recipe: {best_recipe} (NCS: {rankings.iloc[0]['ncs_score']:.4f})")

    start_lat, start_lon, start_time_str = resolve_spill_origin()
    print(f"Origin: {start_lat:.4f}, {start_lon:.4f} at {start_time_str}")

    results = run_ensemble(
        best_recipe=best_recipe,
        start_time=start_time_str,
        start_lat=start_lat,
        start_lon=start_lon,
    )
    print_phase2_outputs(results)
    print("\nPhase 1 + 2 pipeline completed successfully.")


def run_official_phase3b_minimal():
    from src.core.case_context import get_case_context

    case = get_case_context()
    if not case.is_official:
        print("official_phase3b is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting minimal official spill-case path: deterministic control + ensemble + Phase 3B...")
    run_phase1_and_2()
    run_phase3b()


def run_phase3():
    import yaml

    from src.core.constants import BASE_OUTPUT_DIR, RUN_NAME
    from src.helpers.plotting import plot_diagnostic_forcing
    from src.services.diagnostics import run_diagnostics
    from src.services.gnome_comparison import run_gnome_comparison
    from src.services.shoreline import run_shoreline_analysis
    from src.services.weathering import run_refined_weathering, run_weathering
    from src.utils.io import resolve_recipe_selection, resolve_spill_origin

    print("Starting Phase 3: Oil Weathering and Fate Analysis...")
    print_workflow_context()

    selection = resolve_recipe_selection()
    best_recipe = selection.recipe
    ensure_prepared_inputs(
        RUN_NAME,
        recipe_name=best_recipe,
        phase_label="Phase 3",
    )
    print_recipe_selection(selection, label="Transport recipe")
    start_lat, start_lon, start_time_str = resolve_spill_origin()
    print(f"Best recipe  : {best_recipe}")
    print(f"Spill origin : {start_lat:.4f}, {start_lon:.4f} at {start_time_str}")

    print("\nRunning pre-flight diagnostics...")
    diag_report = run_diagnostics(
        best_recipe=best_recipe,
        start_time=start_time_str,
        start_lat=start_lat,
        start_lon=start_lon,
    )
    try:
        plot_diagnostic_forcing(diag_report, str(BASE_OUTPUT_DIR / "diagnostics/forcing_summary.png"))
        print(f"Diagnostic chart -> {BASE_OUTPUT_DIR}/diagnostics/forcing_summary.png")
    except Exception as e:
        print(f"Diagnostic chart generation failed: {e}")

    weathering_results = run_weathering(
        best_recipe=best_recipe,
        start_time=start_time_str,
        start_lat=start_lat,
        start_lon=start_lon,
    )

    print("\nPhase 3 - Mass Budget Summary (at 72 h):")
    for oil_key, res in weathering_results.items():
        df = res["budget_df"]
        last = df.iloc[-1]
        print(f"  [{res['display_name']}]")
        print(f"    Surface:    {last['surface_pct']:.1f}%")
        print(f"    Evaporated: {last['evaporated_pct']:.1f}%")
        print(f"    Dispersed:  {last['dispersed_pct']:.1f}%")
        print(f"    Beached:    {last['beached_pct']:.1f}%")

    print("\nMass-Balance QC Check (tolerance <= 2 %):")
    for oil_key, res in weathering_results.items():
        qc = res.get("qc", {})
        status = "PASS" if qc.get("passed", True) else "FAIL"
        print(
            f"  [{res['display_name']}] {status} "
            f"(max deviation {qc.get('max_deviation_pct', 0):.2f} % "
            f"at hour {qc.get('worst_hour', '?')})"
        )
        if qc.get("failing_hours"):
            for hour, dev in qc["failing_hours"][:5]:
                print(f"     hour {hour}: deviation {dev:.2f} %")

    with open("config/oil.yaml", "r") as oil_file:
        oil_cfg = yaml.safe_load(oil_file)
    shore_cfg = oil_cfg.get("shoreline", {})
    if shore_cfg.get("enabled", False):
        print("\nShoreline Impact Analysis:")
        sim_cfg = oil_cfg.get("simulation", {})
        for oil_key, res in weathering_results.items():
            seg_df = run_shoreline_analysis(
                nc_path=res["nc_path"],
                initial_mass_tonnes=sim_cfg.get("initial_mass_tonnes", 50.0),
                segment_length_km=shore_cfg.get("segment_length_km", 1.0),
                segment_prefix=shore_cfg.get("segment_prefix", "PWN"),
            )
            if seg_df is not None and not seg_df.empty:
                csv_path = BASE_OUTPUT_DIR / "weathering" / f"shoreline_{oil_key}.csv"
                seg_df.to_csv(csv_path, index=False)
                print(f"  [{res['display_name']}] {len(seg_df)} segment(s) impacted -> {csv_path.name}")
                print(seg_df.to_string(index=False, max_rows=10))
            else:
                print(f"  [{res['display_name']}] No particles beached.")

    refined_result = run_refined_weathering(
        best_recipe=best_recipe,
        start_time=start_time_str,
        start_lat=start_lat,
        start_lon=start_lon,
    )
    if refined_result:
        r_df = refined_result["budget_df"]
        r_last = r_df.iloc[-1]
        r_qc = refined_result.get("qc", {})
        r_status = "PASS" if r_qc.get("passed", True) else "FAIL"
        print("\nStage 3b - Refined Oil Budget (at 72 h):")
        print(f"  [{refined_result['display_name']}]")
        print(f"    Surface:    {r_last['surface_pct']:.1f}%")
        print(f"    Evaporated: {r_last['evaporated_pct']:.1f}%")
        print(f"    Dispersed:  {r_last['dispersed_pct']:.1f}%")
        print(f"    Beached:    {r_last['beached_pct']:.1f}%")
        print(f"  QC: {r_status}  (max deviation {r_qc.get('max_deviation_pct', 0):.2f} %)")

    print("\nRunning supplementary PyGNOME cross-comparison...")
    gnome_results = run_gnome_comparison(
        start_lat=start_lat,
        start_lon=start_lon,
        start_time=start_time_str,
        openoil_results=weathering_results,
    )
    if gnome_results:
        print(f"PyGNOME comparison complete. Charts saved to {BASE_OUTPUT_DIR}/gnome_comparison/")
    else:
        print("PyGNOME not available - skipping cross-comparison.")

    print(f"\nPhase 3 complete. Check {BASE_OUTPUT_DIR}/weathering/ for mass budget charts.")


def run_benchmark():
    from src.core.case_context import get_case_context
    from src.core.constants import RUN_NAME
    from src.services.benchmark import BenchmarkPipeline
    from src.utils.io import resolve_recipe_selection, resolve_spill_origin

    case = get_case_context()

    print("Starting Benchmark Pipeline...")
    print_workflow_context()

    selection = resolve_recipe_selection()
    best_recipe = selection.recipe
    ensure_prepared_inputs(
        RUN_NAME,
        recipe_name=best_recipe,
        require_drifter=case.drifter_required,
        phase_label="benchmark phase",
    )
    print_recipe_selection(selection, label="Transport recipe")
    start_lat, start_lon, start_time_str = resolve_spill_origin()
    print(f"Best recipe  : {best_recipe}")
    print(f"Spill origin : {start_lat:.4f}, {start_lon:.4f} at {start_time_str}")

    BenchmarkPipeline().run(
        best_recipe=best_recipe,
        start_lat=start_lat,
        start_lon=start_lon,
        start_time=start_time_str,
    )


def run_phase3b():
    from src.core.constants import BASE_OUTPUT_DIR, RUN_NAME
    from src.core.case_context import get_case_context
    from src.services.scoring import run_phase3b_scoring
    from src.utils.io import resolve_recipe_selection

    print("Starting Phase 3B: Observational Validation vs Satellite Imagery...")
    print_workflow_context()
    case = get_case_context()

    selection = resolve_recipe_selection()
    best_recipe = selection.recipe
    ensure_prepared_inputs(
        RUN_NAME,
        recipe_name=best_recipe,
        phase_label="Phase 3B",
    )
    ensure_phase3b_forecast_outputs(
        RUN_NAME,
        recipe_name=best_recipe,
        phase_label="Phase 3B",
    )
    print_recipe_selection(selection, label="Transport recipe")

    output_dir = BASE_OUTPUT_DIR / ("phase3b" if case.is_official else "validation")
    output_dir.mkdir(parents=True, exist_ok=True)

    results = run_phase3b_scoring(output_dir=output_dir)
    print("\nPhase 3B complete.")
    print(f"FSS metrics saved to: {results.fss_by_date_window}")
    print(f"Summary saved to: {results.summary}")
    if getattr(results, "diagnostics", None):
        print(f"Diagnostics saved to: {results.diagnostics}")
    if getattr(results, "run_manifest", None):
        print(f"Run manifest saved to: {results.run_manifest}")


def run_recipe_sensitivity_phase():
    from src.core.case_context import get_case_context
    from src.core.constants import RUN_NAME
    from src.services.recipe_sensitivity import run_recipe_sensitivity
    from src.utils.io import get_recipe_sensitivity_output_dir, resolve_recipe_selection

    case = get_case_context()
    if not case.is_official:
        print("recipe_sensitivity is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting official Mindoro event-scale Phase 3B recipe sensitivities...")
    print_workflow_context()

    selection = resolve_recipe_selection()
    ensure_prepared_inputs(
        RUN_NAME,
        recipe_name=selection.recipe,
        require_drifter=case.drifter_required,
        phase_label="recipe sensitivity phase",
    )
    print_recipe_selection(selection, label="Frozen historical baseline")

    results = run_recipe_sensitivity()
    output_dir = get_recipe_sensitivity_output_dir()
    print("\nRecipe sensitivity runs complete.")
    print(f"Outputs saved to: {output_dir}")
    print(f"Summary saved to: {results['artifacts']['summary_csv']}")
    print(f"By-window metrics saved to: {results['artifacts']['by_window_csv']}")
    print(f"Diagnostics saved to: {results['artifacts']['diagnostics_csv']}")
    print(f"Report saved to: {results['artifacts']['report_md']}")
    if results["artifacts"].get("overlay_png"):
        print(f"QA overlay saved to: {results['artifacts']['overlay_png']}")


def run_public_obs_appendix_phase():
    from src.core.case_context import get_case_context
    from src.core.constants import RUN_NAME
    from src.services.public_obs_appendix import run_public_obs_appendix
    from src.utils.io import resolve_recipe_selection

    case = get_case_context()
    if not case.is_official:
        print("public_obs_appendix is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting appendix-only public observation expansion...")
    print_workflow_context()

    selection = resolve_recipe_selection()
    ensure_prepared_inputs(
        RUN_NAME,
        recipe_name=selection.recipe,
        require_drifter=case.drifter_required,
        phase_label="public observation appendix",
    )
    print_recipe_selection(selection, label="Frozen historical baseline")

    results = run_public_obs_appendix()
    print("\nPublic observation appendix complete.")
    print(f"Inventory CSV: {results['inventory_csv']}")
    print(f"Inventory JSON: {results['inventory_json']}")
    print(f"Accepted quantitative appendix dates: {', '.join(results['accepted_quantitative_dates'])}")
    print(f"Recommendation: {results['recommendation']}")


def main():
    import subprocess

    from src.core.case_context import get_case_context

    is_spawned = os.environ.get("RUN_SPAWNED")
    case = get_case_context()

    if case.orchestration_dates and not is_spawned:
        print(
            f"Prototype workflow detected. Executing orchestrator for "
            f"{len(case.orchestration_dates)} cases..."
        )
        for date in case.orchestration_dates:
            print(f"\n{'=' * 60}")
            print(f"Spawning pipeline for CASE {date}")
            print(f"{'=' * 60}")
            env = os.environ.copy()
            env["PHASE_1_START_DATE"] = date
            env["RUN_SPAWNED"] = "1"
            result = subprocess.run([sys.executable, "-m", "src"], env=env)
            if result.returncode != 0:
                print(f"Pipeline failed for case {date}. Continuing to next case...")
        return

    phase = os.environ.get("PIPELINE_PHASE", "1_2")
    if phase == "prep":
        run_prep()
    elif phase == "official_phase3b":
        run_official_phase3b_minimal()
    elif phase == "benchmark":
        run_benchmark()
    elif phase == "recipe_sensitivity":
        run_recipe_sensitivity_phase()
    elif phase == "public_obs_appendix":
        run_public_obs_appendix_phase()
    elif phase == "3":
        run_phase3()
    elif phase == "3b":
        run_phase3b()
    else:
        run_phase1_and_2()


if __name__ == "__main__":
    main()
