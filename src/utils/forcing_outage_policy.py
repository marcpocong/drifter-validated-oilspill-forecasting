from __future__ import annotations

import os
import socket
import ssl
from typing import Any

import requests


FORCING_OUTAGE_POLICY_ENV = "FORCING_OUTAGE_POLICY"
FORCING_OUTAGE_POLICY_DEFAULT = "default"
FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED = "continue_degraded"
FORCING_OUTAGE_POLICY_FAIL_HARD = "fail_hard"

FORCING_OUTAGE_SKIP_EXIT_CODE = 87
FORCING_OUTAGE_SKIP_PAYLOAD_PREFIX = "FORCING_OUTAGE_SKIP_PAYLOAD="

REMOTE_OUTAGE_MESSAGE_TOKENS = (
    "502",
    "503",
    "504",
    "service unavailable",
    "bad gateway",
    "gateway timeout",
    "temporarily unavailable",
    "timed out",
    "timeout",
    "name or service not known",
    "temporary failure in name resolution",
    "failed to establish a new connection",
    "max retries exceeded",
    "connection aborted",
    "connection reset",
    "remote disconnected",
    "dns",
    "ssl",
    "tls",
    "proxyerror",
    "error reading from remote server",
    "dap server error",
)

DEFAULT_CONTINUE_DEGRADED_PHASES = {
    "phase3b_extended_public_scored",
    "phase3b_extended_public_scored_march23",
    "source_history_reconstruction_r1",
    "mindoro_march13_14_phase1_focus_trial",
}

DEFAULT_CONTINUE_DEGRADED_WORKFLOWS = {
    "prototype_2016",
    "prototype_2021",
    "phase1_mindoro_focus_pre_spill_2016_2023",
}


def normalize_forcing_outage_policy(value: str | None) -> str:
    normalized = str(value or FORCING_OUTAGE_POLICY_DEFAULT).strip().lower()
    if normalized in {
        FORCING_OUTAGE_POLICY_DEFAULT,
        FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
        FORCING_OUTAGE_POLICY_FAIL_HARD,
    }:
        return normalized
    raise ValueError(
        "FORCING_OUTAGE_POLICY must be one of: "
        f"{FORCING_OUTAGE_POLICY_DEFAULT}, "
        f"{FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED}, "
        f"{FORCING_OUTAGE_POLICY_FAIL_HARD}."
    )


def resolve_forcing_outage_policy(
    *,
    workflow_mode: str | None = None,
    phase: str | None = None,
) -> str:
    env_value = normalize_forcing_outage_policy(os.environ.get(FORCING_OUTAGE_POLICY_ENV))
    if env_value != FORCING_OUTAGE_POLICY_DEFAULT:
        return env_value

    if str(phase or "").strip() in DEFAULT_CONTINUE_DEGRADED_PHASES:
        return FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED

    if str(workflow_mode or "").strip() in DEFAULT_CONTINUE_DEGRADED_WORKFLOWS:
        return FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED

    return FORCING_OUTAGE_POLICY_FAIL_HARD


def forcing_outage_policy_allows_continue(
    *,
    workflow_mode: str | None = None,
    phase: str | None = None,
) -> bool:
    return resolve_forcing_outage_policy(workflow_mode=workflow_mode, phase=phase) == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED


def is_remote_outage_error(exc_or_message: Exception | str | Any) -> bool:
    if isinstance(
        exc_or_message,
        (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.SSLError,
            socket.timeout,
            socket.gaierror,
            TimeoutError,
            ssl.SSLError,
        ),
    ):
        return True

    if isinstance(exc_or_message, requests.exceptions.HTTPError):
        response = getattr(exc_or_message, "response", None)
        if response is not None and response.status_code >= 500:
            return True

    message = str(exc_or_message).lower()
    return any(token in message for token in REMOTE_OUTAGE_MESSAGE_TOKENS)


def forcing_factor_id_for_source(source_id: str) -> str:
    mapping = {
        "hycom": "hycom_curr.nc",
        "cmems": "cmems_curr.nc",
        "cmems_wave": "cmems_wave.nc",
        "era5": "era5_wind.nc",
        "ncep": "ncep_wind.nc",
        "gfs": "gfs_wind.nc",
    }
    try:
        return mapping[str(source_id)]
    except KeyError as exc:
        raise KeyError(f"Unsupported forcing source id: {source_id}") from exc


def forcing_kind_for_source(source_id: str) -> str:
    factor_id = forcing_factor_id_for_source(source_id)
    if factor_id.endswith("_curr.nc"):
        return "current"
    if factor_id.endswith("_wave.nc"):
        return "wave"
    return "wind"


def source_id_for_recipe_component(*, forcing_kind: str, filename: str) -> str:
    lower = str(filename or "").strip().lower()
    if forcing_kind == "current":
        if lower.startswith("cmems"):
            return "cmems"
        if lower.startswith("hycom"):
            return "hycom"
    elif forcing_kind == "wave":
        if lower.startswith("cmems"):
            return "cmems_wave"
    elif forcing_kind == "wind":
        if lower.startswith("era5"):
            return "era5"
        if lower.startswith("ncep"):
            return "ncep"
        if lower.startswith("gfs"):
            return "gfs"
    raise ValueError(f"Unsupported {forcing_kind} forcing file: {filename}")
