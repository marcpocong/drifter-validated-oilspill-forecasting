from __future__ import annotations

import os
import shutil
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
START_PS1 = REPO_ROOT / "start.ps1"


@dataclass(frozen=True)
class LauncherResult:
    args: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    docker_log: Path

    @property
    def output(self) -> str:
        return f"{self.stdout}\n{self.stderr}"


def require_pwsh() -> str:
    pwsh = shutil.which("pwsh")
    if not pwsh:
        pytest.skip("PowerShell launcher tests require pwsh; skipping because pwsh is not installed.")
    return pwsh


def _write_fake_docker_commands(fake_bin: Path, docker_log: Path) -> None:
    fake_bin.mkdir(parents=True, exist_ok=True)

    if os.name == "nt":
        script = (
            "@echo off\r\n"
            "echo %~nx0 %*>> \"%DOCKER_INVOCATION_LOG%\"\r\n"
            "exit /b 99\r\n"
        )
        for name in ("docker.cmd", "docker-compose.cmd"):
            (fake_bin / name).write_text(script, encoding="utf-8")
        return

    script = (
        "#!/usr/bin/env sh\n"
        "printf '%s %s\\n' \"$(basename \"$0\")\" \"$*\" >> \"$DOCKER_INVOCATION_LOG\"\n"
        "exit 99\n"
    )
    for name in ("docker", "docker-compose"):
        path = fake_bin / name
        path.write_text(script, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def run_launcher(
    args: list[str],
    tmp_path: Path,
    stdin: str = "",
    timeout: int = 30,
    extra_env: dict[str, str] | None = None,
) -> LauncherResult:
    pwsh = require_pwsh()
    fake_bin = tmp_path / "fake-bin"
    docker_log = tmp_path / "docker_invocations.log"
    _write_fake_docker_commands(fake_bin=fake_bin, docker_log=docker_log)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}{os.pathsep}{env.get('PATH', '')}"
    env["DOCKER_INVOCATION_LOG"] = str(docker_log)
    env["PYTHONIOENCODING"] = "utf-8"
    if extra_env:
        env.update(extra_env)

    completed = subprocess.run(
        [pwsh, "-NoProfile", "-File", str(START_PS1), *args],
        cwd=REPO_ROOT,
        input=stdin,
        text=True,
        capture_output=True,
        env=env,
        timeout=timeout,
    )
    return LauncherResult(
        args=tuple(args),
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        docker_log=docker_log,
    )


def assert_clean_launcher_exit(result: LauncherResult, expected_returncode: int = 0) -> None:
    assert result.returncode == expected_returncode, result.output
    assert "[ERROR]" not in result.output


def assert_no_docker_execution(result: LauncherResult) -> None:
    docker_output = result.docker_log.read_text(encoding="utf-8") if result.docker_log.exists() else ""
    assert docker_output == "", f"Docker command was invoked unexpectedly:\n{docker_output}\n{result.output}"
    assert "Starting Docker containers" not in result.output
    assert "Launching read-only Streamlit UI" not in result.output
