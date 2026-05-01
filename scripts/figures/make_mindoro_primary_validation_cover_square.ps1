param(
    [string]$OutputDir = "output/presentation_assets/cover",
    [string]$FontPath = "C:\Windows\Fonts\arial.ttf",
    [switch]$NoDocker
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$pythonScript = Join-Path $repoRoot "scripts\figures\make_mindoro_primary_validation_cover_square.py"

if (-not (Test-Path -LiteralPath $pythonScript)) {
    throw "Missing Python generator: $pythonScript"
}
if (-not (Test-Path -LiteralPath $FontPath)) {
    throw "Arial font not found: $FontPath"
}

Push-Location $repoRoot
try {
    if ($NoDocker) {
        python $pythonScript --output-dir $OutputDir --font-path $FontPath
        exit $LASTEXITCODE
    }

    $workDir = Join-Path $OutputDir "_work"
    New-Item -ItemType Directory -Force -Path $workDir | Out-Null
    $workFont = Join-Path $workDir "arial.ttf"
    Copy-Item -LiteralPath $FontPath -Destination $workFont -Force

    try {
        $containerFontPath = $workFont -replace "\\", "/"
        Get-Content -LiteralPath $pythonScript -Raw |
            docker compose exec -T pipeline python - --output-dir $OutputDir --font-path $containerFontPath
        $exitCode = $LASTEXITCODE
    }
    finally {
        if (Test-Path -LiteralPath $workFont) {
            Remove-Item -LiteralPath $workFont -Force
        }
        if ((Test-Path -LiteralPath $workDir) -and ((Get-ChildItem -LiteralPath $workDir -Force | Measure-Object).Count -eq 0)) {
            Remove-Item -LiteralPath $workDir -Force
        }
    }

    exit $exitCode
}
finally {
    Pop-Location
}
