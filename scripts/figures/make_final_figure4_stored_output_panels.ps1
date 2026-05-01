param(
    [string]$RepoRoot = (Resolve-Path ".").Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Drawing

$repoRootPath = (Resolve-Path -LiteralPath $RepoRoot).Path
$outputDir = Join-Path $repoRootPath "output/figure_package_publication"
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
$script:Ndash = [char]0x2013

function Get-RepoPath {
    param([string]$RelativePath)
    return Join-Path $repoRootPath $RelativePath
}

function Save-Png {
    param(
        [System.Drawing.Bitmap]$Bitmap,
        [string]$Path
    )
    $Bitmap.Save($Path, [System.Drawing.Imaging.ImageFormat]::Png)
}

function New-Canvas {
    param(
        [int]$Width = 1800,
        [int]$Height = 1100
    )
    $bitmap = New-Object System.Drawing.Bitmap($Width, $Height)
    $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
    $graphics.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $graphics.TextRenderingHint = [System.Drawing.Text.TextRenderingHint]::ClearTypeGridFit
    $graphics.Clear([System.Drawing.Color]::White)
    return [pscustomobject]@{ Bitmap = $bitmap; Graphics = $graphics }
}

function New-Font {
    param(
        [single]$Size,
        [System.Drawing.FontStyle]$Style = [System.Drawing.FontStyle]::Regular
    )
    return New-Object System.Drawing.Font("Arial", $Size, $Style, [System.Drawing.GraphicsUnit]::Point)
}

function Draw-Text {
    param(
        [System.Drawing.Graphics]$Graphics,
        [string]$Text,
        [System.Drawing.Font]$Font,
        [System.Drawing.Brush]$Brush,
        [single]$X,
        [single]$Y,
        [single]$Width = 1600,
        [single]$Height = 80
    )
    $rect = New-Object System.Drawing.RectangleF($X, $Y, $Width, $Height)
    $format = New-Object System.Drawing.StringFormat
    $format.Trimming = [System.Drawing.StringTrimming]::Word
    $format.FormatFlags = [System.Drawing.StringFormatFlags]::LineLimit
    $Graphics.DrawString($Text, $Font, $Brush, $rect, $format)
    $format.Dispose()
}

function Write-ProvenanceJson {
    param(
        [string]$FigureLabel,
        [string]$Title,
        [string]$OutputPath,
        [string[]]$SourcePaths,
        [string]$Caption,
        [string]$Status
    )
    $jsonPath = [System.IO.Path]::ChangeExtension($OutputPath, ".json")
    $payload = [ordered]@{
        figure_label = $FigureLabel
        title = $Title
        output_path = ($OutputPath.Substring($repoRootPath.Length + 1) -replace "\\", "/")
        status = $Status
        source_file_paths_used = $SourcePaths
        caption_provenance = $Caption
        generation_note = "Generated from stored repository outputs only; no scientific rerun, model simulation, remote download, or manuscript-PDF extraction was performed."
    }
    $payload | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $jsonPath -Encoding UTF8
}

function Draw-Header {
    param(
        [System.Drawing.Graphics]$Graphics,
        [string]$Title,
        [string]$Subtitle
    )
    $titleFont = New-Font -Size 25 -Style ([System.Drawing.FontStyle]::Bold)
    $subtitleFont = New-Font -Size 13
    $dark = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(35, 35, 35))
    $muted = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(85, 85, 85))
    Draw-Text -Graphics $Graphics -Text $Title -Font $titleFont -Brush $dark -X 56 -Y 34 -Width 1680 -Height 42
    Draw-Text -Graphics $Graphics -Text $Subtitle -Font $subtitleFont -Brush $muted -X 58 -Y 80 -Width 1680 -Height 48
    $titleFont.Dispose()
    $subtitleFont.Dispose()
    $dark.Dispose()
    $muted.Dispose()
}

function Draw-Figure41 {
    $acceptedPath = Get-RepoPath "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_accepted_segment_registry.csv"
    $subsetPath = Get-RepoPath "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_ranking_subset_registry.csv"
    $rows = Import-Csv -LiteralPath $acceptedPath
    $subsetIds = @{}
    Import-Csv -LiteralPath $subsetPath | ForEach-Object { $subsetIds[$_.segment_id] = $true }

    $canvas = New-Canvas
    $g = $canvas.Graphics
    Draw-Header -Graphics $g `
        -Title ("Figure 4.1. Focused Phase 1 accepted February" + $script:Ndash + "April segment map") `
        -Subtitle ("Stored Phase 1 accepted segment registry; blue segments are the February" + $script:Ndash + "April ranked subset (n = 19), gray segments are the remaining strict accepted pool.")

    $left = 150
    $top = 165
    $width = 1320
    $height = 770
    $lonMin = 118.751
    $lonMax = 124.305
    $latMin = 10.620
    $latMax = 16.026

    $axisPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(45, 45, 45), 2)
    $gridPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(220, 220, 220), 1)
    $grayPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(150, 150, 150), 2)
    $bluePen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(32, 103, 172), 4)
    $boxPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(25, 25, 25), 2)
    $sourceBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(210, 70, 42))
    $textBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(45, 45, 45))
    $smallFont = New-Font -Size 10
    $labelFont = New-Font -Size 12 -Style ([System.Drawing.FontStyle]::Bold)

    function Project-X([double]$lon) {
        return [single]($left + (($lon - $lonMin) / ($lonMax - $lonMin)) * $width)
    }
    function Project-Y([double]$lat) {
        return [single]($top + (1 - (($lat - $latMin) / ($latMax - $latMin))) * $height)
    }

    foreach ($lon in 119..124) {
        $x = Project-X $lon
        $g.DrawLine($gridPen, $x, $top, $x, $top + $height)
        $g.DrawString("$lon E", $smallFont, $textBrush, [single]($x - 18), [single]($top + $height + 14))
    }
    foreach ($lat in 11..16) {
        $y = Project-Y $lat
        $g.DrawLine($gridPen, $left, $y, $left + $width, $y)
        $g.DrawString("$lat N", $smallFont, $textBrush, [single]($left - 58), [single]($y - 8))
    }
    $g.DrawRectangle($axisPen, $left, $top, $width, $height)

    foreach ($row in $rows) {
        $x1 = Project-X ([double]$row.start_lon)
        $y1 = Project-Y ([double]$row.start_lat)
        $x2 = Project-X ([double]$row.end_lon)
        $y2 = Project-Y ([double]$row.end_lat)
        $pen = if ($subsetIds.ContainsKey($row.segment_id)) { $bluePen } else { $grayPen }
        $g.DrawLine($pen, $x1, $y1, $x2, $y2)
        $g.FillEllipse([System.Drawing.Brushes]::White, [single]($x1 - 3), [single]($y1 - 3), 6, 6)
        $g.DrawEllipse($pen, [single]($x1 - 3), [single]($y1 - 3), 6, 6)
    }

    $sourceX = Project-X 121.5279999999
    $sourceY = Project-Y 13.3229999999501
    $g.FillEllipse($sourceBrush, [single]($sourceX - 7), [single]($sourceY - 7), 14, 14)
    $g.DrawString("Mindoro source point", $smallFont, $textBrush, [single]($sourceX + 12), [single]($sourceY - 12))

    $legendX = 1510
    $legendY = 225
    $g.DrawString("Legend", $labelFont, $textBrush, [single]$legendX, [single]$legendY)
    $g.DrawLine($bluePen, $legendX, $legendY + 50, $legendX + 58, $legendY + 50)
    $g.DrawString("Feb-Apr ranked subset (19)", $smallFont, $textBrush, [single]($legendX + 70), [single]($legendY + 38))
    $g.DrawLine($grayPen, $legendX, $legendY + 92, $legendX + 58, $legendY + 92)
    $g.DrawString("Other strict accepted (46)", $smallFont, $textBrush, [single]($legendX + 70), [single]($legendY + 80))
    $g.FillEllipse($sourceBrush, [single]$legendX, [single]($legendY + 126), 14, 14)
    $g.DrawString("Reference source point", $smallFont, $textBrush, [single]($legendX + 70), [single]($legendY + 118))
    Draw-Text -Graphics $g -Text "Claim boundary: transport provenance only; this is not public-spill validation." -Font $smallFont -Brush $textBrush -X 1510 -Y 410 -Width 245 -Height 95

    $out = Join-Path $outputDir "figure_4_1_focused_phase1_accepted_feb_apr_segment_map.png"
    Save-Png -Bitmap $canvas.Bitmap -Path $out
    Write-ProvenanceJson -FigureLabel "Figure 4.1" -Title ("Focused Phase 1 accepted February" + $script:Ndash + "April segment map") -OutputPath $out -SourcePaths @(
        "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_accepted_segment_registry.csv",
        "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_ranking_subset_registry.csv"
    ) -Caption ("Focused Phase 1 accepted February" + $script:Ndash + "April segment map drawn from stored Phase 1 segment registries.") -Status "generated_from_stored_outputs"

    $axisPen.Dispose(); $gridPen.Dispose(); $grayPen.Dispose(); $bluePen.Dispose(); $boxPen.Dispose()
    $sourceBrush.Dispose(); $textBrush.Dispose(); $smallFont.Dispose(); $labelFont.Dispose()
    $g.Dispose(); $canvas.Bitmap.Dispose()
}

function Draw-Figure42 {
    $rankingPath = Get-RepoPath "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_recipe_ranking.csv"
    $rows = @(Import-Csv -LiteralPath $rankingPath | Sort-Object {[double]$_.mean_ncs_score})

    $canvas = New-Canvas
    $g = $canvas.Graphics
    Draw-Header -Graphics $g `
        -Title "Figure 4.2. Focused Phase 1 recipe ranking chart" `
        -Subtitle ("Mean raw NCS from the stored February" + $script:Ndash + "April focused Phase 1 ranked subset; lower NCS indicates better transport agreement.")

    $left = 360
    $top = 185
    $barAreaWidth = 1020
    $barHeight = 78
    $gap = 48
    $xMin = 4.45
    $xMax = 4.90
    $axisPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(70, 70, 70), 2)
    $gridPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(225, 225, 225), 1)
    $winnerBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(52, 112, 172))
    $otherBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(190, 190, 190))
    $textBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(40, 40, 40))
    $mutedBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(90, 90, 90))
    $font = New-Font -Size 13
    $bold = New-Font -Size 13 -Style ([System.Drawing.FontStyle]::Bold)
    $small = New-Font -Size 10
    $medianPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(25, 25, 25), 4)

    function Scale-X([double]$value) {
        return [single]($left + (($value - $xMin) / ($xMax - $xMin)) * $barAreaWidth)
    }

    foreach ($tick in 0..4) {
        $value = 4.5 + ($tick * 0.1)
        $x = Scale-X $value
        $g.DrawLine($gridPen, $x, $top - 18, $x, $top + 4 * ($barHeight + $gap) - $gap + 25)
        $g.DrawString(("{0:N1}" -f $value), $small, $mutedBrush, [single]($x - 16), [single]($top + 4 * ($barHeight + $gap) - $gap + 38))
    }
    $g.DrawLine($axisPen, $left, $top + 4 * ($barHeight + $gap) - $gap + 25, $left + $barAreaWidth, $top + 4 * ($barHeight + $gap) - $gap + 25)

    for ($i = 0; $i -lt $rows.Count; $i++) {
        $row = $rows[$i]
        $y = $top + $i * ($barHeight + $gap)
        $mean = [double]$row.mean_ncs_score
        $median = [double]$row.median_ncs_score
        $isWinner = $row.recipe -eq "cmems_gfs"
        $brush = if ($isWinner) { $winnerBrush } else { $otherBrush }
        $nameFont = if ($isWinner) { $bold } else { $font }
        $barW = [single]((Scale-X $mean) - $left)
        $g.DrawString(("{0}. {1}" -f $row.rank, $row.recipe), $nameFont, $textBrush, 60, [single]($y + 22))
        $g.FillRectangle($brush, $left, $y, $barW, $barHeight)
        $g.DrawString(("{0:N4}" -f $mean), $font, $textBrush, [single]($left + $barW + 16), [single]($y + 22))
        $medianX = Scale-X $median
        $g.DrawLine($medianPen, $medianX, [single]($y - 8), $medianX, [single]($y + $barHeight + 8))
        $g.DrawString(("median {0:N4}" -f $median), $small, $mutedBrush, [single]($medianX + 10), [single]($y + $barHeight - 6))
    }

    Draw-Text -Graphics $g -Text "Selected recipe: cmems_gfs. Figure is transport-provenance support only, not an oil-footprint validation result." -Font $small -Brush $textBrush -X 60 -Y 875 -Width 1500 -Height 70

    $out = Join-Path $outputDir "figure_4_2_focused_phase1_recipe_ranking_chart.png"
    Save-Png -Bitmap $canvas.Bitmap -Path $out
    Write-ProvenanceJson -FigureLabel "Figure 4.2" -Title "Focused Phase 1 recipe ranking chart" -OutputPath $out -SourcePaths @(
        "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_recipe_ranking.csv",
        "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_recipe_summary.csv"
    ) -Caption "Focused Phase 1 recipe ranking chart drawn from stored ranking CSV; lower raw NCS is better." -Status "generated_from_stored_outputs"

    $axisPen.Dispose(); $gridPen.Dispose(); $winnerBrush.Dispose(); $otherBrush.Dispose()
    $textBrush.Dispose(); $mutedBrush.Dispose(); $font.Dispose(); $bold.Dispose(); $small.Dispose(); $medianPen.Dispose()
    $g.Dispose(); $canvas.Bitmap.Dispose()
}

function Draw-TifPanel {
    param(
        [System.Drawing.Graphics]$Graphics,
        [string]$Path,
        [string]$Title,
        [int]$X,
        [int]$Y,
        [int]$W,
        [int]$H
    )
    $panelPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(80, 80, 80), 2)
    $titleBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(30, 30, 30))
    $font = New-Font -Size 13 -Style ([System.Drawing.FontStyle]::Bold)
    $img = [System.Drawing.Image]::FromFile((Resolve-Path -LiteralPath $Path).Path)
    $Graphics.DrawRectangle($panelPen, $X, $Y, $W, $H)
    $Graphics.DrawImage($img, $X + 18, $Y + 52, $W - 36, $H - 76)
    $Graphics.DrawString($Title, $font, $titleBrush, [single]($X + 18), [single]($Y + 16))
    $img.Dispose(); $panelPen.Dispose(); $titleBrush.Dispose(); $font.Dispose()
}

function Draw-Figure43 {
    $sources = [ordered]@{
        "Deterministic footprint" = "output/CASE_MINDORO_RETRO_2023/forecast/control_footprint_mask_2023-03-06T09-59-00Z.tif"
        "prob_presence" = "output/CASE_MINDORO_RETRO_2023/official_rerun_r1/R1_selected_previous/forecast_datecomposites/prob_presence_2023-03-06_datecomposite.tif"
        "mask_p50 (P >= 0.50)" = "output/CASE_MINDORO_RETRO_2023/official_rerun_r1/R1_selected_previous/forecast_datecomposites/mask_p50_2023-03-06_datecomposite.tif"
        "mask_p90 core (P >= 0.90)" = "output/CASE_MINDORO_RETRO_2023/official_rerun_r1/R1_selected_previous/forecast_datecomposites/mask_p90_2023-03-06_datecomposite.tif"
    }
    foreach ($path in $sources.Values) {
        if (-not (Test-Path -LiteralPath (Get-RepoPath $path))) {
            throw "Missing stored product-family source: $path"
        }
    }

    $canvas = New-Canvas
    $g = $canvas.Graphics
    Draw-Header -Graphics $g `
        -Title "Figure 4.3. Mindoro product-family board with deterministic, probability, and threshold surfaces" `
        -Subtitle "Stored March 6 product-family surfaces; mask_p50 is the preferred probabilistic footprint and mask_p90 is conservative high-confidence support only."

    $positions = @(
        @{ X = 90; Y = 175 },
        @{ X = 940; Y = 175 },
        @{ X = 90; Y = 575 },
        @{ X = 940; Y = 575 }
    )
    $i = 0
    foreach ($entry in $sources.GetEnumerator()) {
        $pos = $positions[$i]
        Draw-TifPanel -Graphics $g -Path (Get-RepoPath $entry.Value) -Title $entry.Key -X $pos.X -Y $pos.Y -W 760 -H 340
        $i++
    }

    $small = New-Font -Size 10
    $brush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(50, 50, 50))
    Draw-Text -Graphics $g -Text "This board is a standardized product-family display from stored raster outputs only; it is not an additional validation score." -Font $small -Brush $brush -X 90 -Y 955 -Width 1580 -Height 52

    $out = Join-Path $outputDir "figure_4_3_mindoro_product_family_board.png"
    Save-Png -Bitmap $canvas.Bitmap -Path $out
    Write-ProvenanceJson -FigureLabel "Figure 4.3" -Title "Mindoro product-family board with deterministic, probability, and threshold surfaces" -OutputPath $out -SourcePaths @($sources.Values) -Caption "Mindoro product-family board drawn from stored deterministic, prob_presence, mask_p50, and mask_p90 rasters." -Status "generated_from_stored_outputs"

    $small.Dispose(); $brush.Dispose(); $g.Dispose(); $canvas.Bitmap.Dispose()
}

function Draw-Figure46 {
    $rankingPath = Get-RepoPath "output/Phase 3B March13-14 Final Output/summary/comparator_pygnome/march13_14_reinit_crossmodel_model_ranking.csv"
    $rows = @(Import-Csv -LiteralPath $rankingPath | Where-Object { $_.track_id -in @("R1_previous_reinit_p50", "pygnome_reinit_deterministic") })

    $canvas = New-Canvas
    $g = $canvas.Graphics
    Draw-Header -Graphics $g `
        -Title ("Figure 4.6. Mindoro same-case OpenDrift" + $script:Ndash + "PyGNOME comparator mean FSS summary") `
        -Subtitle "Mean FSS from the stored same-case comparator ranking CSV; PyGNOME is comparator-only and never observational truth."

    $left = 360
    $top = 260
    $barAreaWidth = 1000
    $barHeight = 110
    $gap = 95
    $xMax = 0.12
    $axisPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(70, 70, 70), 2)
    $gridPen = New-Object System.Drawing.Pen([System.Drawing.Color]::FromArgb(225, 225, 225), 1)
    $odBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(52, 112, 172))
    $pyBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(180, 116, 54))
    $textBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(40, 40, 40))
    $font = New-Font -Size 14
    $bold = New-Font -Size 14 -Style ([System.Drawing.FontStyle]::Bold)
    $small = New-Font -Size 10

    function Scale-FSS([double]$value) {
        return [single]($left + ($value / $xMax) * $barAreaWidth)
    }

    foreach ($tick in 0..6) {
        $value = $tick * 0.02
        $x = Scale-FSS $value
        $g.DrawLine($gridPen, $x, $top - 25, $x, $top + 2 * ($barHeight + $gap) - $gap + 35)
        $g.DrawString(("{0:N2}" -f $value), $small, $textBrush, [single]($x - 14), [single]($top + 2 * ($barHeight + $gap) - $gap + 52))
    }
    $g.DrawLine($axisPen, $left, $top + 2 * ($barHeight + $gap) - $gap + 35, $left + $barAreaWidth, $top + 2 * ($barHeight + $gap) - $gap + 35)

    for ($i = 0; $i -lt $rows.Count; $i++) {
        $row = $rows[$i]
        $y = $top + $i * ($barHeight + $gap)
        $value = [double]$row.mean_fss
        $isOpenDrift = $row.track_id -eq "R1_previous_reinit_p50"
        $label = if ($isOpenDrift) { "OpenDrift p50" } else { "PyGNOME deterministic" }
        $brush = if ($isOpenDrift) { $odBrush } else { $pyBrush }
        $nameFont = if ($isOpenDrift) { $bold } else { $font }
        $barW = [single]((Scale-FSS $value) - $left)
        $g.DrawString($label, $nameFont, $textBrush, 70, [single]($y + 38))
        $g.FillRectangle($brush, $left, $y, $barW, $barHeight)
        $g.DrawString(("{0:N4}" -f $value), $font, $textBrush, [single]($left + $barW + 18), [single]($y + 38))
    }
    Draw-Text -Graphics $g -Text "Comparator-only: the observed March 14 public mask remains the scoring reference; PyGNOME is not a truth product and this is not a second validation row." -Font $small -Brush $textBrush -X 70 -Y 735 -Width 1500 -Height 75

    $out = Join-Path $outputDir "figure_4_6_mindoro_comparator_mean_fss_summary.png"
    Save-Png -Bitmap $canvas.Bitmap -Path $out
    Write-ProvenanceJson -FigureLabel "Figure 4.6" -Title ("Mindoro same-case OpenDrift" + $script:Ndash + "PyGNOME comparator mean FSS summary") -OutputPath $out -SourcePaths @(
        "output/Phase 3B March13-14 Final Output/summary/comparator_pygnome/march13_14_reinit_crossmodel_model_ranking.csv"
    ) -Caption "Mindoro same-case comparator mean FSS summary drawn from the stored comparator ranking CSV." -Status "generated_from_stored_outputs"

    $axisPen.Dispose(); $gridPen.Dispose(); $odBrush.Dispose(); $pyBrush.Dispose()
    $textBrush.Dispose(); $font.Dispose(); $bold.Dispose(); $small.Dispose()
    $g.Dispose(); $canvas.Bitmap.Dispose()
}

Draw-Figure41
Draw-Figure42
Draw-Figure43
Draw-Figure46

Write-Output "Generated final Figure 4 stored-output panels in output/figure_package_publication."
