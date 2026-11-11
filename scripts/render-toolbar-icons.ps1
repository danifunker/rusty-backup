# Renders assets/icons/<set>/*.svg to PNGs with WPF, which ships with Windows (no extra tools).
# The SVGs hold only <path> elements with the attributes on <svg>, which both SVG and WPF read.
# One path may be a badge (class="badge", filled, placed by translate(x y) scale(s)): it is
# filled, and the other paths are cut back a little around it so the two never touch.
# Black icons go to png/ (light theme, macOS template images), white ones to png-dark/ (dark theme).
# Run from the repo root on Windows:  powershell -ExecutionPolicy Bypass -File scripts/render-toolbar-icons.ps1
# The browse view's row icons: powershell -Command "& ./scripts/render-toolbar-icons.ps1 -Group rows -Sizes 16,24,32"
param([string]$Group = "toolbar", [int[]]$Sizes = @(24, 32, 48))

Add-Type -AssemblyName PresentationCore, WindowsBase
$dir = Join-Path $PSScriptRoot "..\assets\icons\$Group"
$sets = @(@{ Dir = "png"; Brush = [System.Windows.Media.Brushes]::Black },
          @{ Dir = "png-dark"; Brush = [System.Windows.Media.Brushes]::White })
# The gap left around a badge, in 24ths of the icon.
$gap = 1.6

# A badge's geometry in icon units, from its path and its translate(x y) scale(s).
function Get-Badge($path) {
    # A parsed geometry is frozen, so the placement goes on a group around it.
    $geom = New-Object System.Windows.Media.GeometryGroup
    $geom.Children.Add([System.Windows.Media.Geometry]::Parse($path.d))
    $nums = [regex]::Matches($path.GetAttribute('transform'), '-?[\d.]+') | ForEach-Object { [double]$_.Value }
    $place = New-Object System.Windows.Media.TransformGroup
    $place.Children.Add((New-Object System.Windows.Media.ScaleTransform $nums[2], $nums[2]))
    $place.Children.Add((New-Object System.Windows.Media.TranslateTransform $nums[0], $nums[1]))
    $geom.Transform = $place
    $geom.GetFlattenedPathGeometry()
}

foreach ($set in $sets) {
$out = Join-Path $dir $set.Dir
New-Item -ItemType Directory -Force $out | Out-Null
foreach ($svg in Get-ChildItem $dir -Filter *.svg) {
    [xml]$doc = Get-Content $svg.FullName
    $root = $doc.svg
    $width = [double]$root.'stroke-width'
    $paths = @($root.path | Where-Object { $_.GetAttribute('class') -ne 'badge' } | ForEach-Object { $_.d })
    $badge = $root.path | Where-Object { $_.GetAttribute('class') -eq 'badge' } | Select-Object -First 1
    $clip = $null
    if ($badge) {
        $badge = Get-Badge $badge
        $edge = New-Object System.Windows.Media.Pen ([System.Windows.Media.Brushes]::Black), (2 * $gap)
        $edge.LineJoin = 'Round'
        $knock = New-Object System.Windows.Media.CombinedGeometry 'Union', $badge.GetWidenedPathGeometry($edge), $badge
        $all = New-Object System.Windows.Media.RectangleGeometry (New-Object System.Windows.Rect 0, 0, 24, 24)
        $clip = New-Object System.Windows.Media.CombinedGeometry 'Exclude', $all, $knock
    }
    foreach ($size in $Sizes) {
        $scale = $size / 24.0
        $visual = New-Object System.Windows.Media.DrawingVisual
        $dc = $visual.RenderOpen()
        $pen = New-Object System.Windows.Media.Pen ($set.Brush), $width
        $pen.StartLineCap = 'Round'; $pen.EndLineCap = 'Round'; $pen.LineJoin = 'Round'
        $dc.PushTransform((New-Object System.Windows.Media.ScaleTransform $scale, $scale))
        if ($clip) { $dc.PushClip($clip) }
        foreach ($d in $paths) {
            $dc.DrawGeometry($null, $pen, [System.Windows.Media.Geometry]::Parse($d))
        }
        if ($clip) { $dc.Pop(); $dc.DrawGeometry($set.Brush, $null, $badge) }
        $dc.Pop(); $dc.Close()
        $bmp = New-Object System.Windows.Media.Imaging.RenderTargetBitmap $size, $size, 96, 96, ([System.Windows.Media.PixelFormats]::Pbgra32)
        $bmp.Render($visual)
        $enc = New-Object System.Windows.Media.Imaging.PngBitmapEncoder
        $enc.Frames.Add([System.Windows.Media.Imaging.BitmapFrame]::Create($bmp))
        $file = Join-Path $out ("{0}-{1}.png" -f $svg.BaseName, $size)
        $fs = [System.IO.File]::Create($file); $enc.Save($fs); $fs.Close()
    }
    "rendered $($set.Dir)/$($svg.BaseName)"
}
}
