# Renders assets/icons/toolbar/*.svg to PNGs with WPF, which ships with Windows (no extra tools).
# The SVGs hold only <path> elements with the attributes on <svg>, which both SVG and WPF read.
# Run from the repo root on Windows:  powershell -ExecutionPolicy Bypass -File scripts/render-toolbar-icons.ps1
param([int[]]$Sizes = @(24, 32, 48))

Add-Type -AssemblyName PresentationCore, WindowsBase
$dir = Join-Path $PSScriptRoot "..\assets\icons\toolbar"
$out = Join-Path $dir "png"
New-Item -ItemType Directory -Force $out | Out-Null

foreach ($svg in Get-ChildItem $dir -Filter *.svg) {
    [xml]$doc = Get-Content $svg.FullName
    $root = $doc.svg
    $width = [double]$root.'stroke-width'
    $paths = @($root.path | ForEach-Object { $_.d })
    foreach ($size in $Sizes) {
        $scale = $size / 24.0
        $visual = New-Object System.Windows.Media.DrawingVisual
        $dc = $visual.RenderOpen()
        $pen = New-Object System.Windows.Media.Pen ([System.Windows.Media.Brushes]::Black), $width
        $pen.StartLineCap = 'Round'; $pen.EndLineCap = 'Round'; $pen.LineJoin = 'Round'
        $dc.PushTransform((New-Object System.Windows.Media.ScaleTransform $scale, $scale))
        foreach ($d in $paths) {
            $dc.DrawGeometry($null, $pen, [System.Windows.Media.Geometry]::Parse($d))
        }
        $dc.Pop(); $dc.Close()
        $bmp = New-Object System.Windows.Media.Imaging.RenderTargetBitmap $size, $size, 96, 96, ([System.Windows.Media.PixelFormats]::Pbgra32)
        $bmp.Render($visual)
        $enc = New-Object System.Windows.Media.Imaging.PngBitmapEncoder
        $enc.Frames.Add([System.Windows.Media.Imaging.BitmapFrame]::Create($bmp))
        $file = Join-Path $out ("{0}-{1}.png" -f $svg.BaseName, $size)
        $fs = [System.IO.File]::Create($file); $enc.Save($fs); $fs.Close()
    }
    "rendered $($svg.BaseName)"
}
