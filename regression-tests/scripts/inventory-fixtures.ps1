<#
.SYNOPSIS
    Scan a source tree for candidate regression fixtures.

.DESCRIPTION
    Walks a directory recursively, matching files against the disk-image and
    optical-image extensions the engine knows how to open (kept in step with
    DISK_IMAGE_EXTS in src/model/file_types.rs), and emits a TSV catalogue of
    candidates plus a per-extension summary.

    This is a *harvest* tool, not a decision tool. It answers "what disk-image
    material exists here", and a human triages the output into logical fixture
    IDs afterwards. See regression-tests/FIXTURES.md.

    The output TSV contains real paths and therefore must never be committed;
    regression-tests/scans/ is gitignored, and the consolidated catalogue lives
    on the NAS.

.PARAMETER Source
    Directory to scan. UNC paths are fine.

.PARAMETER Label
    Short name for this source, recorded in the 'origin' column so rows from
    different scans can be merged.

.PARAMETER OutDir
    Where to write the TSV and summary. Defaults to regression-tests/scans.

.PARAMETER MinBytes
    Ignore files smaller than this. Defaults to 65536; below that a file is
    almost never a disk image worth cataloguing.

.PARAMETER IncludeNoisy
    Also match .bin, .zip and .gz. These are real container extensions for us
    but are overwhelmingly common as something else, so they are off by
    default to keep a first scan readable.

.PARAMETER Hash
    Compute SHA256 for every match. Correct, but slow over SMB — leave it off
    for a survey scan and enable it when building the final catalogue.

.PARAMETER ByFolder
    Also match any file under a directory whose name looks like a platform we
    care about, regardless of extension.

    Extension matching alone has a blind spot that cost us the CD-i, 3DO,
    GameCube and Mac-optical fixtures on the first pass: those discs carry no
    distinctive extension. They are .chd, .iso, .cue/.bin — indistinguishable
    from thousands of console dumps with no fixture value. What identifies
    them is the *folder they sit in* ("CDi", "Philips CD-i", "3DO", "GCN").

    So for any format whose payload is a generic container, search by
    location, not by extension. This switch does that.

.EXAMPLE
    .\inventory-fixtures.ps1 -Source '\\NAS\share' -Label nas-software
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Source,
    [Parameter(Mandatory = $true)][string]$Label,
    [string]$OutDir,
    [long]$MinBytes = 65536,
    [switch]$IncludeNoisy,
    [switch]$Hash,
    [switch]$ByFolder
)

$ErrorActionPreference = 'Stop'

# Kept in step with DISK_IMAGE_EXTS (src/model/file_types.rs), case-folded,
# plus optical container extensions the optical stack accepts and a handful of
# floppy-preservation formats worth spotting even if we cannot open them yet.
$diskExts = @(
    'vhd','img','raw','squashfs','appimage','iso','hda','hdv','2mg','dmg',
    'po','do','dsk','dc42','woz','moof','chd','adf','hdf','adz','hdz','imz',
    'vmdk','qcow2','qcow','gho','ghs','hfv','d88','xdf','hdm','dim','hds',
    'ima','d64','d71','d81','g64','g71','d80','d82','atr','xfd','jvc','vdk',
    'ssd','dsd','trd','pdi','bfs','copydisk','altodisk','zdisk','zdelta',
    'dsk80','dsk300','dsk44','cbk','dart','sparseimage','smi','dd'
)
$opticalExts = @('cue','nrg','ccd','mds','mdf','toast','cdi','gdi','mdx')
# Floppy-preservation formats: catalogued as candidates so we can see what
# material exists, even where the engine has no decoder today.
$floppyExts  = @('st','msa','hfe','scp','ipf','dmk','td0','imd','fdi','vfd')
$noisyExts   = @('bin','zip','gz')

# Platform folders whose contents are disc images with no distinguishing
# extension. Matched against any component of the path under -ByFolder.
# Extend this list rather than widening $diskExts — a generic container in the
# right folder is a fixture; the same container anywhere else is noise.
$folderPatterns = @(
    'cd-?i$', 'philips cd-?i', '3do', 'panasonic',
    'gcn', 'gamecube', '\bwii\b',
    'fmtowns', 'tgcd', 'pc-?engine cd', 'neogeo cd', 'segacd', 'mega ?cd',
    'irix', 'sgi', 'ods-?2', 'vms', 'openvms',
    'pippin', 'cd32', 'cdtv'
)
$folderRegex = ($folderPatterns -join '|')
# Containers worth picking up when the folder already tells us what they are.
$folderExts = [System.Collections.Generic.HashSet[string]]::new(
    [string[]]@('chd','iso','cue','bin','img','nrg','ccd','mdf','rvz','gdi','cdi'),
    [System.StringComparer]::OrdinalIgnoreCase)

$exts = $diskExts + $opticalExts + $floppyExts
if ($IncludeNoisy) { $exts += $noisyExts }
$extSet = [System.Collections.Generic.HashSet[string]]::new(
    [string[]]$exts, [System.StringComparer]::OrdinalIgnoreCase)

if (-not $OutDir) {
    $OutDir = Join-Path (Split-Path -Parent $PSScriptRoot) 'scans'
}
if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

if (-not (Test-Path -LiteralPath $Source)) {
    Write-Error "Source not reachable: $Source"
}

$stamp   = Get-Date -Format 'yyyyMMdd-HHmmss'
$tsvPath = Join-Path $OutDir "scan-$Label-$stamp.tsv"
$sumPath = Join-Path $OutDir "scan-$Label-$stamp.summary.txt"

Write-Host "Scanning $Source (label=$Label)"
Write-Host "  extensions : $($extSet.Count)"
Write-Host "  min size   : $MinBytes bytes"
Write-Host "  hashing    : $($Hash.IsPresent)"

$rows      = New-Object System.Collections.Generic.List[object]
$scanned   = 0
$errors    = New-Object System.Collections.Generic.List[string]

# -ErrorAction Ignore so an unreadable subtree (permissions, offline share)
# skips rather than aborting a multi-hour scan. Unreadable directories are
# counted and reported rather than silently dropped.
Get-ChildItem -LiteralPath $Source -File -Recurse -Force -ErrorAction SilentlyContinue -ErrorVariable scanErrors |
    ForEach-Object {
        $scanned++
        if ($scanned % 20000 -eq 0) { Write-Host "  ...$scanned files seen, $($rows.Count) matched" }

        $ext = $_.Extension.TrimStart('.')

        $matched = $ext -and $extSet.Contains($ext)
        $why     = 'ext'
        if (-not $matched -and $ByFolder -and $ext -and $folderExts.Contains($ext)) {
            # The folder, not the extension, is what identifies these.
            if ($_.DirectoryName -match $folderRegex) {
                $matched = $true
                $why     = 'folder'
            }
        }

        if ($matched -and $_.Length -ge $MinBytes) {
            # Skip recycle bins and our own scratch output.
            if ($_.FullName -match '(\\|/)(#recycle|\$RECYCLE\.BIN|regression-tests[\\/]scratch)(\\|/)') { return }

            $sha = ''
            if ($Hash) {
                try { $sha = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash }
                catch { $sha = 'ERROR' }
            }

            $rows.Add([pscustomobject]@{
                origin = $Label
                why    = $why
                ext    = $ext.ToLowerInvariant()
                bytes  = $_.Length
                mtime  = $_.LastWriteTimeUtc.ToString('s')
                sha256 = $sha
                path   = $_.FullName
            })
        }
    }

foreach ($e in $scanErrors) { $errors.Add($e.ToString()) }

$rows | Sort-Object ext, bytes |
    Export-Csv -LiteralPath $tsvPath -Delimiter "`t" -NoTypeInformation -Encoding UTF8

$summary = New-Object System.Collections.Generic.List[string]
$summary.Add("source     : $Source")
$summary.Add("label      : $Label")
$summary.Add("scanned    : $scanned files")
$summary.Add("matched    : $($rows.Count) candidates")
$summary.Add("total size : {0:N2} GiB" -f (($rows | Measure-Object bytes -Sum).Sum / 1GB))
$summary.Add("unreadable : $($errors.Count) paths")
$summary.Add('')
$summary.Add('count'.PadRight(8) + 'GiB'.PadRight(10) + 'ext')
foreach ($g in ($rows | Group-Object ext | Sort-Object Count -Descending)) {
    $gib = ($g.Group | Measure-Object bytes -Sum).Sum / 1GB
    $summary.Add(("{0}" -f $g.Count).PadRight(8) + ("{0:N2}" -f $gib).PadRight(10) + $g.Name)
}
if ($errors.Count -gt 0) {
    $summary.Add('')
    $summary.Add('-- unreadable paths (first 40) --')
    $errors | Select-Object -First 40 | ForEach-Object { $summary.Add($_) }
}

$summary | Set-Content -LiteralPath $sumPath -Encoding UTF8
$summary | ForEach-Object { Write-Host $_ }

Write-Host ''
Write-Host "TSV     : $tsvPath"
Write-Host "Summary : $sumPath"
