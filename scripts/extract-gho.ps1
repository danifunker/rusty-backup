# Walk a file-aware GHO via rb-cli and extract every regular file under SRC
# (default '/') to DEST on the host. Mirrors the directory layout. Files that
# fail to extract get logged to <DEST>\_recovery-errors.log so you can see
# what's recoverable vs not.
#
# Usage (admin not required):
#   pwsh -File extract-gho.ps1 -Gho C:\path\to\thing.GHO -Dest C:\recovery
#   pwsh -File extract-gho.ps1 -Gho C:\path\to\thing.GHO -Dest C:\recovery -SrcRoot /WINDOWS
#
# Skips $ entries (NTFS system files like $Extend, $MFT, etc.) since they
# aren't user data and would just clutter the output.

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)] [string] $Gho,
    [Parameter(Mandatory=$true)] [string] $Dest,
    [string] $SrcRoot = '/',
    [string] $RbCli = (Join-Path $PSScriptRoot '..\target\release\rb-cli.exe' | Resolve-Path).Path,
    [int] $MaxFiles = 0           # 0 = no cap; useful for quick sampling
)

$ErrorActionPreference = 'Continue'

if (-not (Test-Path $RbCli)) {
    Write-Error "rb-cli not found at $RbCli"
    exit 1
}

if (-not (Test-Path $Dest)) {
    New-Item -ItemType Directory -Path $Dest -Force | Out-Null
}

$errLog = Join-Path $Dest '_recovery-errors.log'
'Recovery started: ' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') | Out-File $errLog

$stats = @{ files = 0; dirs = 0; ok = 0; failed = 0; bytes = 0L }

function Walk-Directory {
    param([string] $Path)

    $listing = & $RbCli ls $Gho $Path 2>$null
    if ($LASTEXITCODE -ne 0) {
        "LIST-ERROR $Path" | Out-File $errLog -Append
        return
    }

    foreach ($line in $listing) {
        # Each line looks like: 'FILE     12345     name' or 'DIR     0     name'
        # The "Partition..." header line is skipped.
        if ($line -match '^\s*(FILE|DIR)\s+(\d+)\s+(.+)$') {
            $kind = $matches[1]
            $size = [long] $matches[2]
            $name = $matches[3].TrimEnd()

            if ($name.StartsWith('$')) { continue }  # skip NTFS metafiles

            $srcChild  = if ($Path -eq '/') { "/$name" } else { "$Path/$name" }
            $destChild = Join-Path $Dest ($srcChild.TrimStart('/').Replace('/', [IO.Path]::DirectorySeparatorChar))

            if ($kind -eq 'DIR') {
                $stats.dirs++
                if (-not (Test-Path $destChild)) {
                    New-Item -ItemType Directory -Path $destChild -Force | Out-Null
                }
                Walk-Directory -Path $srcChild
            } else {
                $stats.files++
                if ($MaxFiles -gt 0 -and $stats.files -gt $MaxFiles) { return }

                $parent = Split-Path -Parent $destChild
                if (-not (Test-Path $parent)) {
                    New-Item -ItemType Directory -Path $parent -Force | Out-Null
                }

                & $RbCli get $Gho $srcChild $destChild 2>$null | Out-Null
                if ($LASTEXITCODE -eq 0 -and (Test-Path $destChild)) {
                    $stats.ok++
                    $stats.bytes += $size
                    if (($stats.ok % 50) -eq 0) {
                        Write-Host ("[{0}] {1}" -f $stats.ok, $srcChild)
                    }
                } else {
                    $stats.failed++
                    "GET-FAILED $srcChild (expected $size bytes)" | Out-File $errLog -Append
                }
            }
        }
    }
}

Walk-Directory -Path $SrcRoot

Write-Host ""
Write-Host "=== Recovery summary ==="
Write-Host ("Files seen:    {0}" -f $stats.files)
Write-Host ("Files OK:      {0}" -f $stats.ok)
Write-Host ("Files failed:  {0}" -f $stats.failed)
Write-Host ("Dirs visited:  {0}" -f $stats.dirs)
Write-Host ("Bytes written: {0:N0}" -f $stats.bytes)
Write-Host ("Error log:     {0}" -f $errLog)
