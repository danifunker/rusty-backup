# Let Windows judge rb-cli's NTFS and exFAT edits: diskpart formats VHDs,
# Windows writes to them, rb-cli edits them, then a read-only chkdsk and dir /x
# give the verdict. Written for the 2026-09-01 audit (docs/Regression_Bugs.md,
# "Windows verification"); rerun it whenever the NTFS or exFAT writers change.
# Run from an ELEVATED pwsh, since attaching a VHD needs it:
#   pwsh -ExecutionPolicy Bypass -File scripts\verify-fs-windows.ps1 [-Only D12,exFAT] [-Ghost <image.gho>]
param(
    [string]$Rb = (Join-Path $PSScriptRoot '..\target\debug\rb-cli.exe'),
    [string]$Work = (Join-Path $env:TEMP 'rb-verify-fs-windows'),
    [string]$Ghost = '',
    [string[]]$Only = @()
)
$ErrorActionPreference = 'Stop'
$Rb = [IO.Path]::GetFullPath($Rb)
$Work = [IO.Path]::GetFullPath($Work)
New-Item -ItemType Directory -Force $Work | Out-Null
$log = Join-Path $Work 'results.log'
"" | Set-Content $log

function Log($m) {
    $line = "[{0}] {1}" -f (Get-Date -Format 'HH:mm:ss'), $m
    $line | Add-Content $log
    Write-Host $line
}
function Rb {
    param([Parameter(ValueFromRemainingArguments)][string[]]$a)
    Log ("rb-cli " + ($a -join ' '))
    $out = (& $Rb @a 2>&1 | Out-String)
    $code = $LASTEXITCODE
    $out.TrimEnd() | Add-Content $log
    Write-Host $out.TrimEnd()
    if ($code -ne 0) { throw "rb-cli exit $code : $($a -join ' ')" }
    return $out
}
function RbQuiet {
    param([Parameter(ValueFromRemainingArguments)][string[]]$a)
    $out = (& $Rb @a 2>&1 | Out-String)
    if ($LASTEXITCODE -ne 0) { $out | Add-Content $log; throw "rb-cli exit $LASTEXITCODE : $($a -join ' ')" }
}
function Diskpart([string]$script) {
    $f = Join-Path $Work ("dp-{0}.txt" -f [guid]::NewGuid())
    $script | Set-Content $f -Encoding ASCII
    $o = (& diskpart.exe /s $f 2>&1 | Out-String)
    $o | Add-Content $log
    Remove-Item $f -Force
    if ($LASTEXITCODE -ne 0) { throw "diskpart failed:`n$o" }
    return $o
}
function Get-VhdDisk([string]$Path) {
    $disk = Get-DiskImage -ImagePath $Path | Get-Disk
    if (-not $disk) { throw "no disk for $Path" }
    return $disk
}
function Get-VhdLetter([string]$Path) {
    $disk = Get-VhdDisk $Path
    $part = $disk | Get-Partition | Where-Object { $_.Type -ne 'Reserved' } | Select-Object -First 1
    if (-not $part) { throw "no partition on $Path" }
    if (-not $part.DriveLetter) {
        $part | Add-PartitionAccessPath -AssignDriveLetter | Out-Null
        Start-Sleep 1
        $part = Get-Partition -DiskNumber $disk.Number -PartitionNumber $part.PartitionNumber
    }
    if (-not $part.DriveLetter) { throw "no drive letter for $Path" }
    return [string]$part.DriveLetter
}
function New-TestVhd([string]$Path, [int]$SizeMB, [string]$Fs, [int]$PartMB = 0, [string]$Label = 'RBTEST') {
    if (Test-Path $Path) { Remove-Item $Path -Force }
    $size = if ($PartMB -gt 0) { "size=$PartMB" } else { '' }
    Diskpart @"
create vdisk file="$Path" maximum=$SizeMB type=fixed
select vdisk file="$Path"
attach vdisk
convert mbr
create partition primary $size
format fs=$Fs quick label=$Label
assign
exit
"@ | Out-Null
    Start-Sleep 2
    $l = Get-VhdLetter $Path
    Log "created $Path ($SizeMB MB, $Fs, partition $(if($PartMB){$PartMB}else{'whole'}) MB) as ${l}:"
    return $l
}
function Attach-Vhd([string]$Path) {
    Mount-DiskImage -ImagePath $Path | Out-Null
    Start-Sleep 2
    $l = Get-VhdLetter $Path
    Log "attached $Path as ${l}:"
    return $l
}
function Detach-Vhd([string]$Path) {
    Dismount-DiskImage -ImagePath $Path | Out-Null
    Start-Sleep 1
    Log "detached $Path"
}
function Chkdsk([string]$Letter, [string]$Tag) {
    Log "chkdsk ${Letter}: ($Tag, read-only)"
    $o = (& chkdsk.exe "${Letter}:" 2>&1 | Out-String)
    $code = $LASTEXITCODE
    $o | Set-Content (Join-Path $Work "chkdsk-$Tag.txt")
    $o | Add-Content $log
    Write-Host $o
    Log "chkdsk exit=$code"
    return $code
}
function DirX([string]$Path, [string]$Tag) {
    $o = (& cmd.exe /c "dir /x `"$Path`"" 2>&1 | Out-String)
    $o | Set-Content (Join-Path $Work "dirx-$Tag.txt")
    $o | Add-Content $log
    Write-Host $o
    return $o
}
# Attach a VHD just to learn what Windows makes of its volume; returns the FS type string.
function Probe-FsType([string]$Path, [string]$Tag) {
    Mount-DiskImage -ImagePath $Path | Out-Null
    Start-Sleep 2
    $disk = Get-VhdDisk $Path
    $part = $disk | Get-Partition | Where-Object { $_.Type -ne 'Reserved' } | Select-Object -First 1
    $vol = $part | Get-Volume
    $type = [string]$vol.FileSystemType
    $size = $vol.Size
    Log "probe $Tag : Windows sees FileSystemType=$type size=$size (partition $($part.Size) bytes)"
    Dismount-DiskImage -ImagePath $Path | Out-Null
    Start-Sleep 1
    return $type
}
function ReadSector([string]$Path, [long]$Lba) {
    $fs = [IO.File]::Open($Path, 'Open', 'Read', 'ReadWrite')
    try { $fs.Seek($Lba * 512, 'Begin') | Out-Null; $b = New-Object byte[] 512; $fs.Read($b, 0, 512) | Out-Null; return $b }
    finally { $fs.Close() }
}
function PartStartLba([string]$Path) {
    $mbr = ReadSector $Path 0
    return [BitConverter]::ToUInt32($mbr, 0x1BE + 8)
}
function SameBytes([byte[]]$a, [byte[]]$b) { return [Linq.Enumerable]::SequenceEqual($a, $b) }
function AnyNonZero([byte[]]$a) { foreach ($x in $a) { if ($x -ne 0) { return $true } }; return $false }
$results = [ordered]@{}
function Result([string]$Id, [bool]$Pass, [string]$Note) {
    $results[$Id] = @{ pass = $Pass; note = $Note }
    Log ("RESULT {0}: {1} - {2}" -f $Id, $(if ($Pass) { 'PASS' } else { 'FAIL' }), $Note)
}
function Try-Check([string]$Id, [scriptblock]$Body) {
    if ($Only.Count -gt 0 -and -not ($Only | Where-Object { $Id -like "*$_*" })) { Log "skipping $Id (-Only)"; return }
    try { & $Body } catch { Result $Id $false ("exception: " + $_.Exception.Message); Log $_.ScriptStackTrace }
}

Log "rb-cli: $Rb"
Rb --version | Out-Null
Log "work: $Work"

# ---------------------------------------------------------------- D12 + D8
Try-Check 'D12/D8' {
    $v = Join-Path $Work 'd12.vhd'
    $L = New-TestVhd $v 64 NTFS
    # Fresh volumes have 8.3 creation off; the D12 scenario needs a DOS alias.
    & fsutil.exe 8dot3name set "${L}:" 0 | Add-Content $log
    Set-Content "${L}:\ThisIsALongFileName_for_D12.txt" 'hello d12'
    Set-Content "${L}:\original.txt" 'linked content'
    & cmd.exe /c "mklink /H ${L}:\hardlink.txt ${L}:\original.txt" | Add-Content $log
    DirX "${L}:\" 'd12-before' | Out-Null
    Detach-Vhd $v
    Rb mv "$v@1" /ThisIsALongFileName_for_D12.txt Renamed_After_D12_Fix.txt | Out-Null
    Rb rm "$v@1" /hardlink.txt | Out-Null
    # R-049 touches created names too: one resident, one non-resident long name.
    $small = Join-Path $Work 'created-small.txt'; 'resident' | Set-Content $small
    $bigf = Join-Path $Work 'created-big.bin'; [IO.File]::WriteAllBytes($bigf, (New-Object byte[] 100000))
    Rb put "$v@1" $small /Created_By_rb-cli_Long_Resident.txt | Out-Null
    Rb put "$v@1" $bigf /Created_By_rb-cli_Long_NonResident.bin | Out-Null
    Rb mkdir "$v@1" /Created_Long_Directory_Name | Out-Null
    $fsck = Rb fsck --checkonly "$v@1"
    $L = Attach-Vhd $v
    $code = Chkdsk $L 'd12'
    $dx = DirX "${L}:\" 'd12-after'
    $orig = Get-Content "${L}:\original.txt"
    $linkGone = -not (Test-Path "${L}:\hardlink.txt")
    Detach-Vhd $v
    $createdShown = ($dx -match 'Created_By_rb-cli_Long_Resident\.txt') -and ($dx -match 'Created_By_rb-cli_Long_NonResident\.bin') -and ($dx -match 'Created_Long_Directory_Name')
    $renamedShown = ($dx -match 'Renamed_After_D12_Fix\.txt') -and $createdShown
    $oldAliasGone = -not ($dx -match 'THISIS~1')
    Result 'D12' (($code -eq 0) -and $renamedShown -and $oldAliasGone) "chkdsk=$code renamed_listed=$renamedShown old_alias_gone=$oldAliasGone"
    Result 'D8' (($code -eq 0) -and ($orig -eq 'linked content') -and $linkGone) "chkdsk=$code original_intact=$($orig -eq 'linked content') link_gone=$linkGone"
}

# ---------------------------------------------------------------- D10
Try-Check 'D10' {
    $v = Join-Path $Work 'd10.vhd'
    $L = New-TestVhd $v 128 NTFS 64
    Set-Content "${L}:\before-resize.txt" 'd10'
    Detach-Vhd $v
    Rb partmap resize $v 1 --size 120M | Out-Null
    Rb resize "$v@1" --size 120M | Out-Null
    $fsckOk = $true
    try { Rb fsck --checkonly "$v@1" | Out-Null } catch { $fsckOk = $false; Log $_.Exception.Message }
    $start = PartStartLba $v
    $boot = ReadSector $v $start
    $total = [BitConverter]::ToInt64($boot, 0x28)
    $backup = ReadSector $v ($start + $total)
    $backupMatches = SameBytes $boot $backup
    Log "partition start LBA $start, TotalSectors $total, backup boot sector at LBA $($start + $total) matches primary: $backupMatches"
    $L = Attach-Vhd $v
    $code = Chkdsk $L 'd10'
    $size = (Get-Volume -DriveLetter $L).Size
    $file = Get-Content "${L}:\before-resize.txt"
    Detach-Vhd $v
    Result 'D10' (($code -eq 0) -and $backupMatches -and ($size -gt 100MB) -and ($file -eq 'd10')) "chkdsk=$code backup_sector_matches=$backupMatches volume_size=$size file_intact=$($file -eq 'd10') rb_fsck_clean=$fsckOk"
}

# ---------------------------------------------------------------- exFAT D1 / D5 / D7 / D9, one snapshot per step
Try-Check 'exFAT' {
    $v = Join-Path $Work 'exfat.vhd'
    $L = New-TestVhd $v 128 exFAT 64
    $rand = New-Object byte[] (1MB); (New-Object Random 42).NextBytes($rand)
    [IO.File]::WriteAllBytes("${L}:\contig.bin", $rand)
    $hash0 = (Get-FileHash "${L}:\contig.bin").Hash
    New-Item -ItemType Directory "${L}:\Many" | Out-Null
    1..5 | ForEach-Object { Set-Content "${L}:\Many\windows_wrote_$_.txt" "w$_" }
    Set-Content "${L}:\delete_me_windows.txt" ('x' * 200000)
    Detach-Vhd $v
    $s0 = Join-Path $Work 'exfat-0-windows.vhd'; Copy-Item $v $s0 -Force

    Rb mv "$v@1" /contig.bin renamed_contig.bin | Out-Null
    Rb rm "$v@1" /delete_me_windows.txt | Out-Null
    $s1 = Join-Path $Work 'exfat-1-mv-rm.vhd'; Copy-Item $v $s1 -Force

    $small = Join-Path $Work 'small.txt'
    'small' | Set-Content $small
    Log 'putting 400 long-named files into /Many ...'
    1..400 | ForEach-Object { RbQuiet put "$v@1" $small ("/Many/LongFileName_{0:d3}_padding_padding_padding_padding.txt" -f $_) }
    Log 'done'
    $s2 = Join-Path $Work 'exfat-2-puts.vhd'; Copy-Item $v $s2 -Force

    Rb partmap resize $v 1 --size 120M | Out-Null
    $resizeLog = Rb resize "$v@1" --size 120M
    $fsckOk = $true
    try { Rb fsck --checkonly "$v@1" | Out-Null } catch { $fsckOk = $false; Log $_.Exception.Message }
    $s3 = Join-Path $Work 'exfat-3-resized.vhd'; Copy-Item $v $s3 -Force

    # The theory for the RAW mount: the volume length disagreeing with the partition.
    $start = PartStartLba $v
    $boot = ReadSector $v $start
    $volLen = [BitConverter]::ToInt64($boot, 0x48)
    $s4 = Join-Path $Work 'exfat-4-partition-matched.vhd'; Copy-Item $v $s4 -Force
    RbQuiet partmap resize $s4 1 --size ($volLen * 512)
    Log "exFAT VolumeLength after resize: $volLen sectors; snapshot 4 has its partition entry set to exactly that"

    $t0 = Probe-FsType $s0 'exfat-0-windows'
    $t1 = Probe-FsType $s1 'exfat-1-mv-rm'
    $t2 = Probe-FsType $s2 'exfat-2-puts'
    $t3 = Probe-FsType $s3 'exfat-3-resized'
    $t4 = Probe-FsType $s4 'exfat-4-partition-matched'

    # Judge the edits on whichever snapshot Windows still mounts as exFAT.
    $judge = if ($t3 -eq 'exFAT') { $s3 } elseif ($t4 -eq 'exFAT') { $s4 } elseif ($t2 -eq 'exFAT') { $s2 } else { $s1 }
    Log "judging edits on $judge"
    $L = Attach-Vhd $judge
    $code = Chkdsk $L 'exfat'
    $hash1 = (Get-FileHash "${L}:\renamed_contig.bin").Hash
    $count = (Get-ChildItem "${L}:\Many" -Force).Count
    $deleted = -not (Test-Path "${L}:\delete_me_windows.txt")
    $size = (Get-Volume -DriveLetter $L).Size
    $probe = if (Test-Path "${L}:\Many\LongFileName_400_padding_padding_padding_padding.txt") { Get-Content "${L}:\Many\LongFileName_400_padding_padding_padding_padding.txt" } else { '' }
    Detach-Vhd $judge
    Result 'D1' ($hash0 -eq $hash1) "NoFatChain file hash before=$hash0 after=$hash1 (mounted: 0=$t0 1=$t1)"
    Result 'D5' (($code -eq 0) -and $deleted) "chkdsk=$code windows_file_deleted=$deleted"
    Result 'D9' (($count -eq 405) -and ($probe -eq 'small')) "Explorer sees $count of 405 entries in /Many, last file reads '$probe' (mounted: 2=$t2)"
    Result 'D7' (($t3 -eq 'exFAT') -and ($code -eq 0) -and ($size -gt 100MB)) "resized volume mounts as '$t3' (partition-matched copy: '$t4'); chkdsk=$code volume_size=$size rb_fsck_clean=$fsckOk"
}

# ---------------------------------------------------------------- D2
Try-Check 'D2' {
    $v = Join-Path $Work 'd2win.vhd'
    $L = New-TestVhd $v 64 NTFS
    Set-Content "${L}:\d2.txt" 'd2'
    Detach-Vhd $v
    $winStart = PartStartLba $v
    $winSector6 = ReadSector $v ($winStart + 6)
    $bk = Join-Path $Work 'd2bk'
    Rb backup $v $bk --name win --format raw | Out-Null
    $raw = Get-ChildItem (Join-Path $bk 'win') -Filter 'partition-*.raw' | Select-Object -First 1
    $at63 = Join-Path $Work 'd2-at63.img'
    Rb new hd mbr --size 66M --align 63s --partition rest:07 --fill "1=$($raw.FullName)" $at63 --force | Out-Null
    Rb backup $at63 $bk --name at63 --format raw | Out-Null
    $restored = Join-Path $Work 'd2-restored.img'
    if (Test-Path $restored) { Remove-Item $restored -Force }
    Rb restore (Join-Path $bk 'at63') $restored --alignment modern1mb --target-size 73400320 | Out-Null
    $rStart = PartStartLba $restored
    $rSector6 = ReadSector $restored ($rStart + 6)
    $bootCodeKept = (SameBytes $winSector6 $rSector6) -and (AnyNonZero $rSector6)
    Log "restored partition starts at LBA $rStart; sector 6 identical to Windows' original and non-zero: $bootCodeKept"
    $fsckOk = $true
    try { Rb fsck --checkonly "$restored@1" | Out-Null } catch { $fsckOk = $false; Log $_.Exception.Message }
    $out = Join-Path $Work 'd2out'
    Rb convert $restored $out --format vhd --overwrite | Out-Null
    $rv = Get-ChildItem $out -Filter '*.vhd' | Select-Object -First 1
    $L = Attach-Vhd $rv.FullName
    $code = Chkdsk $L 'd2'
    $file = Get-Content "${L}:\d2.txt"
    Detach-Vhd $rv.FullName
    Result 'D2' (($rStart -eq 2048) -and $bootCodeKept -and ($code -eq 0) -and ($file -eq 'd2')) "restored_lba=$rStart boot_code_kept=$bootCodeKept chkdsk=$code file_intact=$($file -eq 'd2') rb_fsck_clean=$fsckOk"
}

# ---------------------------------------------------------------- D13 (optional)
if ($Ghost) {
    Try-Check 'D13' {
        $out = Join-Path $Work 'ghost'
        Rb convert $Ghost $out --format vhd --overwrite | Out-Null
        $gv = Get-ChildItem $out -Filter '*.vhd' | Select-Object -First 1
        Mount-DiskImage -ImagePath $gv.FullName | Out-Null
        Start-Sleep 2
        $disk = Get-VhdDisk $gv.FullName
        $codes = @()
        foreach ($p in ($disk | Get-Partition | Where-Object { $_.Type -ne 'Reserved' })) {
            if (-not $p.DriveLetter) { $p | Add-PartitionAccessPath -AssignDriveLetter | Out-Null; Start-Sleep 1; $p = Get-Partition -DiskNumber $disk.Number -PartitionNumber $p.PartitionNumber }
            $l = [string]$p.DriveLetter
            DirX "${l}:\" "d13-$l" | Out-Null
            $codes += Chkdsk $l "d13-$l"
        }
        Detach-Vhd $gv.FullName
        Result 'D13' (($codes | Where-Object { $_ -ne 0 }).Count -eq 0) "chkdsk codes: $($codes -join ',') - compare the dir /x listings by eye for duplicate 8.3 aliases"
    }
} else {
    Log 'D13 skipped: no -Ghost image given'
}

Log '================ SUMMARY ================'
foreach ($k in $results.Keys) { Log ("{0,-8} {1}  {2}" -f $k, $(if ($results[$k].pass) { 'PASS' } else { 'FAIL' }), $results[$k].note) }
$results | ConvertTo-Json -Depth 3 | Set-Content (Join-Path $Work 'results.json')
Log "results in $Work"
