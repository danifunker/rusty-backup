@echo off
SET DOSDIR=A:\FREEDOS
SET PATH=%DOSDIR%\BIN;%DOSDIR%\LINKS
SET NLSPATH=%DOSDIR%\NLS
SET HELPPATH=%DOSDIR%\HELP
SET LANG=EN
alias reboot=fdapm warmboot
alias halt=fdapm poweroff
REM Long-filename driver: cb-dos backup folders use non-8.3 names
REM (metadata.json / partition-N.gz / manifest-N.json), so load DOSLFN before
REM any local-folder backup/restore -- without it those names truncate and
REM collide on the FreeDOS kernel (which has no LFN API of its own). DOSLFN.COM
REM ships at the media root next to CRUSTYBK.EXE (see DOSLFN-ATTRIBUTION.md).
DOSLFN
SET LFN=Y
cls
REM Boot straight into the crusty-backup text UI.
CRUSTYBK.EXE
echo.
echo   crusty-backup (cb-dos) -- the menu has exited.
echo     CRUSTYBK   relaunch the backup/restore menu
echo     DISKSPK    int13h disk dump (MBR + FAT/NTFS/exFAT) -- diagnostics
echo     LFNTEST    long-filename write test -- diagnostics
echo.
echo   See docs/cb_dos.md in the rusty-backup repo.
echo.
