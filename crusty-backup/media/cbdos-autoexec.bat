@echo off
SET DOSDIR=A:\FREEDOS
SET PATH=%DOSDIR%\BIN;%DOSDIR%\LINKS
SET NLSPATH=%DOSDIR%\NLS
SET HELPPATH=%DOSDIR%\HELP
SET LANG=EN
alias reboot=fdapm warmboot
alias halt=fdapm poweroff
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
