@echo off
SET DOSDIR=A:\FREEDOS
SET PATH=%DOSDIR%\BIN;%DOSDIR%\LINKS
SET NLSPATH=%DOSDIR%\NLS
SET HELPPATH=%DOSDIR%\HELP
SET LANG=EN
alias reboot=fdapm warmboot
alias halt=fdapm poweroff
cls
echo.
echo   crusty-backup (cb-dos) -- DOS backup/restore tools
echo   ==================================================
echo.
echo   Tools on this disk (type the name at the A: prompt):
echo     TUI_POC    text UI preview (Phase 0a)
echo     DISKSPK    int13h disk spike: dump MBR + FAT/NTFS/exFAT
echo     LFNTEST    long-filename write test
echo.
echo   cb-dos is in development - the backup engine is still being built.
echo   See docs/cb_dos.md in the rusty-backup repo.
echo.
