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
REM any local-folder backup/restore. DOSLFN.COM ships at the media root.
DOSLFN
SET LFN=Y
cls
REM --- CD-ROM redirector: assign a drive letter to whichever CD bus loaded a
REM     device (OAKCDROM=MSCD001 / USB=MSCD002). Skipped on the no-CD option. ---
IF "%CONFIG%"=="BARE" GOTO NOCD
SHSUCDX.COM /D:MSCD001 /D:MSCD002
:NOCD
REM --- Networking, per the boot-menu choice (%CONFIG%) ---
IF "%CONFIG%"=="PCI" CALL A:\NET.BAT
IF "%CONFIG%"=="ISA" CALL A:\NETISA.BAT
echo.
REM --- Launch the backup/restore UI ---
CRUSTYBK.EXE
echo.
echo   crusty-backup (cb-dos) -- the menu has exited.
echo     CRUSTYBK                         relaunch the backup/restore menu
echo     CRUSTYBK backup rb://host/name   back up over the network
echo     NET                              re-detect/load the PCI network driver
echo     NETISA                           load an NE2000-compatible ISA driver
echo.
echo   CD-ROM (when loaded) is the drive SHSUCDX assigned above.
echo   See docs/cb_dos.md in the rusty-backup repo.
echo.
