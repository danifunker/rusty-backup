@echo off
REM PCI/onboard NIC auto-setup. Runs crustybk's PCI detector; the exit code
REM picks the packet driver, which we load from \NET\DRIVERS (int 60h) so a
REM later "CRUSTYBK backup rb://..." finds it. Re-run NET after copying a
REM driver from the CD. Keep the codes in sync with src/cmd_netdetect.c.
CRUSTYBK.EXE netdetect
SET NICDRV=
IF ERRORLEVEL 8 IF NOT ERRORLEVEL 9 SET NICDRV=DC21X4
IF ERRORLEVEL 7 IF NOT ERRORLEVEL 8 SET NICDRV=E100BPKT
IF ERRORLEVEL 6 IF NOT ERRORLEVEL 7 SET NICDRV=3C90XPD
IF ERRORLEVEL 5 IF NOT ERRORLEVEL 6 SET NICDRV=R6040PD
IF ERRORLEVEL 4 IF NOT ERRORLEVEL 5 SET NICDRV=PCNTPK
IF ERRORLEVEL 3 IF NOT ERRORLEVEL 4 SET NICDRV=R8169PD
IF ERRORLEVEL 2 IF NOT ERRORLEVEL 3 SET NICDRV=RTSPKT
IF ERRORLEVEL 1 IF NOT ERRORLEVEL 2 SET NICDRV=NE2000
IF "%NICDRV%"=="" GOTO NONE
IF NOT EXIST \NET\DRIVERS\%NICDRV%.COM GOTO MISSING
echo Loading %NICDRV%.COM on packet-int 0x60 ...
\NET\DRIVERS\%NICDRV%.COM 0x60
echo Network driver loaded. IP is via DHCP (A:\WATTCP.CFG) at backup time.
GOTO DONE
:MISSING
echo.
echo   %NICDRV%.COM is not on A:\NET\DRIVERS. Copy it from the CD (see the
echo   message above), then run NET again.
GOTO DONE
:NONE
echo.
echo   No supported PCI NIC auto-detected. If you have an ISA card, reboot
echo   and choose option 2 (NE2000), or load a driver from \NET\DRIVERS by hand.
:DONE
SET NICDRV=
