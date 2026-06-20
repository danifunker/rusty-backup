@ECHO OFF
REM ===================================================================
REM  rusty-backup / cb-dos networking helper
REM ===================================================================
REM  STEP 1 - load YOUR network card's packet driver FIRST, on soft-int 60h.
REM           See \NET\DRIVERS\DRIVERS.TXT for the driver + arguments for
REM           your card. Examples:
REM             NE2000 clone, I/O 300h, IRQ 3:  \NET\DRIVERS\NE2000 0x60 3 0x300
REM             3Com 3C509 (self-configuring): \NET\DRIVERS\3C5X9PD 0x60
REM  STEP 2 - run this batch (NET) to pull an IP and see what's next.
REM ===================================================================
SET MTCPCFG=\NET\MTCP.CFG
ECHO Requesting an IP address via DHCP...
\NET\DHCP
ECHO.
ECHO If that succeeded you are online. To move a backup image off this box:
ECHO   \NET\FTPSRV         - run an FTP server HERE; pull the file from another PC
ECHO   \NET\FTP ^<host^>     - connect out and PUT the backup to an FTP server
ECHO   \NET\PING ^<host^>    - test the connection
ECHO.
ECHO No network card? You can still go over a serial cable with SLIP -
ECHO see docs/cb_dos_networking.md.
