@echo off
REM NE2000-compatible ISA NIC. ISA cards can't be auto-probed safely, so we load
REM NE2000.COM at the common default (I/O 0x300, IRQ 3). If your card differs,
REM press Ctrl-C and load it by hand, e.g.:  \NET\DRIVERS\NE2000 0x60 5 0x320
IF NOT EXIST \NET\DRIVERS\NE2000.COM GOTO MISSING
echo Loading NE2000.COM  (packet-int 0x60, IRQ 3, I/O 0x300) ...
echo   Wrong settings? Ctrl-C, then:  \NET\DRIVERS\NE2000 0x60 ^<irq^> ^<io^>
\NET\DRIVERS\NE2000.COM 0x60 3 0x300
echo Network driver loaded. IP is via DHCP (A:\WATTCP.CFG) at backup time.
GOTO DONE
:MISSING
echo NE2000.COM is not on A:\NET\DRIVERS -- add it (see \NET\DRIVERS\DRIVERS.TXT).
:DONE
