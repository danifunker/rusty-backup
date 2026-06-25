# crusty-backup (cb-dos) -- DJGPP cross-build.
#
# Requires the DJGPP cross-compiler. Default path matches the local install
# (~/djgpp); override with `make DJGPP=/path/to/djgpp` or set CC directly.
#
#   make            # build the unified CRUSTYBK.EXE + diagnostics into build/
#   make crustybk   # just the unified tool (TUI + backup/restore/clone/inspect)
#   make size       # build + report .exe sizes
#   make clean

DJGPP ?= $(HOME)/djgpp
# Direct assignment (not ?=): make pre-defines CC=cc, which ?= would not override.
# A command-line `make CC=...` still wins over this.
CC    := $(DJGPP)/bin/i586-pc-msdosdjgpp-gcc

CFLAGS  ?= -Os -Wall -Wextra -fno-ident
LDFLAGS ?= -s            # strip symbols (smaller .exe)

BUILD := build

ZLIB    := deps/zlib
ZLIBINC := $(ZLIB)/inc
ZLIBLIB := $(ZLIB)/lib

# --- The unified cb-dos tool ----------------------------------------------
# One executable: the text UI when run bare, plus backup / restore / clone /
# inspect subcommands, all over the shared cbdisk engine. Needs the DJGPP zlib
# static lib (gzip codec for backup/restore), cross-built separately and
# gitignored -- run `sh deps/fetch-zlib.sh` once.
CB_SRCS := src/crustybk.c src/cmd_backup.c src/cmd_restore.c src/cmd_clone.c \
           src/cmd_inspect.c src/cmd_browse.c src/cbdisk.c
CB_HDRS := src/cbdisk.h src/cbbrowse.h

# Diagnostics POCs (no zlib): single-source, built by the generic rule below.
DIAG := $(BUILD)/disk_spike.exe $(BUILD)/lfn_test.exe

crustybk: $(BUILD)/crustybk.exe
all: crustybk $(DIAG)

$(BUILD)/crustybk.exe: $(CB_SRCS) $(CB_HDRS) | $(BUILD)
	@test -f $(ZLIBLIB)/libz.a || { \
	  echo "zlib not built -- run: sh deps/fetch-zlib.sh"; exit 1; }
	$(CC) $(CFLAGS) -I$(ZLIBINC) $(LDFLAGS) -o $@ $(CB_SRCS) -L$(ZLIBLIB) -lz

# Generic single-source rule (the diagnostics).
$(BUILD)/%.exe: src/%.c | $(BUILD)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

$(BUILD):
	mkdir -p $(BUILD)

size: all
	@echo "=== cb-dos .exe sizes ==="
	@ls -l $(BUILD)/crustybk.exe $(DIAG) | awk '{printf "%8d bytes  %s\n", $$5, $$9}'

# --- Networked client (WATT-32) -------------------------------------------
# Not part of `all`: it needs the WATT-32 DJGPP package, fetched separately
# (BSD-licensed, gitignored). Run `sh net/fetch-watt32.sh` once, then
# `make net`. WATT-32 gives DJGPP BSD sockets over a packet driver -- see
# docs/cb_dos_network_and_state.md (§1b) and docs/cb_dos_networking.md.
WATT32  := net/watt32
WATTINC := $(WATT32)/inc
WATTLIB := $(WATT32)/lib

net: $(BUILD)/nethello.exe

$(BUILD)/nethello.exe: src/net_hello.c | $(BUILD)
	@test -f $(WATTLIB)/libwatt.a || { \
	  echo "WATT-32 not found -- run: sh net/fetch-watt32.sh"; exit 1; }
	$(CC) $(CFLAGS) -I$(WATTINC) $(LDFLAGS) -o $@ $< -L$(WATTLIB) -lwatt

clean:
	rm -rf $(BUILD)

.PHONY: all crustybk size net clean
