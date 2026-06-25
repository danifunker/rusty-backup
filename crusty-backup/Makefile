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

LZ4     := deps/lz4
LZ4INC  := $(LZ4)/inc
LZ4LIB  := $(LZ4)/lib

WATT32  := net/watt32
WATTINC := $(WATT32)/inc
WATTLIB := $(WATT32)/lib

# --- The unified cb-dos tool ----------------------------------------------
# One executable: the text UI when run bare, plus backup / restore / clone /
# inspect subcommands, all over the shared cbdisk engine. Needs the DJGPP zlib
# (gzip codec) + liblz4 (LZ4 frame codec) + WATT-32 (networked backup over a
# packet driver) static libs, all cross-built/fetched separately and gitignored
# -- run `sh deps/fetch-zlib.sh`, `sh deps/fetch-lz4.sh`, and
# `sh net/fetch-watt32.sh` once.
CB_SRCS := src/crustybk.c src/cmd_backup.c src/cmd_restore.c src/cmd_clone.c \
           src/cmd_inspect.c src/cmd_browse.c src/cbdisk.c src/cbntfs.c \
           src/cbdefrag.c src/cbcodec.c src/cbnet.c src/cbmanifest.c
CB_HDRS := src/cbdisk.h src/cbbrowse.h src/cbntfs.h src/cbdefrag.h src/cbcodec.h \
           src/cbnet.h src/cbmanifest.h

# Diagnostics POCs (no zlib): single-source, built by the generic rule below.
DIAG := $(BUILD)/disk_spike.exe $(BUILD)/lfn_test.exe

crustybk: $(BUILD)/crustybk.exe
all: crustybk $(DIAG)

$(BUILD)/crustybk.exe: $(CB_SRCS) $(CB_HDRS) | $(BUILD)
	@test -f $(ZLIBLIB)/libz.a || { \
	  echo "zlib not built -- run: sh deps/fetch-zlib.sh"; exit 1; }
	@test -f $(LZ4LIB)/liblz4.a || { \
	  echo "lz4 not built -- run: sh deps/fetch-lz4.sh"; exit 1; }
	@test -f $(WATTLIB)/libwatt.a || { \
	  echo "WATT-32 not found -- run: sh net/fetch-watt32.sh"; exit 1; }
	$(CC) $(CFLAGS) -I$(ZLIBINC) -I$(LZ4INC) -I$(WATTINC) $(LDFLAGS) -o $@ $(CB_SRCS) \
	  -L$(ZLIBLIB) -lz -L$(LZ4LIB) -llz4 -L$(WATTLIB) -lwatt

# Generic single-source rule (the diagnostics).
$(BUILD)/%.exe: src/%.c | $(BUILD)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

$(BUILD):
	mkdir -p $(BUILD)

size: all
	@echo "=== cb-dos .exe sizes ==="
	@ls -l $(BUILD)/crustybk.exe $(DIAG) | awk '{printf "%8d bytes  %s\n", $$5, $$9}'

# --- Networked handshake diagnostic (WATT-32) -----------------------------
# The networked *backup* is baked into CRUSTYBK (`backup rb://...`). NETHELLO is
# a standalone handshake probe kept for diagnostics. Both link WATT-32 (DJGPP
# BSD sockets over a packet driver) -- see docs/cb_dos_network_and_state.md
# (§1b) and docs/cb_dos_networking.md. Run `sh net/fetch-watt32.sh` once.
net: $(BUILD)/nethello.exe

$(BUILD)/nethello.exe: src/net_hello.c | $(BUILD)
	@test -f $(WATTLIB)/libwatt.a || { \
	  echo "WATT-32 not found -- run: sh net/fetch-watt32.sh"; exit 1; }
	$(CC) $(CFLAGS) -I$(WATTINC) $(LDFLAGS) -o $@ $< -L$(WATTLIB) -lwatt

clean:
	rm -rf $(BUILD)

.PHONY: all crustybk size net clean
