# crusty-backup (cb-dos) -- DJGPP cross-build.
#
# Requires the DJGPP cross-compiler. Default path matches the local install
# (~/djgpp); override with `make DJGPP=/path/to/djgpp` or set CC directly.
#
#   make            # build all POC binaries into build/
#   make size       # build + report .exe sizes (the Phase 0a measurement)
#   make clean

DJGPP ?= $(HOME)/djgpp
# Direct assignment (not ?=): make pre-defines CC=cc, which ?= would not override.
# A command-line `make CC=...` still wins over this.
CC    := $(DJGPP)/bin/i586-pc-msdosdjgpp-gcc

CFLAGS  ?= -Os -Wall -Wextra -fno-ident
LDFLAGS ?= -s            # strip symbols (smaller .exe)

BUILD := build

POCS := $(BUILD)/crustybk.exe $(BUILD)/disk_spike.exe $(BUILD)/lfn_test.exe

all: $(POCS)

$(BUILD)/%.exe: src/%.c | $(BUILD)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

$(BUILD):
	mkdir -p $(BUILD)

size: all
	@echo "=== cb-dos POC .exe sizes ==="
	@ls -l $(POCS) | awk '{printf "%8d bytes  %s\n", $$5, $$9}'

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

.PHONY: all size net clean
