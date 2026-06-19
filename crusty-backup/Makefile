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

POCS := $(BUILD)/tui_poc.exe $(BUILD)/disk_spike.exe $(BUILD)/lfn_test.exe

all: $(POCS)

$(BUILD)/%.exe: src/%.c | $(BUILD)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

$(BUILD):
	mkdir -p $(BUILD)

size: all
	@echo "=== cb-dos POC .exe sizes ==="
	@ls -l $(POCS) | awk '{printf "%8d bytes  %s\n", $$5, $$9}'

clean:
	rm -rf $(BUILD)

.PHONY: all size clean
