#!/bin/sh
# solaris-usb-unblock.sh -- diagnose a USB disk that has wedged Solaris' scsa2usb driver.
#
#   scripts/solaris-usb-unblock.sh            report only (default, touches nothing)
#   scripts/solaris-usb-unblock.sh --apply    also write the scsa2usb reduced-command
#                                             override and reload the driver
#
# Run this on the Solaris machine. Many cheap flash bridges do not implement the full SCSI
# command set scsa2usb issues, and then wedge on open(2) -- ignoring O_NONBLOCK, leaving
# processes unkillable. `format` and `rmformat` hang on such a device too, so everything here
# reads published data instead: nothing below opens a disk.
#
# See docs/solaris-raw-devices.md.
set -eu

APPLY=0
[ "${1:-}" = "--apply" ] && APPLY=1

CONF=/kernel/drv/scsa2usb.conf
OVERRIDE='attribute-override-list = "vid=* reduced-cmd-support=true";'

say() { printf '==> %s\n' "$*"; }

say "Disks the drivers are publishing (no device is opened)"
# `kstat -p` is module:instance:name:statistic <TAB> value, so the value is everything
# after the tab -- splitting on ':' as well would cut a product string containing one.
kstat -p 'sderr:::' 2>/dev/null | awk -F'\t' '
    { n = split($1, k, ":"); stat = k[n]; inst = k[2] }
    stat == "Size"    { size[inst] = $2 }
    stat == "Vendor"  { vend[inst] = $2 }
    # Some drivers run the product field into the next INQUIRY label; drop the artifact.
    stat == "Product" { sub(/ *Revision$/, "", $2); prod[inst] = $2 }
    END {
        for (i in size)
            printf "    inst %-4s %-28s %s bytes%s\n", i, vend[i] " " prod[i], size[i],
                   (size[i] == 0 ? "   <-- NOT ANSWERING" : "")
    }' | sort

say "Processes stuck on a raw disk"
# A process wedged in the driver cannot be killed; it clears when the device is unplugged.
stuck=$(ps -ef | grep '[/]dev/rdsk' | awk '{print "    pid " $2 "  " $8 " " $9}')
if [ -n "$stuck" ]; then
    printf '%s\n' "$stuck"
    echo "    (these are in uninterruptible sleep -- kill -9 will not clear them)"
else
    echo "    none"
fi

say "USB mass storage the kernel has attached"
dmesg 2>/dev/null | grep -i 'scsa2usb\|USB-device' | tail -4 | sed 's/^/    /' || echo "    none"

say "scsa2usb command-set override"
if grep -q 'reduced-cmd-support' "$CONF" 2>/dev/null; then
    echo "    already set in $CONF"
elif [ "$APPLY" -eq 1 ]; then
    # Documented remedy for non-compliant devices; see the Oracle link in the docs.
    cp "$CONF" "$CONF.bak.$$"
    echo "$OVERRIDE" >> "$CONF"
    echo "    appended to $CONF (backup at $CONF.bak.$$)"
    if update_drv -f scsa2usb 2>&1; then
        echo "    driver reloaded"
    else
        echo "    reload failed -- the driver is busy while processes remain stuck" >&2
    fi
else
    echo "    not set. To apply the documented remedy for non-compliant devices:"
    echo
    echo "        echo '$OVERRIDE' >> $CONF"
    echo "        update_drv -f scsa2usb"
    echo
    echo "    Re-run with --apply to do that. It affects ALL USB storage on this machine,"
    echo "    a bad .conf is only noticed at the next boot, and the driver cannot reload"
    echo "    while processes are still stuck holding it open."
fi

say "If the device stays wedged"
echo "    Unplug it. That is the only reliable recovery, and it clears the stuck processes."
