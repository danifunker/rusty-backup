# Dropping new fixtures

Put anything you find in:

```
\\NAS\share\rb-fixtures\new\
```

No naming convention needed — original filenames are fine, and keeping the
original name actually helps because provenance often lives in it. Drop
`.bin`/`.cue` pairs together, and leave archives (`.zip`, `.7z`) intact if
that is how you found them.

I sort each drop by: opening it with `rb-cli`, working out which gap it
fills, minimising it if it is oversized, giving it a logical ID, moving it
into `fixtures/`, and adding a catalogue row. Anything that turns out to be
a duplicate or that no verb can open goes back out with a note.

---

## What a High Sierra CD is

**Short version: the draft that became ISO 9660.** In November 1985 a group
of CD-ROM vendors met at the High Sierra Hotel at Lake Tahoe to agree a
common disc format, because every early CD-ROM shipped with its own
incompatible layout. The result — the "High Sierra Format" — was published
in 1986, then standardised with small changes as **ISO 9660 in 1988**.

So High Sierra discs are a narrow window: **roughly 1986 through 1989.**
After that essentially everything is ISO 9660. This is why they are hard to
find and why nothing in USBODE or winworld matched — those collections are
mostly 1990s material.

**What they look like:** early reference and text titles. Grolier Electronic
Encyclopedia, Microsoft Bookshelf, Microsoft Programmer's Library, early
Bureau of Electronic Publishing discs, PC-SIG shareware collections, early
library and government discs. Anything with a 1986-1989 copyright and a
CD-ROM caddy.

**How to tell without special tools.** Both formats put a volume descriptor
at sector 16 (byte 32768), but the magic sits at a different offset:

```bash
dd if=disc.iso bs=1 skip=32768 count=16 2>/dev/null | xxd
```

| You see | Format |
|---------|--------|
| `.CD001.` — `CD001` at byte **32769** | ISO 9660 |
| eight bytes, then `CDROM` at byte **32777** | **High Sierra** |
| all zeroes | neither — some other filesystem (our IRIX EFS disc looks like this) |

Verified against a known-good ISO 9660 disc in our corpus, which dumps as
`0143 4430 3031 01` = type byte `01`, `CD001`, version `01`. High Sierra
carries an 8-byte logical sector number *before* the type byte, which is what
pushes its `CDROM` identifier out to offset 9 within the descriptor.

We do implement High Sierra (`F::HighSierra` / `pvd.high_sierra` in
`src/cli/verbs/optical.rs`), so this is purely a missing fixture — the code
is there and untested.

If you find a candidate, that `dd` one-liner confirms it in a second. A disc
that says `CD001` is not useful to us; we have plenty of those.

---

## Corrections to the earlier shopping list

Two items were wrong, and one source turned out to be tools rather than data.

### Solaris install CDs do not give us optical UFS

I asked for a Solaris CD to close the optical-UFS row. I probed three
(`Solaris 8 x86`, `Solaris 2.6 i386`, `Solaris 1.1.2 / SunOS 4.1.4 SPARC`)
and **all three report plain `iso9660`.** That is correct behaviour — Sun
shipped install media as ISO 9660; the UFS lives on the *installed disk*,
not the CD.

So the request splits in two:

- **Optical UFS** — the canonical case is a **NeXTSTEP / OPENSTEP install
  CD**, which really is UFS-formatted. Failing that, an SGI IRIX disc (we
  already have EFS covered) or a BSD live CD that uses UFS on the disc.
- **Sun disk label + UFS on disk** — needs an *installed* Solaris/SunOS
  **disk image**, not a CD. A small SPARC or x86 VM disk would do it, and
  would close the Sun-disk-label partition row at the same time.

### QXL.WIN — the archive has tools, not containers

The Sinclair QL page you linked lists QXL.WIN *utilities*, not sample
containers: `qxlformat.zip` (formatter), `qxl.fschk.zip` (checker),
`qwe091.zip` (QXLWIN Explorer), `rcx-e.zip` (RecoverX), `wxqt2.zip`,
and `qxltool.zip`.

`qxltool` is the interesting one — it reads *and writes* QXL.WIN filesystems
and runs on modern hosts, so the cheapest path to a fixture is probably to
**generate a small QXL.WIN with qxltool** rather than hunt for one in the
wild. That also gives us a minimal container instead of somebody's full
hard-disk image. Worth trying before sourcing.

A real-world QXL.WIN is still preferable as a second fixture if one turns up
inside a QPC2 or QXL distribution.
