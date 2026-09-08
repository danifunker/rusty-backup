// MacAtrium headless Snow harness.
//
// Boots a Macintosh II (ROM + Macintosh Display Card 8*24 ROM) with a SCSI hard
// disk attached, runs for a cycle budget, periodically dumps the framebuffer to
// PNG, and can tap a scripted sequence of keys at given cycle marks. This is the
// no-GUI observation path for verifying the launch-return keystone and the MVP
// launcher (the dev machine has no display server).
//
// Usage:
//   macatrium_harness <rom> <mdc_rom> <hdd.img> <out_dir> <max_cycles> \
//       [--snap-every N] [--keys "CYCLE:KEY;CYCLE:KEY;..."] [--wall-secs S] \
//       [--pram FILE] [--disk2 HDD2] [--floppy IMG[@CYCLE]]...
//
// KEY names: letters, digits, dot minus slash delete enter return esc up down
// left right space tab (lowercase); `type@TEXT` taps a whole string.
// A click is scheduled with KEY = `click@X,Y` (absolute framebuffer pixels), e.g.
//   --keys "2500000000:click@320,160;3000000000:click@600,300"
// A press-and-hold (for auto-repeat) is KEY = `hold@X,Y,DUR` — button down DUR
// cycles before release, e.g. holding a scroll arrow:  4000000:hold@621,223,40000000
//
// --pram FILE persists PRAM in FILE (created if absent). Requires the harness to
// be built with snow_core's `mmap` feature; without it persist_pram cannot write
// back (and SCSI disks are read-only too).

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Result, bail};

use snow_core::emulator::Emulator;
use snow_core::emulator::comm::{EmulatorCommand, EmulatorEvent, EmulatorSpeed};
use snow_core::keymap::{KeyEvent, Keymap};
use snow_core::mac::{ExtraROMs, MacModel};
use snow_core::tickable::Tickable;

/// A scheduled input action fired at a given cycle mark.
#[derive(Clone, Copy)]
enum Act {
    Key(u8, bool),      // scancode, is-down
    MouseAbs(u16, u16), // warp cursor to absolute framebuffer pixel (x, y)
    MouseBtn(bool),     // mouse button down/up (position unchanged)
}

fn scancode(name: &str) -> Option<u8> {
    Some(match name {
        "a" => 0x00, "s" => 0x01, "d" => 0x02, "f" => 0x03, "h" => 0x04,
        "g" => 0x05, "z" => 0x06, "x" => 0x07, "c" => 0x08, "v" => 0x09,
        "b" => 0x0B, "q" => 0x0C, "w" => 0x0D, "e" => 0x0E, "r" => 0x0F,
        "y" => 0x10, "t" => 0x11, "o" => 0x1F, "u" => 0x20, "i" => 0x22,
        "p" => 0x23, "l" => 0x25, "j" => 0x26, "k" => 0x28, "n" => 0x2D,
        "m" => 0x2E,
        // digits and the punctuation a file name needs (ADB scancodes)
        "1" => 0x12, "2" => 0x13, "3" => 0x14, "4" => 0x15, "5" => 0x17,
        "6" => 0x16, "7" => 0x1A, "8" => 0x1C, "9" => 0x19, "0" => 0x1D,
        "dot" | "." => 0x2F, "minus" | "-" => 0x1B, "slash" => 0x2C,
        "delete" | "backspace" => 0x33,
        "space" => 0x31,
        "tab" => 0x30,
        "enter" | "return" => 0x24,
        "esc" => 0x35,
        "up" => 0x3E, "down" => 0x3D, "left" => 0x3B, "right" => 0x3C,
        _ => return None,
    })
}

fn write_png(path: &str, w: u16, h: u16, rgba: &[u8]) -> Result<()> {
    let mut enc = png::Encoder::new(File::create(path)?, w as u32, h as u32);
    // A 1-bit screen (Snow paints it with two greys, 0x22 and 0xEE) packs to a
    // 1-bit grayscale PNG: ~10 KB instead of ~650 KB, small enough to keep as evidence.
    let mut seen = std::collections::BTreeSet::new();
    for p in rgba.chunks_exact(4) {
        seen.insert((p[0], p[1], p[2]));
        if seen.len() > 2 { break; }
    }
    let bw = seen.len() <= 2 && seen.iter().all(|c| c.0 == c.1 && c.1 == c.2);
    if bw {
        let (w, h) = (w as usize, h as usize);
        let stride = w.div_ceil(8);
        let mut packed = vec![0u8; stride * h];
        for y in 0..h {
            for x in 0..w {
                if rgba[(y * w + x) * 4] >= 128 {
                    packed[y * stride + x / 8] |= 0x80 >> (x % 8);
                }
            }
        }
        enc.set_color(png::ColorType::Grayscale);
        enc.set_depth(png::BitDepth::One);
        let mut wr = enc.write_header()?;
        wr.write_image_data(&packed)?;
        // A PBM (P4, 1 = black) next to it: trivial for a script to parse and
        // search for a known dialog string without a PNG decoder.
        if let Some(stem) = path.strip_suffix(".png") {
            let mut pbm = format!("P4\n{w} {h}\n").into_bytes();
            pbm.extend(packed.iter().map(|b| !b));
            fs::write(format!("{stem}.pbm"), pbm)?;
        }
        return Ok(());
    }

    enc.set_color(png::ColorType::Rgba);
    enc.set_depth(png::BitDepth::Eight);
    let mut wr = enc.write_header()?;
    wr.write_image_data(rgba)?;
    Ok(())
}

fn main() -> Result<()> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let a: Vec<String> = std::env::args().collect();
    if a.len() < 6 {
        bail!("usage: macatrium_harness <rom> <mdc_rom> <hdd> <out_dir> <max_cycles> [--snap-every N] [--keys CYCLE:KEY;...] [--wall-secs S]");
    }
    let rom_path = &a[1];
    let mdc_path = &a[2];
    let hdd_path = &a[3];
    let out_dir = &a[4];
    let max_cycles: u64 = a[5].parse()?;

    let mut snap_every: u64 = 100_000_000;
    let mut wall_secs: u64 = 1800;
    let mut pram_path: Option<String> = None;
    let mut disk2: Option<String> = None; // 2nd SCSI disk (docs/37 multi-disk verify)
    // Floppies to insert into drive 0: (cycle, path). `--floppy IMG[@CYCLE]`; a
    // CYCLE of 0 inserts right after the emulator starts (before the ROM boots).
    let mut floppies: Vec<(u64, String)> = Vec::new();
    // schedule[cycle] = input actions due at that cycle
    let mut schedule: BTreeMap<u64, Vec<Act>> = BTreeMap::new();
    let mut i = 6;
    while i < a.len() {
        match a[i].as_str() {
            "--snap-every" => { snap_every = a[i + 1].parse()?; i += 2; }
            "--wall-secs"  => { wall_secs  = a[i + 1].parse()?; i += 2; }
            "--pram"       => { pram_path  = Some(a[i + 1].clone()); i += 2; }
            "--disk2"      => { disk2      = Some(a[i + 1].clone()); i += 2; }
            "--floppy" => {
                let spec = &a[i + 1];
                let (path, cyc) = match spec.rsplit_once('@') {
                    Some((p, c)) if c.chars().all(|ch| ch.is_ascii_digit()) && !c.is_empty() => (p.to_string(), c.parse::<u64>()?),
                    _ => (spec.clone(), 0),
                };
                floppies.push((cyc, path));
                i += 2;
            }
            "--keys" => {
                const CMD: u8 = 0x37; // Command (universal scancode)
                const OPT: u8 = 0x3A; // Option
                for tok in a[i + 1].split(';').filter(|s| !s.is_empty()) {
                    let (c, k) = tok.split_once(':').expect("CYCLE:KEY");
                    let cyc: u64 = c.parse()?;
                    if let Some(coords) = k.strip_prefix("click@") {
                        // Mouse click at absolute framebuffer (x,y): warp the cursor,
                        // then press + release the button a few ms apart.
                        let (xs, ys) = coords.split_once(',').expect("click@X,Y");
                        let x: u16 = xs.parse()?;
                        let y: u16 = ys.parse()?;
                        schedule.entry(cyc).or_default().push(Act::MouseAbs(x, y));
                        schedule.entry(cyc + 1_000_000).or_default().push(Act::MouseBtn(true));
                        schedule.entry(cyc + 3_000_000).or_default().push(Act::MouseBtn(false));
                    } else if let Some(coords) = k.strip_prefix("hold@") {
                        // Press-and-hold at (x,y) for DUR cycles, then release. Drives
                        // hold-to-scroll auto-repeat (a scroll-arrow held down) — the
                        // Control Manager fires the control's action proc the whole time.
                        //   hold@X,Y,DUR
                        let mut it = coords.split(',');
                        let x: u16 = it.next().expect("hold@X,Y,DUR").parse()?;
                        let y: u16 = it.next().expect("hold@X,Y,DUR").parse()?;
                        let dur: u64 = it.next().expect("hold@X,Y,DUR").parse()?;
                        schedule.entry(cyc).or_default().push(Act::MouseAbs(x, y));
                        schedule.entry(cyc + 1_000_000).or_default().push(Act::MouseBtn(true));
                        schedule.entry(cyc + 1_000_000 + dur).or_default().push(Act::MouseBtn(false));
                    } else if let Some(coords) = k.strip_prefix("drag@") {
                        // Press at (x1,y1), warp the cursor through to (x2,y2) while held,
                        // then release — drives a click-drag (e.g. a column divider). The
                        // guest's StillDown/GetMouse loop tracks the intermediate warps.
                        //   drag@X1,Y1,X2,Y2
                        let mut it = coords.split(',');
                        let x1: u16 = it.next().expect("drag@X1,Y1,X2,Y2").parse()?;
                        let y1: u16 = it.next().expect("drag@X1,Y1,X2,Y2").parse()?;
                        let x2: u16 = it.next().expect("drag@X1,Y1,X2,Y2").parse()?;
                        let y2: u16 = it.next().expect("drag@X1,Y1,X2,Y2").parse()?;
                        schedule.entry(cyc).or_default().push(Act::MouseAbs(x1, y1));
                        schedule.entry(cyc + 1_000_000).or_default().push(Act::MouseBtn(true));
                        let steps = 8u64;
                        for s in 1..=steps {                 // glide from p1 to p2 while held
                            let x = x1 as i64 + (x2 as i64 - x1 as i64) * s as i64 / steps as i64;
                            let y = y1 as i64 + (y2 as i64 - y1 as i64) * s as i64 / steps as i64;
                            schedule.entry(cyc + 1_000_000 + s * 1_500_000)
                                .or_default().push(Act::MouseAbs(x as u16, y as u16));
                        }
                        schedule.entry(cyc + 1_000_000 + (steps + 2) * 1_500_000)
                            .or_default().push(Act::MouseBtn(false));
                    } else if let Some(text) = k.strip_prefix("type@") {
                        // Type a string: one tap per character, spaced so the
                        // guest's keyboard queue keeps up (~4M cycles each).
                        for (n, ch) in text.chars().enumerate() {
                            let name = if ch == ' ' { "space".to_string() } else { ch.to_ascii_lowercase().to_string() };
                            let sc = scancode(&name).unwrap_or_else(|| panic!("cannot type {ch:?}"));
                            let at = cyc + n as u64 * 4_000_000;
                            schedule.entry(at).or_default().push(Act::Key(sc, true));
                            schedule.entry(at + 2_000_000).or_default().push(Act::Key(sc, false));
                        }
                    } else if let Some(base) = k.strip_prefix("cmd-opt-") {
                        // Cmd+Option chord: both modifiers down, key tap, both up.
                        let sc = scancode(base).unwrap_or_else(|| panic!("unknown key {base}"));
                        schedule.entry(cyc).or_default().push(Act::Key(CMD, true));
                        schedule.entry(cyc).or_default().push(Act::Key(OPT, true));
                        schedule.entry(cyc + 1_000_000).or_default().push(Act::Key(sc, true));
                        schedule.entry(cyc + 3_000_000).or_default().push(Act::Key(sc, false));
                        schedule.entry(cyc + 4_000_000).or_default().push(Act::Key(OPT, false));
                        schedule.entry(cyc + 4_000_000).or_default().push(Act::Key(CMD, false));
                    } else if let Some(base) = k.strip_prefix("cmd-") {
                        // Command-modified chord: Cmd down, key tap, Cmd up.
                        let sc = scancode(base).unwrap_or_else(|| panic!("unknown key {base}"));
                        schedule.entry(cyc).or_default().push(Act::Key(CMD, true));
                        schedule.entry(cyc + 1_000_000).or_default().push(Act::Key(sc, true));
                        schedule.entry(cyc + 3_000_000).or_default().push(Act::Key(sc, false));
                        schedule.entry(cyc + 4_000_000).or_default().push(Act::Key(CMD, false));
                    } else {
                        let sc = scancode(k).unwrap_or_else(|| panic!("unknown key {k}"));
                        // press now, release ~3M cycles later (a few ms)
                        schedule.entry(cyc).or_default().push(Act::Key(sc, true));
                        schedule.entry(cyc + 3_000_000).or_default().push(Act::Key(sc, false));
                    }
                }
                i += 2;
            }
            other => bail!("unknown arg {other}"),
        }
    }

    fs::create_dir_all(out_dir)?;

    let rom = fs::read(rom_path)?;
    let mdc = fs::read(mdc_path)?;
    let model = MacModel::detect_from_rom(&rom).expect("cannot detect model from ROM");
    log::info!("Detected model: {model}");

    let extra = [ExtraROMs::MDC12(&mdc)];
    let (mut emu, frame_recv) = Emulator::new(&rom, &extra, model)?;

    // Persist PRAM across runs (boot depth / monitor settings live in slot PRAM).
    // Needs the `mmap` feature or this is a silent no-op (load-only, no write-back).
    if let Some(ref p) = pram_path {
        emu.persist_pram(std::path::Path::new(p));
        log::info!("PRAM persisted in {p}");
    }

    let cmd = emu.create_cmd_sender();
    let events = emu.create_event_recv();

    cmd.send(EmulatorCommand::ScsiAttachHdd(0, PathBuf::from(hdd_path)))?;
    if let Some(ref d2) = disk2 {
        cmd.send(EmulatorCommand::ScsiAttachHdd(1, PathBuf::from(d2)))?;
        log::info!("attached 2nd SCSI disk (id 1): {d2}");
    }
    cmd.send(EmulatorCommand::Run)?;
    cmd.send(EmulatorCommand::SetSpeed(EmulatorSpeed::Uncapped))?;

    let start = Instant::now();
    let mut next_snap: u64 = snap_every;
    let mut last_frame: Option<(u16, u16, Vec<u8>)> = None;
    let mut snap_idx = 0u32;
    let mut fired: Vec<u64> = schedule.keys().copied().collect();
    fired.sort_unstable();
    let mut fire_i = 0usize;
    floppies.sort_by_key(|(c, _)| *c);
    let mut floppy_i = 0usize;

    loop {
        let cyc = emu.get_cycles();
        if cyc >= max_cycles { break; }
        if start.elapsed().as_secs() >= wall_secs {
            log::warn!("wall-clock limit reached at {cyc} cycles");
            break;
        }

        // drain frames, keep the latest
        loop {
            let taken = { frame_recv.lock().unwrap().take() };
            match taken {
                Some(buf) => {
                    let (w, h) = (buf.width(), buf.height());
                    last_frame = Some((w, h, buf.into_inner()));
                }
                None => break,
            }
        }

        // drain events (so the channel doesn't back up)
        while let Ok(ev) = events.try_recv() {
            if let EmulatorEvent::Status(s) = ev {
                if !s.running && s.cycles > 100 {
                    log::warn!("emulator stopped at {} cycles", s.cycles);
                }
            }
        }

        // insert any floppies that are due (drive 0, not write-protected;
        // Snow never writes the image back unless writeback is enabled)
        while floppy_i < floppies.len() && floppies[floppy_i].0 <= cyc {
            let (at, ref path) = floppies[floppy_i];
            cmd.send(EmulatorCommand::InsertFloppy(0, path.clone(), false))?;
            log::info!("cyc {at}: floppy inserted: {path}");
            floppy_i += 1;
        }

        // fire any scheduled key edges that are due
        while fire_i < fired.len() && fired[fire_i] <= cyc {
            let at = fired[fire_i];
            for act in schedule.get(&at).unwrap() {
                match *act {
                    Act::Key(sc, down) => {
                        let ev = if down {
                            KeyEvent::KeyDown(sc, Keymap::Universal)
                        } else {
                            KeyEvent::KeyUp(sc, Keymap::Universal)
                        };
                        cmd.send(EmulatorCommand::KeyEvent(ev))?;
                        log::info!("cyc {at}: key sc=0x{sc:02X} down={down}");
                    }
                    Act::MouseAbs(x, y) => {
                        cmd.send(EmulatorCommand::MouseUpdateAbsolute { x, y })?;
                        log::info!("cyc {at}: mouse abs ({x},{y})");
                    }
                    Act::MouseBtn(down) => {
                        cmd.send(EmulatorCommand::MouseUpdateRelative {
                            relx: 0,
                            rely: 0,
                            btn: Some(down),
                        })?;
                        log::info!("cyc {at}: mouse btn down={down}");
                    }
                }
            }
            fire_i += 1;
        }

        // periodic snapshot
        if cyc >= next_snap {
            if let Some((w, h, ref px)) = last_frame {
                let p = format!("{out_dir}/snap_{snap_idx:03}_{cyc}.png");
                write_png(&p, w, h, px)?;
                log::info!("snapshot {p} ({w}x{h})");
            }
            snap_idx += 1;
            next_snap += snap_every;
        }

        emu.tick(1, ())?;
    }

    if let Some((w, h, ref px)) = last_frame {
        let p = format!("{out_dir}/final.png");
        write_png(&p, w, h, px)?;
        log::info!("final {p} ({w}x{h}) after {} cycles", emu.get_cycles());
    } else {
        log::warn!("no frames captured");
    }
    log::info!("done in {:.1}s", start.elapsed().as_secs_f64());
    Ok(())
}
