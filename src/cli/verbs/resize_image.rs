//! `rb-cli resize-image IMG --size SIZE [--part N=SIZE|fill|min|keep]...` — the GUI's Resize
//! Image wizard without the questions: a new disk size, each partition's new size with its
//! filesystem, and the volume checks before and after. It drives the same
//! `controller::resize_image` the GUIs do, on what a headless Inspect reads from the image.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::controller::inspect::{
    InspectAction, InspectController, InspectServices, InspectSource, JobRegistry,
};
use crate::controller::resize_image::{
    purpose, PartMode, ResizeImageAction as A, ResizeImageController, StepState,
};
use crate::controller::Controller;
use crate::model::job::NoNotify;
use crate::model::partition_resize::stamp_mib;

#[derive(Debug, Args)]
pub struct ResizeImageArgs {
    /// The disk image to resize (raw `.img` / `.hda` / `.vhd`).
    pub image: PathBuf,
    /// The disk's new size (e.g. `512M`, `2G`).
    #[arg(long)]
    pub size: String,
    /// A partition's new size, by its `idx` from `rb-cli inspect`: `2=300M`, `2=fill` (the
    /// space the others leave), `2=min` (its smallest) or `2=keep`. Repeat per partition.
    #[arg(long = "part", value_name = "N=SIZE")]
    pub parts: Vec<String>,
    /// Write the resized image here (default: `NAME-resized.EXT` beside the image).
    #[arg(long, conflicts_with = "in_place")]
    pub out: Option<PathBuf>,
    /// Change the image itself instead of writing a copy.
    #[arg(long)]
    pub in_place: bool,
    /// Skip the volume checks before and after.
    #[arg(long)]
    pub no_check: bool,
    /// Resize even when the check before finds problems.
    #[arg(long)]
    pub force: bool,
}

/// `N=SIZE` as a row position and the mode and field it sets.
fn part_arg(text: &str) -> Result<(usize, PartMode, Option<String>)> {
    let (n, what) = text
        .split_once('=')
        .ok_or_else(|| anyhow!("--part {text}: expected N=SIZE, N=fill, N=min or N=keep"))?;
    let n: usize = n
        .trim()
        .parse()
        .with_context(|| format!("--part {text}: N"))?;
    if n == 0 {
        bail!("--part {text}: partitions are numbered from 1");
    }
    Ok(match what.trim().to_ascii_lowercase().as_str() {
        "fill" => (n, PartMode::Fill, None),
        "min" | "minimum" => (n, PartMode::Minimum, None),
        "keep" => (n, PartMode::Keep, None),
        size => {
            let bytes = parse_size(size).with_context(|| format!("--part {text}"))?;
            (n, PartMode::Custom, Some(stamp_mib(bytes)))
        }
    })
}

fn settle(c: &mut ResizeImageController) {
    c.wait_idle();
    for (_, line) in c.take_log() {
        log_stderr(format!("rb-cli resize-image: {line}"));
    }
}

pub fn run(args: ResizeImageArgs) -> Result<()> {
    let wanted: Vec<_> = args
        .parts
        .iter()
        .map(|p| part_arg(p))
        .collect::<Result<_>>()?;
    let mut inspect = InspectController::new(InspectServices {
        jobs: JobRegistry::new(),
        notify: Arc::new(NoNotify),
    });
    inspect.apply(InspectAction::Open(InspectSource::Image {
        path: args.image.clone(),
        temp: None,
    }));
    while inspect.loading().is_some() {
        std::thread::sleep(std::time::Duration::from_millis(5));
        inspect.tick();
    }
    let source = inspect.resize_image_source().map_err(|e| anyhow!(e))?;
    let mut c = ResizeImageController::new(Arc::new(NoNotify));
    c.apply(A::Open(Box::new(source)));
    settle(&mut c);
    if !c.is_open() {
        bail!("cannot read {}", args.image.display());
    }

    c.apply(A::SetDiskSize(args.size.clone()));
    if let Err(e) = c.disk_size() {
        bail!("--size {}: {e}", args.size);
    }
    c.apply(A::SetCheck(!args.no_check));
    c.apply(A::Next);
    settle(&mut c);
    for row in c.checks() {
        let verdict = row.verdict.as_ref().map(|v| v.text()).unwrap_or_default();
        println!("before  {}: {verdict}", row.title);
    }
    if c.check_trouble() {
        if !args.force {
            bail!("the check before found problems; repair them, or pass --force");
        }
        c.apply(A::ContinueAnyway(true));
    }
    c.apply(A::Next);

    for (n, mode, text) in wanted {
        let index =
            c.rows().get(n - 1).map(|r| r.index).ok_or_else(|| {
                anyhow!("--part {n}: the image has {} partition(s)", c.rows().len())
            })?;
        match text {
            Some(text) => c.apply(A::SetSizeText { index, text }),
            None => c.apply(A::SetMode { index, mode }),
        };
        let row = &c.rows()[n - 1];
        if row.mode != mode {
            bail!("--part {n}: {} ({})", mode.label(), row.note());
        }
    }
    if let Some(e) = c.live_error() {
        bail!("{e}");
    }
    c.apply(A::Next);
    settle(&mut c);
    if let Some(e) = c.plan_error() {
        bail!("{e}");
    }

    c.apply(A::SetInPlace(args.in_place));
    if let Some(out) = args.out {
        c.apply(A::Picked {
            purpose: purpose::OUTPUT,
            path: Some(out),
        });
    }
    for line in c.review() {
        println!("plan    {line}");
    }
    let asked = !c.apply(A::Start).is_empty();
    if asked {
        c.apply(A::Confirmed {
            purpose: purpose::IN_PLACE,
            yes: true,
        });
    }
    if let Some(e) = c.plan_error() {
        bail!("{e}");
    }
    settle(&mut c);
    for (step, state) in c.steps() {
        let state = match state {
            StepState::Done(note) => format!("done ({note})"),
            StepState::Failed(e) => format!("FAILED: {e}"),
            StepState::Pending => "not run".to_string(),
            StepState::Running => "stopped".to_string(),
        };
        println!("step    {}: {state}", step.label());
    }
    for row in c.after_checks() {
        let verdict = row.verdict.as_ref().map(|v| v.text()).unwrap_or_default();
        println!("after   {}: {verdict}", row.title);
    }
    match c.outcome() {
        Some(Ok(())) => {
            if let Some(out) = c.output() {
                println!("wrote   {}", out.display());
            }
            Ok(())
        }
        Some(Err(e)) => bail!("{e}"),
        None => bail!("the resize did not run"),
    }
}
