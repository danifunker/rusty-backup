//! A remote image's link to its daemon: a heartbeat while it is open, a "Lost connection" question
//! when the daemon stops answering, and a reconnect that opens the same image again.

use std::sync::Arc;
use std::time::Duration;

use super::{purpose, HostRequest, InspectController, InspectSource, Source};
use crate::controller::{ConfirmSpec, Effect};
use crate::model::inspect_runner::RemoteImage;
use crate::model::job::Job;

/// How often an idle remote image is checked.
const BEAT: Duration = Duration::from_secs(10);

#[derive(Default)]
pub(super) struct LinkState {
    beat: Option<Job<bool>>,
    /// The daemon and image path of a connection that stopped answering.
    lost: Option<(String, String)>,
    reconnect: Option<Job<RemoteImage>>,
}

impl LinkState {
    pub fn reset(&mut self) {
        if let Some(job) = &self.beat {
            job.cancel();
        }
        *self = LinkState::default();
    }

    pub fn lost(&self) -> bool {
        self.lost.is_some()
    }
}

impl InspectController {
    /// Each tick: a finished reconnect opens the image again; a dead link asks what to do.
    pub(super) fn poll_link(&mut self) -> Vec<Effect> {
        if let Some(done) = self
            .link
            .reconnect
            .as_ref()
            .and_then(crate::controller::job_outcome)
        {
            self.link.reconnect = None;
            let (addr, path) = self.link.lost.clone().unwrap_or_default();
            return match done {
                Ok(conn) => {
                    self.info(format!("Reconnected to {addr}; reopening {path}"));
                    self.link.lost = None;
                    self.load(InspectSource::Remote { conn });
                    Vec::new()
                }
                Err(e) => vec![lost_question(&addr, &path, &e)],
            };
        }
        let Some(Source::Remote { conn }) = &self.source else {
            return Vec::new();
        };
        if self.link.lost.is_some() {
            return Vec::new();
        }
        let (conn, path) = (Arc::clone(&conn.0), conn.1.clone());
        // A failed read elsewhere already marked the session.
        let mut why = conn
            .try_lock()
            .ok()
            .and_then(|c| c.lost().map(str::to_string));
        if why.is_none() {
            if let Some(alive) = self
                .link
                .beat
                .as_ref()
                .and_then(crate::controller::job_outcome)
            {
                self.link.beat = None;
                if alive == Ok(false) {
                    why = Some("the daemon stopped answering".to_string());
                }
            }
        }
        if why.is_none() {
            if self.link.beat.is_none() {
                // The job waits out the interval itself: its finishing is what wakes the shell.
                let probe = Arc::clone(&conn);
                self.link.beat = Some(Job::spawn(
                    "Checking the remote connection...",
                    Arc::clone(&self.services.notify),
                    move |job| {
                        let mut waited = Duration::ZERO;
                        while waited < BEAT {
                            job.check_cancel()?;
                            std::thread::sleep(Duration::from_millis(100));
                            waited += Duration::from_millis(100);
                        }
                        // A transfer holding the line proves it is alive.
                        Ok(match probe.try_lock() {
                            Ok(mut c) => c.heartbeat(),
                            Err(_) => true,
                        })
                    },
                ));
            }
            return Vec::new();
        }
        let why = why.unwrap_or_default();
        let addr = crate::model::worker::lock_status(&*conn).addr().to_string();
        self.warn(format!("Lost the connection to {addr}: {why}"));
        self.requests.push(HostRequest::CloseBrowse);
        self.link.lost = Some((addr.clone(), path.clone()));
        vec![lost_question(&addr, &path, "")]
    }

    /// The answer to the "Lost connection" question: reconnect and reopen, or close the image.
    pub(super) fn link_answered(&mut self, yes: bool) {
        let Some((addr, _)) = self.link.lost.clone() else {
            return;
        };
        if !yes {
            self.link.reset();
            self.unload(false);
            return;
        }
        let (to, path) = (addr.clone(), self.link.lost.clone().unwrap_or_default().1);
        self.info(format!("Reconnecting to {addr}..."));
        self.link.reconnect = Some(Job::spawn(
            format!("Reconnecting to {addr}..."),
            Arc::clone(&self.services.notify),
            move |job| {
                job.set_can_cancel(false);
                let conn =
                    crate::remote::RemoteConnection::connect_shared(to.as_str()).map_err(|e| {
                        let why = format!("{e:#}");
                        anyhow::anyhow!(crate::controller::remote_pick::connect_failure(&to, &why))
                    })?;
                Ok((conn, path))
            },
        ));
    }

    /// The link is down: the source rows stay, but nothing reads through it until a reconnect.
    pub fn remote_lost(&self) -> bool {
        self.link.lost()
    }
}

fn lost_question(addr: &str, path: &str, failed: &str) -> Effect {
    let mut message = format!(
        "{addr} stopped answering while {path} was open. The machine may be off, asleep or off \
         the network, or rb-cli serve may have stopped.\n\nReconnect once it is back, or \
         Cancel to close the image."
    );
    if !failed.is_empty() {
        message = format!("{failed}\n\n{message}");
    }
    Effect::Confirm(ConfirmSpec {
        purpose: purpose::REMOTE_LOST,
        title: format!("Lost connection to {addr}"),
        message,
        confirm_label: "Reconnect".into(),
        destructive: false,
    })
}
