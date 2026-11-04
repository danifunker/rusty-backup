//! A question a controller asked, drawn over a screen: a y / n confirmation or a typed answer.
//!
//! `W` names which of a screen's controllers asked, so the answer goes back to it. The typed
//! answer runs the controller's validator as the user types (CONTRIBUTING.md § Prompts).

use crossterm::event::KeyCode;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::Frame;

use super::App;
use crate::controller::{ConfirmSpec, TextSpec};

pub(super) enum Ask<W> {
    Confirm(W, ConfirmSpec),
    Text {
        who: W,
        spec: TextSpec,
        input: String,
        edited: bool,
    },
}

impl<W> Ask<W> {
    /// A typed answer, starting from the prompt's initial text.
    pub(super) fn text(who: W, spec: TextSpec) -> Self {
        Ask::Text {
            who,
            input: spec.initial.clone(),
            spec,
            edited: false,
        }
    }

    /// One key; `Some((yes, typed text))` once the question is answered.
    pub(super) fn key(&mut self, code: KeyCode) -> Option<(bool, Option<String>)> {
        match self {
            Ask::Confirm(..) => match code {
                KeyCode::Char('y') | KeyCode::Char('Y') => Some((true, None)),
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => Some((false, None)),
                _ => None,
            },
            Ask::Text {
                spec,
                input,
                edited,
                ..
            } => match code {
                KeyCode::Esc => Some((false, None)),
                KeyCode::Enter if spec.validate(input.as_str()).is_ok() => {
                    Some((true, Some(input.clone())))
                }
                KeyCode::Backspace => {
                    input.pop();
                    *edited = true;
                    None
                }
                KeyCode::Char(c) if !c.is_control() => {
                    input.push(c);
                    *edited = true;
                    None
                }
                _ => None,
            },
        }
    }
}

impl App {
    /// The question's box; `device_note` adds the elevation line a device write may need.
    pub(super) fn draw_ask<W>(
        &self,
        frame: &mut Frame,
        area: Rect,
        ask: &Ask<W>,
        device_note: bool,
    ) {
        let pal = self.palette;
        match ask {
            Ask::Confirm(_, spec) => {
                let style = if spec.destructive {
                    pal.warn()
                } else {
                    pal.accent()
                };
                let mut lines: Vec<Line<'static>> = spec
                    .message
                    .lines()
                    .map(|l| Line::raw(format!(" {l}")))
                    .collect();
                lines.push(Line::raw(""));
                lines.push(Line::styled(
                    format!(" y {}   n cancel", spec.confirm_label),
                    style,
                ));
                if device_note && !self.elevated {
                    lines.push(Line::styled(
                        " Writing a device needs elevation; you may be prompted.",
                        pal.dim(),
                    ));
                }
                self.draw_cmd_box_auto(frame, area, 76, spec.title.as_str(), lines);
            }
            Ask::Text {
                spec,
                input,
                edited,
                ..
            } => {
                let error = match (&spec.error, *edited) {
                    (Some(e), false) => Some(e.clone()),
                    _ => spec
                        .validate(input.as_str())
                        .err()
                        .filter(|e| !e.is_empty()),
                };
                let ok = spec.validate(input.as_str()).is_ok();
                let mut lines: Vec<Line<'static>> = spec
                    .prompt
                    .lines()
                    .map(|l| Line::raw(format!(" {l}")))
                    .collect();
                lines.push(Line::styled(format!(" > {input}_"), pal.accent()));
                lines.push(match error {
                    Some(e) => Line::styled(format!(" {e}"), pal.warn()),
                    None => Line::raw(""),
                });
                lines.push(Line::styled(
                    if ok {
                        format!(" Enter {}   Esc cancel", spec.ok_label)
                    } else {
                        " Esc cancel".to_string()
                    },
                    pal.dim(),
                ));
                self.draw_cmd_box_auto(frame, area, 72, spec.title.as_str(), lines);
            }
        }
    }
}
