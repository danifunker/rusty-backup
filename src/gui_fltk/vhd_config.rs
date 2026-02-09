use fltk::{prelude::*, *};
use rusty_backup::partition::PartitionInfo;
use std::collections::HashMap;

/// Configuration result from VHD sizing dialog
#[derive(Debug, Clone)]
pub struct VhdConfig {
    pub whole_disk: bool,
    pub partition_sizes: HashMap<usize, u64>, // partition index -> size in bytes
}

/// Separate window for VHD backup/export configuration
pub struct VhdConfigWindow {
    wind: window::Window,
    result: Option<VhdConfig>,
}

impl VhdConfigWindow {
    pub fn new(partitions: &[PartitionInfo], operation_name: &str) -> Self {
        let mut wind = window::Window::default()
            .with_size(600, 500)
            .with_label(&format!("VHD Configuration - {}", operation_name));
        wind.make_modal(true);

        // Header
        let mut header = frame::Frame::new(10, 10, 580, 30, None);
        header.set_label("Configure VHD output sizing");
        header.set_align(enums::Align::Left | enums::Align::Inside);

        // Whole disk option
        let _whole_disk_radio = button::RadioButton::new(10, 50, 580, 30, "Whole disk backup");

        // Per-partition option
        let per_partition_radio =
            button::RadioButton::new(10, 85, 580, 30, "Per-partition configuration");

        // Partition list (scrollable area)
        let scroll = group::Scroll::new(10, 120, 580, 300, None);
        let mut y_pos = 125;

        for (idx, part) in partitions.iter().enumerate() {
            let mut frame = frame::Frame::new(15, y_pos, 500, 25, None);
            frame.set_label(&format!(
                "Partition {}: {} (Type 0x{:02x}) - Original: {}",
                idx,
                &part.type_name,
                part.partition_type_byte,
                rusty_backup::partition::format_size(part.size_bytes)
            ));
            y_pos += 30;

            // Size choice for this partition
            menu::Choice::new(15, y_pos, 200, 25, None);
            // TODO: Add choices: Original, Minimum, Custom

            y_pos += 35;
        }
        scroll.end();

        // Action buttons
        let mut ok_btn = button::Button::new(10, 430, 100, 30, "OK");
        let mut cancel_btn = button::Button::new(120, 430, 100, 30, "Cancel");

        ok_btn.set_callback({
            let mut w = wind.clone();
            move |_| {
                // TODO: Gather configuration and set result
                w.hide();
            }
        });

        cancel_btn.set_callback({
            let mut w = wind.clone();
            move |_| {
                w.hide();
            }
        });

        wind.end();

        Self { wind, result: None }
    }

    pub fn show(&mut self) -> Option<VhdConfig> {
        self.wind.show();
        while self.wind.shown() {
            app::wait();
        }
        self.result.take()
    }
}
