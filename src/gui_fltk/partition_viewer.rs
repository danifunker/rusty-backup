use fltk::{prelude::*, *};
use rusty_backup::partition::PartitionInfo;

/// Separate window for viewing partition table information
pub struct PartitionViewerWindow {
    wind: window::Window,
}

impl PartitionViewerWindow {
    pub fn new(partitions: &[PartitionInfo], disk_name: &str) -> Self {
        let mut wind = window::Window::default()
            .with_size(700, 400)
            .with_label(&format!("Partition Table - {}", disk_name));
        wind.make_modal(true);

        // Header
        let mut header = frame::Frame::new(10, 10, 680, 30, None);
        header.set_label(&format!("Detected {} partition(s)", partitions.len()));
        header.set_align(enums::Align::Left | enums::Align::Inside);

        // Table display (simple text for now, can be upgraded to fltk::table::Table)
        let mut display = text::TextDisplay::new(10, 50, 680, 300, None);
        let mut buffer = text::TextBuffer::default();

        let mut text = String::from("Index | Type | Start LBA | Size | Filesystem\n");
        text.push_str("------+------+-----------+------+-----------\n");

        for (idx, part) in partitions.iter().enumerate() {
            text.push_str(&format!(
                "{:5} | {:4x} | {:9} | {:>6} | {}\n",
                idx,
                part.partition_type_byte,
                part.start_lba,
                rusty_backup::partition::format_size(part.size_bytes),
                part.type_name.as_str()
            ));
        }

        buffer.set_text(&text);
        display.set_buffer(buffer);

        // Close button
        let mut close_btn = button::Button::new(10, 360, 100, 30, "Close");
        close_btn.set_callback({
            let mut w = wind.clone();
            move |_| {
                w.hide();
            }
        });

        wind.end();

        Self { wind }
    }

    pub fn show(&mut self) {
        self.wind.show();
        while self.wind.shown() {
            app::wait();
        }
    }
}
