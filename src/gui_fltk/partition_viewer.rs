use fltk::{prelude::*, *};
use rusty_backup::backup::metadata::BackupMetadata;
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

    /// Create a partition viewer from BackupMetadata
    pub fn new_from_metadata(metadata: &BackupMetadata) -> Self {
        let mut wind = window::Window::default()
            .with_size(900, 500)
            .center_screen()
            .with_label("Partition Table Viewer");
        wind.make_modal(true);

        let mut pack = group::Pack::new(10, 10, 880, 480, None);
        pack.set_spacing(10);

        // Header with disk info
        let mut header = frame::Frame::default().with_size(0, 30);
        header.set_label(&format!(
            "Source: {} | Size: {} bytes | Type: {}",
            metadata.source_device, metadata.source_size_bytes, metadata.partition_table_type
        ));
        header.set_align(enums::Align::Left | enums::Align::Inside);
        header.set_label_size(13);

        // Alignment info
        let mut align_frame = frame::Frame::default().with_size(0, 25);
        align_frame.set_label(&format!(
            "Alignment: {} | First Partition LBA: {} | Alignment Sectors: {} | CHS: {}/{}",
            metadata.alignment.detected_type,
            metadata.alignment.first_partition_lba,
            metadata.alignment.alignment_sectors,
            metadata.alignment.heads,
            metadata.alignment.sectors_per_track
        ));
        align_frame.set_align(enums::Align::Left | enums::Align::Inside);
        align_frame.set_label_size(11);

        // Partition table (text display)
        let mut display = text::TextDisplay::default().with_size(0, 340);
        let mut buffer = text::TextBuffer::default();

        let mut text = String::new();
        text.push_str("Idx | Type | Start LBA |    Original Size    |     Imaged Size     | Filesystem  | Resized | Compacted | Logical\n");
        text.push_str("----+------+-----------+---------------------+---------------------+-------------+---------+-----------+--------\n");

        for part in &metadata.partitions {
            text.push_str(&format!(
                "{:3} | 0x{:02X} | {:>9} | {:>19} | {:>19} | {:>11} | {:>7} | {:>9} | {:>7}\n",
                part.index,
                part.partition_type_byte,
                part.start_lba,
                rusty_backup::partition::format_size(part.original_size_bytes),
                rusty_backup::partition::format_size(part.imaged_size_bytes),
                part.type_name.as_str(),
                if part.resized { "Yes" } else { "No" },
                if part.compacted { "Yes" } else { "No" },
                if part.is_logical { "Yes" } else { "No" }
            ));
        }

        // Add summary
        let total_original: u64 = metadata
            .partitions
            .iter()
            .map(|p| p.original_size_bytes)
            .sum();
        let total_imaged: u64 = metadata
            .partitions
            .iter()
            .map(|p| p.imaged_size_bytes)
            .sum();
        text.push_str(&format!(
            "\nTotal: {} partitions | Original: {} | Imaged: {} | Compression: {}",
            metadata.partitions.len(),
            rusty_backup::partition::format_size(total_original),
            rusty_backup::partition::format_size(total_imaged),
            metadata.compression_type
        ));

        buffer.set_text(&text);
        display.set_buffer(buffer);
        display.set_text_font(enums::Font::Courier);
        display.set_text_size(12);

        // Close button
        let mut close_btn = button::Button::default()
            .with_size(100, 35)
            .with_label("Close");
        close_btn.set_callback({
            let mut w = wind.clone();
            move |_| {
                w.hide();
            }
        });

        pack.end();
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
