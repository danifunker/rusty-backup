/// Dialog for selecting a partition to browse
use fltk::{prelude::*, *};

/// Show a partition selection dialog and return the selected index, or None if cancelled
pub fn show_partition_selector(partitions: &[(usize, String, String, u64)]) -> Option<usize> {
    // partitions: (index, type_name, volume_label, size_bytes)

    if partitions.is_empty() {
        return None;
    }

    if partitions.len() == 1 {
        return Some(0); // Auto-select if only one
    }

    // Create dialog window
    let mut dialog = window::Window::default()
        .with_size(500, 300)
        .with_label("Select Partition to Browse");
    dialog.make_modal(true);

    frame::Frame::new(10, 10, 480, 30, "Select a partition:")
        .with_align(enums::Align::Left | enums::Align::Inside);

    let mut choice = menu::Choice::new(10, 50, 480, 30, None);

    // Populate choice widget with formatted partition info
    for (idx, type_name, volume_label, size_bytes) in partitions {
        let size_str = format_size(*size_bytes);
        let label_str = if volume_label.is_empty() {
            "UNKNOWN VOLUME LABEL".to_string()
        } else {
            volume_label.clone()
        };

        let entry = format!(
            "Partition {} - {} - {} ({})",
            idx, type_name, label_str, size_str
        );
        choice.add_choice(&entry);
    }

    choice.set_value(0); // Select first by default

    let mut ok_btn = button::Button::new(300, 250, 90, 30, "OK");
    let mut cancel_btn = button::Button::new(400, 250, 90, 30, "Cancel");

    dialog.end();

    let selected = std::rc::Rc::new(std::cell::RefCell::new(None));
    let selected_clone = selected.clone();

    ok_btn.set_callback({
        let mut dialog = dialog.clone();
        move |_| {
            *selected_clone.borrow_mut() = Some(choice.value() as usize);
            dialog.hide();
        }
    });

    cancel_btn.set_callback({
        let mut dialog = dialog.clone();
        move |_| {
            dialog.hide();
        }
    });

    dialog.show();

    // Run modal event loop
    while dialog.shown() {
        app::wait();
    }

    let result = *selected.borrow();
    result
}

/// Format size in bytes to human-readable string (MiB for >10MB)
fn format_size(bytes: u64) -> String {
    const MIB: u64 = 1024 * 1024;
    const TEN_MB: u64 = 10 * 1024 * 1024;

    if bytes >= TEN_MB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}
