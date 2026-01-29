mod gui;

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([900.0, 700.0])
            .with_min_inner_size([600.0, 400.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Rusty Backup",
        options,
        Box::new(|_cc| Ok(Box::new(gui::RustyBackupApp::default()))),
    )
}
