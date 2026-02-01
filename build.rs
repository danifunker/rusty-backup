fn main() {
    #[cfg(all(target_os = "windows", not(debug_assertions)))]
    {
        // Embed manifest in release builds only for automatic elevation
        embed_resource::compile("rusty-backup.manifest", embed_resource::NONE);
    }
}
