# Contributing to Rusty Backup

## Development Setup

### Prerequisites
- Rust toolchain (latest stable)
- Git
- Platform-specific dependencies (see main README)

### First-Time Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/rusty-backup.git
   cd rusty-backup
   ```

2. Build the project (this also installs git hooks):
   ```bash
   cargo build
   ```
   
   **Important**: The first build installs a pre-commit hook via `cargo-husky` that automatically runs `cargo fmt` before each commit.

## Pre-Commit Hook

This repository uses `cargo-husky` to automatically format code before commits:

- **Automatic formatting**: Every commit will run `cargo fmt --all` automatically
- **No manual action needed**: Just commit normally with `git commit`
- **Bypass if needed**: Use `git commit --no-verify` to skip the hook (not recommended)
- **Cross-platform**: Works on Windows, macOS, and Linux

The hook is defined in `.cargo-husky/hooks/pre-commit` and installed to `.git/hooks/pre-commit` during builds.

## Code Style

- Run `cargo fmt` to format code (or rely on the pre-commit hook)
- Run `cargo clippy` to catch common mistakes
- Follow the patterns established in existing code
- See `CLAUDE.md` for architecture guidelines

## Testing

```bash
cargo test              # Run all tests
cargo test test_name    # Run specific test
cargo test --lib        # Unit tests only
cargo test --test '*'   # Integration tests only
```

## Building

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo run                      # Run debug build
cargo run --release            # Run release build
```

## Commit Guidelines

- Write clear, concise commit messages
- Reference issue numbers when applicable
- Code will be auto-formatted by the pre-commit hook
- Ensure tests pass before pushing

## Pull Requests

- Fork the repository and create a feature branch
- Make your changes with clear commits
- Ensure all tests pass and code is formatted
- Submit a PR with a clear description of changes
