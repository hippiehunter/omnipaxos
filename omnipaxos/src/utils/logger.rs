use slog::{o, Drain, Logger};
use std::{fs::OpenOptions, sync::Mutex};

/// Creates an asynchronous logger which outputs to both the terminal and a specified file_path.
/// Falls back to terminal-only logging if the file cannot be opened.
pub fn create_logger(file_path: &str) -> Logger {
    let file_logger = (|| -> Result<_, std::io::Error> {
        let path = std::path::Path::new(file_path);
        if let Some(prefix) = path.parent() {
            std::fs::create_dir_all(prefix)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path)?;
        Ok(file)
    })();

    match file_logger {
        Ok(file) => {
            let term_decorator = slog_term::TermDecorator::new().build();
            let file_decorator = slog_term::PlainSyncDecorator::new(file);

            let term_fuse = slog_term::FullFormat::new(term_decorator).build().fuse();
            let file_fuse = slog_term::FullFormat::new(file_decorator).build().fuse();

            let both = Mutex::new(slog::Duplicate::new(term_fuse, file_fuse)).fuse();
            let both = slog_async::Async::new(both).build().fuse();
            Logger::root(both, o!())
        }
        Err(_) => {
            // Fall back to terminal-only logging
            let term_decorator = slog_term::TermDecorator::new().build();
            let term_fuse = slog_term::FullFormat::new(term_decorator).build().fuse();
            let term_fuse = slog_async::Async::new(Mutex::new(term_fuse).fuse())
                .build()
                .fuse();
            Logger::root(term_fuse, o!())
        }
    }
}
