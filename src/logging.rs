use tracing_subscriber::{fmt, EnvFilter};

/// Initialize tracing subscriber with JSON or text formatting
pub fn init_tracing(level: &str, format: &str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level));

    match format {
        "json" => {
            fmt::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        _ => {
            fmt::fmt()
                .with_env_filter(filter)
                .init();
        }
    }
}
