# Multi-stage Dockerfile for ibkr-mcp-rs
# Uses cargo-chef for efficient dependency caching

# Stage 1: Planner — generate recipe.json for cargo-chef
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Builder — compile dependencies and application
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies (cached layer)
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --release --bin ibkr-mcp-rs

# Stage 3: Runtime — minimal Debian image
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create non-root user
RUN groupadd -g 1000 appgroup && \
    useradd -u 1000 -g appgroup -s /bin/bash -m appuser

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/config && \
    chown -R appuser:appgroup /app

# Copy binary from builder
COPY --from=builder /app/target/release/ibkr-mcp-rs /app/ibkr-mcp-rs

# Copy default config
COPY --from=builder /app/config/default.yaml /app/config/default.yaml

# Set permissions
RUN chmod +x /app/ibkr-mcp-rs

# Switch to non-root user
USER appuser

# Expose MCP server port
EXPOSE 8881

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8881/health/ready || exit 1

# Default command
CMD ["/app/ibkr-mcp-rs"]

# Labels
LABEL maintainer="jiri-options" \
      version="0.1.0" \
      description="Interactive Brokers MCP Server - Rust implementation"
