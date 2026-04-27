# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o data-ingestion-tool ./cmd/ingester

# Final stage
FROM alpine:latest

# Install ca-certificates for TLS connections
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -g 1000 appgroup && \
    adduser -u 1000 -G appgroup -s /bin/sh -D appuser

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/data-ingestion-tool .

# Copy default config
COPY --from=builder /app/config.yaml .

# Create directories for data
RUN mkdir -p data-lake metadata logs && \
    chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Expose any ports if needed (for future HTTP API)
# EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD pgrep data-ingestion-tool || exit 1

# Run the application
ENTRYPOINT ["./data-ingestion-tool"]
CMD ["-config=config.yaml"]
