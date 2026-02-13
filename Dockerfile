# Build stage
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build args from buildx
ARG TARGETOS
ARG TARGETARCH

# Build static binary for correct platform
RUN CGO_ENABLED=0 \
    GOOS=$TARGETOS \
    GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" \
    -o telemetry ./cmd/telemetry

# Runtime stage
FROM alpine:latest

# Set working directory
WORKDIR /app

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 -S telemetry && \
    adduser -u 1000 -S telemetry -G telemetry

# Copy binary from builder
COPY --from=builder /build/telemetry .
COPY --from=builder /build/configs ./configs
COPY --from=builder /build/pkgs/db/migrations ./pkgs/db/migrations

# Change ownership
RUN chown -R telemetry:telemetry /app

# Switch to non-root user
USER telemetry

EXPOSE 8080

# Run the application
ENTRYPOINT ["./telemetry"]
CMD ["serve"]
