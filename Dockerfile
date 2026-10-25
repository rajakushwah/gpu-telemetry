# One image holds all four binaries; each workload picks its command (broker, streamer, collector, api).
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/... \
    # Stage the data dir with a traversable (0755) directory and a world-readable CSV,
    # since the source file may be mode 600 and the final container runs as non-root.
    && mkdir -p /broker-data /data \
    && cp data/dcgm_metrics_20250718_134233.csv /data/telemetry.csv \
    && chmod 0755 /data && chmod 0644 /data/telemetry.csv

# Distroless: no shell or package manager, runs as non-root (uid 65532).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
COPY --from=build --chown=65532:65532 /broker-data /var/lib/broker
COPY --from=build /data /data
USER 65532:65532
