# syntax=docker/dockerfile:1

# Multi-stage build. Pure-Go (CGO disabled) yields a static binary that runs
# on a minimal distroless base — no libvips/system deps required.
#
# BIN selects which command to build: "api" or "worker".

FROM golang:1.26 AS build
WORKDIR /src

# Dependency layer: copy manifests first for better build caching.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG BIN=api
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/app ./cmd/${BIN}

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
