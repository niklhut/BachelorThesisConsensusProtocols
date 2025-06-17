# ================================
# Build stage
# ================================
FROM golang:1.24-alpine AS build

# Install required packages
RUN apk add --no-cache git

# Set the working directory
WORKDIR /app

# Copy go mod files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the application (replace with your correct build target if needed)
RUN go build -o raft-peer ./cmd/peer/main.go

# ================================
# Runtime stage
# ================================
FROM alpine:latest AS run

# Install minimal dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -H -u 10001 raft

# Set working directory
WORKDIR /app

# Copy built binary from build stage
COPY --from=build /app/raft-peer .

# Change ownership
RUN chown raft:raft /app/raft-peer

# Run as non-root user
USER raft

# Expose port (default, can be overridden)
EXPOSE 10001

# Default command, can be overridden by `docker run` arguments
ENTRYPOINT ["./raft-peer"]

LABEL org.opencontainers.image.title="Raft Go"
LABEL org.opencontainers.image.description="A Raft Consensus Protocol implementation in Go"
LABEL org.opencontainers.image.authors="niklhut"
LABEL org.opencontainers.image.source="https://github.com/niklhut/BachelorThesisConsensusProtocols/tree/main/RaftGo"
LABEL org.opencontainers.image.licenses="All rights reserved"
