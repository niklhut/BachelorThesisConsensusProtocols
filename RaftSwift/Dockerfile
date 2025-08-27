# ================================
# Build image
# ================================
FROM swift:6.1-bookworm AS build

# Install OS updates
RUN export DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true \
    && apt-get -q update \
    && apt-get -q dist-upgrade -y \
    && apt-get -q install -y \
       libjemalloc-dev \
       protobuf-compiler \
       unzip

# Set up a build area
WORKDIR /build

# First just resolve dependencies.
# This creates a cached layer that can be reused
# as long as your Package.swift/Package.resolved
# files do not change.
COPY ./Package.* ./
RUN swift package resolve \
        $([ -f ./Package.resolved ] && echo "--force-resolved-versions" || true)

# Copy entire repo into container
COPY . .
# Replace the proto generator config file to use the protoc in the container
RUN cp ./Sources/RaftGRPCTransport/Protos/grpc-swift-proto-generator-config.docker.json ./Sources/RaftGRPCTransport/Protos/grpc-swift-proto-generator-config.json

# Build everything, with optimizations, with static linking, and using jemalloc
# N.B.: The static version of jemalloc is incompatible with the static Swift runtime.
RUN swift build -c release \
                --static-swift-stdlib \
                -Xlinker -ljemalloc \
                -Xlinker -s

# Switch to the staging area
WORKDIR /staging

# Copy main executable to staging area
RUN cp "$(swift build --package-path /build -c release --show-bin-path)/Raft" ./

# Copy resources bundled by SPM to staging area
RUN find -L "$(swift build --package-path /build -c release --show-bin-path)/" -regex '.*\.resources$' -exec cp -Ra {} ./ \;

# ================================
# Run image
# ================================
FROM debian:bookworm-slim AS run

# Make sure all system packages are up to date, and install only essential packages.
RUN export DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true \
    && apt-get -q update \
    && apt-get -q dist-upgrade -y \
    && apt-get -q install -y \
      libjemalloc2 \
      libcurl4 \
      ca-certificates \
      tzdata \
      iputils-ping \
      netcat-traditional \
    && rm -r /var/lib/apt/lists/*

# Create a raft user and group with /app as its home directory
RUN useradd --user-group --create-home --system --skel /dev/null --home-dir /app raft

# Switch to the new home directory
WORKDIR /app

# Copy built executable and any staged resources from builder
COPY --from=build --chown=raft:raft /staging /app

# Ensure all further commands run as the raft user
USER raft:raft

# Let Docker bind to port 8080
EXPOSE 8080

# Start the Raft service when the image is run
ENTRYPOINT ["./Raft"]

LABEL org.opencontainers.image.title="Raft Swift"
LABEL org.opencontainers.image.description="A Raft Consensus Protocol implementation in Swift"
LABEL org.opencontainers.image.authors="niklhut"
LABEL org.opencontainers.image.vendor="niklhut"
LABEL org.opencontainers.image.source="https://github.com/niklhut/BachelorThesisConsensusProtocols/tree/main/RaftSwift"
LABEL org.opencontainers.image.licenses="All rights reserved"

