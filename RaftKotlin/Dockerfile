# ================================
# Build stage
# ================================
FROM gradle:8.14-jdk21 AS build

# Set the working directory
WORKDIR /home/gradle/project

# Copy gradle wrapper and config files
COPY gradlew .
COPY gradle ./gradle
COPY settings.gradle.kts .
COPY gradle.properties .

# Copy the raft-node project source
COPY raft-node ./raft-node

# Build the application distribution
RUN ./gradlew :raft-node:installDist --no-daemon

# ================================
# Runtime stage
# ================================
FROM eclipse-temurin:21-jre-alpine AS run

# Install minimal dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create a non-root user and group
RUN addgroup -S raft && adduser -S -G raft raft

# Set working directory
WORKDIR /app

# Copy the built application from the build stage
COPY --from=build /home/gradle/project/raft-node/build/install/raft-node/ .

# Change ownership to the non-root user
RUN chown -R raft:raft /app

# Run as the non-root user
USER raft

# Expose the default port (adjust if your application uses a different one)
EXPOSE 8080

# Set the entrypoint to the application's start script
ENTRYPOINT ["./bin/raft-node"]

LABEL org.opencontainers.image.title="Raft Kotlin"
LABEL org.opencontainers.image.description="A Raft Consensus Protocol implementation in Kotlin"
LABEL org.opencontainers.image.authors="niklhut"
LABEL org.opencontainers.image.vendor="niklhut"
LABEL org.opencontainers.image.source="https://github.com/niklhut/BachelorThesisConsensusProtocols/tree/main/RaftKotlin"
LABEL org.opencontainers.image.licenses="All rights reserved"
