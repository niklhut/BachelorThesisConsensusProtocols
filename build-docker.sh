#!/bin/bash
set -euo pipefail

if [ -z "${1:-}" ]; then
    echo "Usage: $0 <full-version-tag> (e.g., 1.3.1)"
    exit 1
fi

FULL_VERSION="$1"
MAJOR_MINOR=$(echo "$FULL_VERSION" | cut -d. -f1,2)
PLATFORMS="linux/amd64,linux/arm64"
REGISTRY="registry.niklabs.de/niklhut"

# Explicit component list
COMPONENTS=("raftswift" "raftgo" "raftkotlin")

# Function to get directory for component (compatible with older bash)
get_dir() {
    case "$1" in
        raftswift) echo "RaftSwift" ;;
        raftgo) echo "RaftGo" ;;
        raftkotlin) echo "RaftKotlin" ;;
        *) echo "Unknown component: $1" >&2; exit 1 ;;
    esac
}

# Show what will be built
echo "The following image tags will be built and pushed:"
for name in "${COMPONENTS[@]}"; do
    echo " $REGISTRY/$name:$MAJOR_MINOR"
    echo " $REGISTRY/$name:$FULL_VERSION"
    echo " $REGISTRY/$name:latest"
done

# Ask for confirmation
read -rp "Do you want to continue? [y/N]: " confirm
confirm="$(echo "$confirm" | tr '[:upper:]' '[:lower:]')"
if [[ "$confirm" != "y" && "$confirm" != "yes" ]]; then
    echo "Aborted."
    exit 1
fi

# Store pushed tags for logging
declare -a PUSHED_TAGS=()

echo ""
echo "🚀 Starting parallel builds for version: $FULL_VERSION ($MAJOR_MINOR, latest)..."

for name in "${COMPONENTS[@]}"; do
    dir=$(get_dir "$name")
    TAG1="$REGISTRY/$name:$MAJOR_MINOR"
    TAG2="$REGISTRY/$name:$FULL_VERSION"
    TAG3="$REGISTRY/$name:latest"

    PUSHED_TAGS+=("$TAG1" "$TAG2" "$TAG3")

    echo "▶️ Building $name from $dir..."
    docker buildx build --platform "$PLATFORMS" --push \
        -t "$TAG1" \
        -t "$TAG2" \
        -t "$TAG3" \
        "$dir" &
done

# Wait for all parallel builds
wait

echo ""
echo "✅ All builds completed."
echo ""
echo "📦 Tags pushed:"
for tag in "${PUSHED_TAGS[@]}"; do
    echo " - $tag"
done