#!/bin/bash

# Usage: ./raft_cluster.sh [NUM_PEERS]
NUM_PEERS=${1:-3} # Default to 3 peers if not provided

# Define colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Create a lock file to track if the script is running
LOCK_FILE="/tmp/raft_cluster.lock"
PID_FILE="/tmp/raft_cluster.pids"

# Function to clean up previous runs
cleanup_previous_runs() {
    echo -e "${YELLOW}Cleaning up any existing Raft processes...${NC}"
    
    # Kill any existing Raft nodes more thoroughly
    # First try graceful termination
    if [[ -f "$PID_FILE" ]]; then
        echo -e "${YELLOW}Found PID file from previous run, terminating those processes...${NC}"
        while read -r pid; do
            if ps -p "$pid" > /dev/null 2>&1; then
                echo -e "${YELLOW}Terminating process $pid${NC}"
                kill "$pid" 2>/dev/null
            fi
        done < "$PID_FILE"
        
        # Give processes a moment to shut down gracefully
        sleep 2
        
        # Force kill any remaining processes
        while read -r pid; do
            if ps -p "$pid" > /dev/null 2>&1; then
                echo -e "${RED}Force killing process $pid${NC}"
                kill -9 "$pid" 2>/dev/null
            fi
        done < "$PID_FILE"
        
        rm -f "$PID_FILE"
    fi
    
    # Backup plan: find and kill any processes with 'swift run Raft' in their command line
    RAFT_PIDS=$(pgrep -f "swift run Raft")
    if [[ -n "$RAFT_PIDS" ]]; then
        echo -e "${YELLOW}Found additional Raft processes, terminating them...${NC}"
        echo "$RAFT_PIDS" | xargs kill 2>/dev/null
        sleep 2
        echo "$RAFT_PIDS" | xargs kill -9 2>/dev/null 2>&1
    fi
    
    # Remove the lock file if it exists
    rm -f "$LOCK_FILE"
}

# Cleanup function to call on script exit
cleanup() {
    echo -e "${RED}Shutting down all nodes and client...${NC}"
    for pid in "${PIDS[@]}"; do
        if ps -p "$pid" > /dev/null 2>&1; then
            kill "$pid" 2>/dev/null
            # Give it a moment to shut down gracefully
            sleep 1
            # Force kill if still running
            if ps -p "$pid" > /dev/null 2>&1; then
                kill -9 "$pid" 2>/dev/null
            fi
        fi
    done
    
    if [[ -n "$CLIENT_PID" ]] && ps -p "$CLIENT_PID" > /dev/null 2>&1; then
        kill "$CLIENT_PID" 2>/dev/null
        sleep 1
        if ps -p "$CLIENT_PID" > /dev/null 2>&1; then
            kill -9 "$CLIENT_PID" 2>/dev/null
        fi
    fi
    
    # Remove the lock and PID files
    rm -f "$LOCK_FILE"
    rm -f "$PID_FILE"
    
    echo -e "${GREEN}All processes stopped.${NC}"
}

# Set trap to ensure cleanup runs even if script is interrupted
trap cleanup EXIT INT TERM

# Check for existing lock file
if [[ -f "$LOCK_FILE" ]]; then
    echo -e "${RED}It appears that another instance of this script is running.${NC}"
    echo -e "${YELLOW}If you're sure no other instance is running, you can force cleanup with:${NC}"
    echo -e "${BLUE}  ./$(basename "$0") --force-cleanup${NC}"
    
    if [[ "$1" == "--force-cleanup" ]]; then
        cleanup_previous_runs
    else
        exit 1
    fi
fi

# Create lock file
echo $$ > "$LOCK_FILE"

# Clean up from previous runs
cleanup_previous_runs

echo -e "${BLUE}Starting Raft cluster with $NUM_PEERS peers${NC}"

# Start server nodes
PIDS=()
PEERS=()
BASE_PORT=12000

# Generate peers list
for ((i=1; i<=NUM_PEERS; i++)); do
    PORT=$((BASE_PORT + i))
    PEERS+=("$i:0.0.0.0:$PORT")
done

PEERS_STRING=$(IFS=','; echo "${PEERS[*]}")

# Start each peer
for ((i=1; i<=NUM_PEERS; i++)); do
    PORT=$((BASE_PORT + i))
    echo -e "${GREEN}Starting server node $i on port $PORT...${NC}"
    
    # Remove this node from the peers list
    NODE_PEERS=()
    for peer in "${PEERS[@]}"; do
        [[ $peer != "$i:"* ]] && NODE_PEERS+=("$peer")
    done
    NODE_PEERS_STRING=$(IFS=','; echo "${NODE_PEERS[*]}")
    
    swift run Raft peer --id "$i" --port "$PORT" --peers "$NODE_PEERS_STRING" &
    PIDS+=($!)
    echo "$!" >> "$PID_FILE"
done

# Wait for the servers to start
echo -e "${YELLOW}Waiting for servers to start...${NC}"
sleep 5

# Verify that all nodes are still running
for pid in "${PIDS[@]}"; do
    if ! ps -p "$pid" > /dev/null; then
        echo -e "${RED}Warning: Node with PID $pid failed to start or crashed!${NC}"
    fi
done

# Run the client
echo -e "${BLUE}Starting client...${NC}"
swift run Raft client --peers "$PEERS_STRING" --correctness-test --stress-test --operations 50000 --concurrency 10 &
CLIENT_PID=$!
echo "$CLIENT_PID" >> "$PID_FILE"

# Wait for client to finish
wait $CLIENT_PID
CLIENT_EXIT_CODE=$?

if [[ $CLIENT_EXIT_CODE -eq 0 ]]; then
    echo -e "${GREEN}Test completed successfully!${NC}"
else
    echo -e "${RED}Test failed with exit code $CLIENT_EXIT_CODE${NC}"
fi

# Cleanup will be handled by the EXIT trap
echo -e "${GREEN}Test complete!${NC}"