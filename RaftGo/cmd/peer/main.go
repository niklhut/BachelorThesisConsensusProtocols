package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/niklhut/raft_go/internal/core/node"
	"github.com/niklhut/raft_go/internal/core/util"
	"github.com/niklhut/raft_go/internal/transport/grpc"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "peer",
		Short: "Start a Raft peer node",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse flags
			id, _ := cmd.Flags().GetInt("id")
			addr, _ := cmd.Flags().GetString("address")
			port, _ := cmd.Flags().GetInt("port")
			rawPeers, _ := cmd.Flags().GetString("peers")
			persistenceType, _ := cmd.Flags().GetString("persistence")
			compactionThreshold, _ := cmd.Flags().GetInt("compaction-threshold")
			collectMetrics, _ := cmd.Flags().GetBool("collect-metrics")
			var persistence node.RaftNodePersistence
			switch persistenceType {
			case "file":
				persistence, _ = node.NewFileRaftNodePersistence(compactionThreshold)
			case "inMemory":
				persistence = node.NewInMemoryRaftNodePersistence(compactionThreshold)
			default:
				return fmt.Errorf("invalid persistence type: %s", persistenceType)
			}

			ownPeer := util.Peer{
				ID:      id,
				Address: addr,
				Port:    port,
			}

			peers := parsePeers(rawPeers)

			// Choose transport and start
			app := grpc.NewRaftGRPCServer(ownPeer, peers, persistence, collectMetrics)

			ctx := context.Background()
			return app.Serve(ctx)
		},
	}

	rootCmd.Flags().Int("id", 0, "ID of this server")
	rootCmd.Flags().String("address", "0.0.0.0", "Address to listen on")
	rootCmd.Flags().Int("port", 10001, "Port to listen on")
	rootCmd.Flags().String("peers", "", "Comma-separated list of peers in 'id:host:port' format")
	rootCmd.Flags().String("persistence", "inMemory", "Persistence type (file, inMemory)")
	rootCmd.Flags().Int("compaction-threshold", 1000, "Compaction threshold")
	rootCmd.Flags().Bool("collect-metrics", false, "Enable metrics collection, only works in docker container")

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("error: %v", err)
	}
}

// Converts "1:localhost:10001,2:localhost:10002" to []util.Peer
func parsePeers(raw string) []util.Peer {
	var peers []util.Peer
	for _, part := range strings.Split(raw, ",") {
		fmt.Println(part)
		if part == "" {
			continue
		}

		segments := strings.Split(part, ":")
		if len(segments) != 3 {
			log.Fatalf("invalid peer format: %s", part)
		}

		id, err := strconv.Atoi(segments[0])
		if err != nil {
			log.Fatalf("invalid peer id in: %s", part)
		}

		port, err := strconv.Atoi(segments[2])
		if err != nil {
			log.Fatalf("invalid peer port in: %s", part)
		}

		peers = append(peers, util.Peer{
			ID:      id,
			Address: segments[1],
			Port:    port,
		})
	}
	return peers
}
