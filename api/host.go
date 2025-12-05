package api

import (
	"context"
	"fmt"
	"log"
	"optimusdb/config"
	"optimusdb/logger"
	"strings"

	libp2p "github.com/libp2p/go-libp2p"
)

// StartHost initializes a libp2p host and starts the discovery service
func StartHost(ctx context.Context) error {
	// Create a libp2p host
	host, err := libp2p.New()
	if err != nil {
		return fmt.Errorf("failed to create libp2p host: %w", err)
	}
	// Defer host.Close to ensure the host remains active while the application is running
	defer func() {
		if err := host.Close(); err != nil {
			log.Printf("Error closing host: %v", err)
		}
	}()

	// Print the host's ID and addresses
	logger.Info("[INFO] Swarmchetrate Env, searching for KB Lead Agent  or   Swarm Agents ~ AutoDiscovery is enabled")

	if *config.FlagCoordinator {
		logger.Info("[ELECTION] Agent is a KB Coordinator, swarm AgentID: %v", host.ID())
	} else {
		logger.Info("[ELECTION] Agent is a KB Follower, swarm AgentID: %v", host.ID())
	}
	fmt.Println("Agent Listening on:")
	for _, addr := range host.Addrs() {
		//fmt.Println(addr)
		// Check if it's an IPv4 address and not loopback
		if strings.HasPrefix(addr.String(), "/ip4/") && !strings.Contains(addr.String(), "127.0.0.1") {
			fmt.Println(addr)
		}

	}
	// Keep the program running
	select {}
}
