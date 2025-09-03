package api

import (
	"context"
	"fmt"
	"log"
	"optimusdb/config"
	"os"
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
	//fmt.Println("################")
	//fmt.Println("Swarmchetrate Env, searching for KB Lead Agent  or   Swarm Agents")
	fmt.Fprintf(os.Stdout, "Swarmchetrate Env, searching for KB Lead Agent  or   Swarm Agents ~ AutoDiscovery is enabled", nil)

	if *config.FlagCoordinator {
		//fmt.Println("Agent is a KB Coordinator, swarm Lead Agent ID: ", host.ID())
		fmt.Fprintf(os.Stdout, "Agent is a KB Coordinator, swarm Lead Agent ID: ", host.ID())
		//fmt.Fprintf(os.Stdout, "Agent Reference ID:  ", host.ID())

		//    /ip4/192.168.2.13/tcp/4001/QmbDQb8vB22cbxqYHEyXrcZ3YWF1pqgaBr76cndta99ER9
	} else {
		fmt.Println("Agent is a KB follower, swarm Agent ID: ", host.ID())
	}
	fmt.Println("Agent Listening on:")
	for _, addr := range host.Addrs() {
		//fmt.Println(addr)
		// Check if it's an IPv4 address and not loopback
		if strings.HasPrefix(addr.String(), "/ip4/") && !strings.Contains(addr.String(), "127.0.0.1") {
			fmt.Println(addr)
		}

	}

	// Create a discovery service
	//_, err = NewService(ctx, host)
	//if err != nil {
	//	return fmt.Errorf("failed to create discovery service: %w", err)
	//}

	// Keep the program running
	select {}
}
