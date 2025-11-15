#!/usr/bin/env python3
"""
OptimusDB Docker Compose Generator
Generates docker-compose.yml with configurable number of agents
"""

import argparse
import sys
from typing import Dict, List

# Template for coordinator (agent-1)
COORDINATOR_TINYLLAMA_TEMPLATE = """
  tinyllama-agent-1:
    image: optimusdb/tinyllama:latest
    build:
      context: .
      dockerfile: Dockerfile.tinyllama
    container_name: tinyllama-agent-1
    hostname: tinyllama-agent-1
    restart: unless-stopped
    deploy:
      resources:
        limits:
          cpus: '{coordinator_cpu}'
          memory: {coordinator_mem}G
        reservations:
          cpus: '{coordinator_cpu_reserve}'
          memory: {coordinator_mem_reserve}G
    environment:
      - MODEL_PATH=/models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf
      - LLAMA_HOST=0.0.0.0
      - LLAMA_PORT=8080
      - LLAMA_CTX_SIZE={coordinator_ctx}
      - LLAMA_N_GPU_LAYERS=0
      - LLAMA_THREADS={coordinator_threads}
      - LLAMA_N_PARALLEL=2
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    networks:
      optimusdb-network:
        ipv4_address: 172.28.1.10
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
    labels:
      - "com.optimusdb.service=tinyllama"
      - "com.optimusdb.agent=1"
      - "com.optimusdb.role=coordinator"
"""

COORDINATOR_AGENT_TEMPLATE = """
  optimusdb-agent-1:
    image: optimusdb/agent:latest
    build:
      context: .
      dockerfile: Dockerfile
    container_name: optimusdb-agent-1
    hostname: optimusdb-agent-1
    restart: unless-stopped
    depends_on:
      tinyllama-agent-1:
        condition: service_healthy
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
    ports:
      - "9091:9091"
      - "8081:8081"
    environment:
      - AGENT_ID=1
      - AGENT_PORT=9091
      - CREDENTIALS_PORT=8081
      - IPFS_API=/ip4/127.0.0.1/tcp/5001
      - ORBITDB_DIRECTORY=/data/orbitdb
      - SQLITE_PATH=/data/optimusdb.db
      - BOOTSTRAP_PEERS=
      - ELECTION_ENABLED=true
      - ELECTION_HEARTBEAT_INTERVAL=5s
      - ELECTION_LEASE_DURATION=15s
      - ELECTION_VOTE_TIMEOUT=10s
      - TINYLLAMA_ENDPOINT=http://tinyllama-agent-1:8080/v1/completions
      - TINYLLAMA_ENABLED=true
      - TINYLLAMA_TIMEOUT=30s
      - TINYLLAMA_MAX_TOKENS=512
      - TINYLLAMA_TEMPERATURE=0.7
      - LOG_LEVEL=info
    volumes:
      - agent1-data:/data
      - agent1-orbitdb:/data/orbitdb
    networks:
      optimusdb-network:
        ipv4_address: 172.28.1.11
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
    labels:
      - "com.optimusdb.service=agent"
      - "com.optimusdb.agent=1"
      - "com.optimusdb.role=coordinator"
"""

# Template for follower agents
FOLLOWER_TINYLLAMA_TEMPLATE = """
  tinyllama-agent-{agent_id}:
    image: optimusdb/tinyllama:latest
    container_name: tinyllama-agent-{agent_id}
    hostname: tinyllama-agent-{agent_id}
    restart: unless-stopped
    deploy:
      resources:
        limits:
          cpus: '{follower_cpu}'
          memory: {follower_mem}G
        reservations:
          cpus: '{follower_cpu_reserve}'
          memory: {follower_mem_reserve}G
    environment:
      - MODEL_PATH=/models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf
      - LLAMA_HOST=0.0.0.0
      - LLAMA_PORT=8080
      - LLAMA_CTX_SIZE={follower_ctx}
      - LLAMA_N_GPU_LAYERS=0
      - LLAMA_THREADS={follower_threads}
      - LLAMA_N_PARALLEL=1
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    networks:
      optimusdb-network:
        ipv4_address: 172.28.1.{tinyllama_ip}
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
    labels:
      - "com.optimusdb.service=tinyllama"
      - "com.optimusdb.agent={agent_id}"
      - "com.optimusdb.role=follower"
"""

FOLLOWER_AGENT_TEMPLATE = """
  optimusdb-agent-{agent_id}:
    image: optimusdb/agent:latest
    container_name: optimusdb-agent-{agent_id}
    hostname: optimusdb-agent-{agent_id}
    restart: unless-stopped
    depends_on:
      tinyllama-agent-{agent_id}:
        condition: service_healthy
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
    ports:
      - "{rest_port}:9091"
      - "{creds_port}:8081"
    environment:
      - AGENT_ID={agent_id}
      - AGENT_PORT=9091
      - CREDENTIALS_PORT=8081
      - IPFS_API=/ip4/127.0.0.1/tcp/5001
      - ORBITDB_DIRECTORY=/data/orbitdb
      - SQLITE_PATH=/data/optimusdb.db
      - BOOTSTRAP_PEERS={bootstrap_peers}
      - ELECTION_ENABLED=true
      - ELECTION_HEARTBEAT_INTERVAL=5s
      - ELECTION_LEASE_DURATION=15s
      - ELECTION_VOTE_TIMEOUT=10s
      - TINYLLAMA_ENDPOINT=http://tinyllama-agent-{agent_id}:8080/v1/completions
      - TINYLLAMA_ENABLED=true
      - TINYLLAMA_TIMEOUT=30s
      - TINYLLAMA_MAX_TOKENS=512
      - TINYLLAMA_TEMPERATURE=0.7
      - LOG_LEVEL=info
    volumes:
      - agent{agent_id}-data:/data
      - agent{agent_id}-orbitdb:/data/orbitdb
    networks:
      optimusdb-network:
        ipv4_address: 172.28.1.{agent_ip}
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
    labels:
      - "com.optimusdb.service=agent"
      - "com.optimusdb.agent={agent_id}"
      - "com.optimusdb.role=follower"
"""

HEADER = """version: '3.8'

# OptimusDB Production Deployment - Auto-generated
# Architecture: Heterogeneous Resource Sidecar Pattern
# Total Containers: {total_containers} ({num_agents} agents + {num_agents} TinyLlama sidecars)
# Generated by: scripts/generate-docker-compose.py

services:
"""

NETWORK_SECTION = """
networks:
  optimusdb-network:
    driver: bridge
    ipam:
      driver: default
      config:
        - subnet: 172.28.0.0/16
          gateway: 172.28.0.1
    driver_opts:
      com.docker.network.bridge.name: optimusdb0
      com.docker.network.bridge.enable_icc: "true"
      com.docker.network.bridge.enable_ip_masquerade: "true"
"""

VOLUMES_HEADER = """
volumes:
"""

VOLUME_TEMPLATE = """  # Agent-{agent_id} volumes
  agent{agent_id}-data:
    driver: local
    labels:
      com.optimusdb.agent: "{agent_id}"
      com.optimusdb.type: "data"
  agent{agent_id}-orbitdb:
    driver: local
    labels:
      com.optimusdb.agent: "{agent_id}"
      com.optimusdb.type: "orbitdb"
"""


def generate_bootstrap_peers(agent_id: int, num_agents: int) -> str:
    """Generate bootstrap peers list for an agent"""
    peers = []
    for i in range(1, agent_id):
        ip = 10 + (i * 10) + 1  # Agent IPs: .11, .21, .31, etc.
        peers.append(f"/ip4/172.28.1.{ip}/tcp/9091")
    return ",".join(peers)


def generate_compose(
    num_agents: int = 8,
    coordinator_cpu: int = 4,
    coordinator_mem: int = 8,
    coordinator_ctx: int = 4096,
    follower_cpu: int = 2,
    follower_mem: int = 4,
    follower_ctx: int = 2048,
) -> str:
    """Generate complete docker-compose.yml content"""

    output = []

    # Header
    output.append(HEADER.format(
        total_containers=num_agents * 2,
        num_agents=num_agents
    ))

    # Coordinator section
    output.append("  # " + "=" * 77)
    output.append("  # COORDINATOR NODE - Agent-1")
    output.append("  # Enhanced resources for heavy metadata generation workload")
    output.append("  # " + "=" * 77)

    output.append(COORDINATOR_TINYLLAMA_TEMPLATE.format(
        coordinator_cpu=coordinator_cpu,
        coordinator_mem=coordinator_mem,
        coordinator_cpu_reserve=coordinator_cpu // 2,
        coordinator_mem_reserve=coordinator_mem // 2,
        coordinator_ctx=coordinator_ctx,
        coordinator_threads=coordinator_cpu
    ))

    output.append(COORDINATOR_AGENT_TEMPLATE)

    # Follower sections
    for agent_id in range(2, num_agents + 1):
        output.append(f"\n  # {'=' * 77}")
        output.append(f"  # FOLLOWER NODE - Agent-{agent_id}")
        output.append(f"  # {'=' * 77}")

        tinyllama_ip = (agent_id * 10)
        agent_ip = (agent_id * 10) + 1
        rest_port = 9090 + agent_id
        creds_port = 8080 + agent_id

        output.append(FOLLOWER_TINYLLAMA_TEMPLATE.format(
            agent_id=agent_id,
            follower_cpu=follower_cpu,
            follower_mem=follower_mem,
            follower_cpu_reserve=follower_cpu // 2,
            follower_mem_reserve=follower_mem // 2,
            follower_ctx=follower_ctx,
            follower_threads=follower_cpu,
            tinyllama_ip=tinyllama_ip
        ))

        bootstrap_peers = generate_bootstrap_peers(agent_id, num_agents)

        output.append(FOLLOWER_AGENT_TEMPLATE.format(
            agent_id=agent_id,
            rest_port=rest_port,
            creds_port=creds_port,
            bootstrap_peers=bootstrap_peers,
            agent_ip=agent_ip
        ))

    # Network section
    output.append("\n# " + "=" * 77)
    output.append("# NETWORKS")
    output.append("# " + "=" * 77)
    output.append(NETWORK_SECTION)

    # Volumes section
    output.append("\n# " + "=" * 77)
    output.append("# VOLUMES")
    output.append("# " + "=" * 77)
    output.append(VOLUMES_HEADER)

    for agent_id in range(1, num_agents + 1):
        output.append(VOLUME_TEMPLATE.format(agent_id=agent_id))

    return "".join(output)


def main():
    parser = argparse.ArgumentParser(
        description="Generate OptimusDB docker-compose.yml with configurable agents"
    )
    parser.add_argument(
        "--agents",
        type=int,
        default=8,
        help="Number of agents (default: 8)"
    )
    parser.add_argument(
        "--coordinator-cpu",
        type=int,
        default=4,
        help="Coordinator CPU cores (default: 4)"
    )
    parser.add_argument(
        "--coordinator-mem",
        type=int,
        default=8,
        help="Coordinator memory in GB (default: 8)"
    )
    parser.add_argument(
        "--coordinator-ctx",
        type=int,
        default=4096,
        help="Coordinator context size (default: 4096)"
    )
    parser.add_argument(
        "--follower-cpu",
        type=int,
        default=2,
        help="Follower CPU cores (default: 2)"
    )
    parser.add_argument(
        "--follower-mem",
        type=int,
        default=4,
        help="Follower memory in GB (default: 4)"
    )
    parser.add_argument(
        "--follower-ctx",
        type=int,
        default=2048,
        help="Follower context size (default: 2048)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="docker-compose.yml",
        help="Output file (default: docker-compose.yml)"
    )

    args = parser.parse_args()

    # Validate
    if args.agents < 1:
        print("Error: Number of agents must be at least 1", file=sys.stderr)
        sys.exit(1)

    # Generate
    content = generate_compose(
        num_agents=args.agents,
        coordinator_cpu=args.coordinator_cpu,
        coordinator_mem=args.coordinator_mem,
        coordinator_ctx=args.coordinator_ctx,
        follower_cpu=args.follower_cpu,
        follower_mem=args.follower_mem,
        follower_ctx=args.follower_ctx
    )

    # Write
    with open(args.output, 'w') as f:
        f.write(content)

    print(f"✅ Generated {args.output}")
    print(f"   - {args.agents} agents")
    print(f"   - {args.agents * 2} total containers")
    print(f"   - Coordinator: {args.coordinator_cpu} CPU, {args.coordinator_mem}GB RAM, {args.coordinator_ctx} ctx")
    print(f"   - Followers: {args.follower_cpu} CPU, {args.follower_mem}GB RAM, {args.follower_ctx} ctx")

    total_ram = (args.coordinator_mem + args.coordinator_mem // 2) + \
                ((args.agents - 1) * (args.follower_mem + args.follower_mem // 2))
    print(f"   - Total RAM needed: ~{total_ram}GB")


if __name__ == "__main__":
    main()