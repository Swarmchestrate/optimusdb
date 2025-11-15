# TinyLlama Deployment Guide

## Architecture

OptimusDB uses a **sidecar pattern** for TinyLlama deployment:
```
Production (16 containers):
├── tinyllama-agent-1 (8GB RAM, 4 CPU)
├── optimusdb-agent-1
├── tinyllama-agent-2 (4GB RAM, 2 CPU)
├── optimusdb-agent-2
└── ... (agents 3-8)

Development (8 containers):
├── optimusdb-dev-agent-1 (OptimusDB + TinyLlama)
├── optimusdb-dev-agent-2 (OptimusDB + TinyLlama)
└── ... (agents 3-8)
```

## Resource Allocation

### Coordinator (Agent-1)
- **TinyLlama**: 8GB RAM, 4 CPU cores, 4096 context
- **Reasoning**: Handles 60-70% of metadata generation

### Followers (Agents 2-8)
- **TinyLlama**: 4GB RAM, 2 CPU cores, 2048 context
- **Reasoning**: Lighter workload, occasional generation

### Total Resources
- **Production**: ~44GB RAM (8GB + 7×4GB coordinator, 8×2GB agents)
- **Development**: ~24GB RAM (8×3GB combined containers)

## Deployment

### Production
```bash
# Build and deploy
bash scripts/deploy-production.sh

# Monitor
pwsh scripts/monitor-tinyllama-cluster.ps1

# Verify
bash scripts/verify-tinyllama-connectivity.sh
```

### Development
```bash
# Quick start
bash scripts/deploy-dev.sh

# Monitor
docker-compose -f docker-compose.dev.yml logs -f
```

## Health Checks

Each TinyLlama container includes health checks:
- **Interval**: 30 seconds
- **Timeout**: 10 seconds
- **Start Period**: 60 seconds (model loading)
- **Endpoint**: `http://localhost:8080/health`

## Troubleshooting

### TinyLlama won't start
```bash
# Check logs
docker logs tinyllama-agent-1

# Common issues:
# - Insufficient memory (needs 4GB+)
# - Model file missing
# - Port 8080 already in use
```

### OptimusDB can't reach TinyLlama
```bash
# Test connectivity
docker exec optimusdb-agent-1 wget -O- http://tinyllama-agent-1:8080/health

# Check network
docker network inspect optimusdb-network
```

### Slow generation
```bash
# Check resource usage
docker stats tinyllama-agent-1

# Increase threads in docker-compose.yml:
environment:
- LLAMA_THREADS=4  # Increase from 2
```

## Performance Tuning

### Context Size
```yaml
# Larger context = better quality, more RAM
environment:
- LLAMA_CTX_SIZE=4096  # Default: 2048
```

### Thread Count
```yaml
# More threads = faster generation, more CPU
environment:
- LLAMA_THREADS=4  # Default: 2
```

### Batch Size
```yaml
# For multiple simultaneous requests
environment:
- LLAMA_N_PARALLEL=4  # Default: 1
```

## Research Claims

For CENTERIS 2025 paper:

> "OptimusDB implements a fully decentralized architecture where
> each of the eight agent nodes operates with a dedicated TinyLlama
> sidecar container, ensuring complete operational independence and
> fault isolation. Unlike centralized metadata services, this
> architecture eliminates single points of failure while maintaining
> consistent AI-powered contextual metadata generation across the
> distributed cluster."

**Key Points**:
- ✅ "Fully decentralized" - each agent has dedicated LLM
- ✅ "Fault isolation" - coordinator LLM failure doesn't affect followers
- ✅ "No single point of failure" - no shared metadata service
- ✅ "Consistent generation" - deterministic prompts, same model version