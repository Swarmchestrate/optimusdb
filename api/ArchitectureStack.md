┌─────────────────────────────────────────────────────────────┐
│                    YOUR APPLICATION                          │
│              (OptimusDB, IPFS, OrbitDB)                     │
└─────────────────────────────────────────────────────────────┘
▲
│ uses
▼
┌─────────────────────────────────────────────────────────────┐
│                      GOSSIPSUB                               │
│         (Pub/Sub Protocol for Broadcasting)                 │
│  • Topic subscriptions                                       │
│  • Message routing via mesh                                  │
│  • Efficient gossip propagation                             │
└─────────────────────────────────────────────────────────────┘
▲
│ runs on
▼
┌─────────────────────────────────────────────────────────────┐
│                       LIBP2P                                 │
│         (Peer-to-Peer Networking Library)                   │
│  • Peer discovery (mDNS, DHT)                               │
│  • Connection management (TCP, QUIC, WebRTC)                │
│  • Security (encryption, authentication)                    │
│  • NAT traversal                                            │
│  • Stream multiplexing                                      │
└─────────────────────────────────────────────────────────────┘
▲
│ uses
▼
┌─────────────────────────────────────────────────────────────┐
│                   NETWORK (TCP/IP)                           │
│              (Operating System Sockets)                      │
└─────────────────────────────────────────────────────────────┘