# OptimusDB

Database management systems have been evolved and profoundly influenced by the way data is stored, retrieved, and managed
to serve end-user demands or being part of a centralized infrastructure ecosystem.
In the last decade, distributed databases have been shaped by the principles of the CAP theorem and used in enterprises
to solve a number of dependencies and limitation upon centralized data access and processing approaches.
Under this research we promote the usage of a new Decentralized Database Management system (hereinafter referred as OptimusDB),
which achieves the extension of this theorem  and merely focuses on Decentralization, Consistency and Scalability of the data
in scope among different entities of a multi-diverse ecosystem. This attempt, in a form of a prototype, aims to leverage
a combination of decentralized technologies introducing new paradigms in data storage and management, whilst, addressing
contemporary challenges such as data resilience, scalability, and collaborative access among disparate entities of a data ecosystem.
We represent the next significant milestone in this evolution, by articulating the establishment of a decentralized data store architecture
incorporating advanced data management capabilities for on-premises but also cloud oriented data.
We epitomize these advancements, and unlike traditional database systems, which rely on centralized or distributed models and coordination,
we propose an operation under a peer-to-peer environment, using decentralized data stores, ensuring transparency, and seamless synchronization
and communication across nodes. The aim of this advent is to address critical challenges in data management, thus our novel proposition for OptimusDB
is designed to redefine knowledge exposure across various industry domains and research initiatives.
By converting conceptual and operational designs of specific research principles and challenges, this proposition
is motivated by the underscore of the transformative potential of databases in fostering innovation, contributing to a possible
pathway of reshapement of a global data management and data processing practices.



## Special Considerations and Contributions
OrbitDB:
PeersDB: A distributed p2p database, based on orbitdb and intended for the sharing of datasets for model training.
Libp2p


## Table of Contents
- [Get Started](#get-started)
  - [Flags](#flags)
- [Contribution](#contribution)
  - [Debugging](#debugging)
- [Architecture](#architecture)
  - [Store Replication](#store-replication)
  - [IPFS Replication](#ipfs-replication)
  - [Validation](#validation)
- [APIs](#apis)
  - [Shell](#shell)
  - [HTTP](#http)
- [Evaluation](#evaluation)

# Get Started
# To run the Code and start the instance of an optimusDB


```shell
#Ensure the dependencies are downloaded:
go mod tidy

in case of GOPROXY, us the following to identify

go env GOPROXY , if nothing is returned, use the following

go env -w GOPROXY=https://proxy.golang.org,direct

ensure you use Go version 1.19.13: gvm use go1.19.13 --default

```

Run your project:
```shell
go run main.go
```

Build a Binary (Optional)
To create a binary for faster execution, build the project:
go build -o optimusdb main.go

Run the binary in a Linux OS:
```shell
./optimusdb
```



## Flags
You may use the following flags to configure your optimusDB instance.

| Flag           | Description | Default |
|----------------|-------------|---------|
| -shell | enables the shell interface | false |
| -http | enables the http interface | false |
| -ipfs-port | sets the ipfs port | 4001 |
| -http-port | sets the http port | 8089 |
| -experimental  | enables kubo experimental features | true |
| -repo | configure the repo/directory name for the ipfs node | optimusdb |
| -devlogs | enables development level logging | false |
| -coordinator | makes this agent a LSA agent meaning it will create it's own datastore | false |
| -download-dir | configure where to store downloaded files etc. | ~/Downloads/ |
| -full-replica | enable full data replication through ipfs pinning | false |
| -bootstrap    | set a bootstrap peer to connect to on startup | "" |
| -benchmark    | enables benchmarking on this node | false |
| -region       | if the nodes region is set, it is added to the benchmark data | "" |
