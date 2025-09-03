package config

import (
	"flag"
	"time"
)

var FlagShell = flag.Bool("shell", false, "enable shell interface")
var FlagHTTP = flag.Bool("http", true, "enable http interface")

var FlagIPFSPort = flag.String("ipfs-port", "4001", "configure ipfs port")
var FlagHTTPPort = flag.String("http-port", "8089", "configure http port")

var FlagExp = flag.Bool("experimental", true, "enable ipfs experimental features")
var FlagRepo = flag.String("repo", "swarmkbIpfs", "configure the repo/directory name for the ipfs KUBO node")
var FlagDevLogs = flag.Bool("devlogs", false, "enable development level logging for optimusdb")
var FlagCoordinator = flag.Bool("coordinator", true, "creating a Coordinator (LSA) node means it's possible to create a new datastore")
var FlagDownloadDir = flag.String("download-dir", "~/", "the destination path for downloaded data")
var FlagFullReplica = flag.Bool("full-replica", true, "pins all added data")
var FlagBootstrap = flag.String("bootstrap", "", "set a bootstrap peer to connect to on startup")
var FlagBenchmark = flag.Bool("benchmark", false, "enable benchmarking")
var FlagSwarmName = flag.String("Swarmchestrate", "", "the swarm name this agent is operating")

var FlagMetrics = flag.Bool("metrics", true, "enable metrics for CPU,RAM,etc..")

/*
This is for the discocvery
*/
var FlagAutodiscovery = flag.Bool("autodis", true, "aim to address autodiscovery under multiswarm")
var FlagAutodiscoveryMDNS = flag.Bool("dismDNS", true, "aim to address autodiscovery under mDNS")
var FlagAutodiscoveryipfsPubSub = flag.Bool("disIpfsPubSub", false, "aim to address autodiscovery under ipfsPubSub")
var FlagAutodiscoveryDHT = flag.Bool("disDHT", false, "aim to address autodiscovery under DHT")

/*
THe P2P context among the agents of the swarm
*/
var FlagContext = flag.String("swarmkb", "swarmkb", "set a context to use for http and other P2P")

var Flagdsswres = flag.String("dsswres", "dsswres", "dsswres Data Store")
var Flagkbdata = flag.String("kbdata", "kbdata", "kbdata Data Store")
var Flagkbmetadata = flag.String("kbmetadata", "kbmetadata", "kbmetadata Data Store")
var Flagcontributions = flag.String("contributions", "contributions", "contributions Data Store")

var Flagdsswresaloc = flag.String("dsswresaloc", "dsswresaloc", "dsswresaloc Data Store")

var FlagRDBMSDB = flag.String("kbrdbms", "kbrdbms", "kbrdbms Database")
var FlagRDBMSTable1 = flag.String("datacatalog", "datacatalog", "datacatalog RDBMS")

var FlagLogFilename = flag.String("logfile", "logs/optimusdb.log", "The log path and filename of OptimusDB")
var FlagLokiIsDisabled = flag.Bool("LokiIsDisabled", false, "enables Loki telemetry")
var ElectionMaxRetries = flag.Int("election-retry-limit", 1, "Max number of election retry attempts")
var ElectionRetryDelay = flag.Duration("election-retry-delay", 3*time.Second, "Initial delay before retrying election")
