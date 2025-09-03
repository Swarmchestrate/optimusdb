package debuger

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"os"
	"sync"
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
)

/*
The IPFS repository is a directory where IPFS stores all its configuration, data, and metadata.
This includes:

    Datastore: The primary storage for IPFS data (blocks, objects, etc.).
    Config File: Configuration settings for the IPFS node (e.g., networking, datastore type).
    Keys: Cryptographic keys for node identity.
    Logs and Cache Files: Operational logs and temporary data.

The path /path/to/ipfs/repo is a placeholder and should be replaced with the actual file path where your IPFS repository resides. For example:

    Default Path:
        On most systems, the default repository path for IPFS is:
            Linux/macOS: ~/.ipfs
            Windows: %USERPROFILE%\.ipfs


1. Blocks (Content Data)

    What:
        The primary data stored in IPFS consists of blocks, which are small pieces of content (chunks of files or data).
        These blocks are identified by their CID (Content Identifier), which is a hash of the block's content.

    In PostgreSQL:
        Each block is stored as a row in the database, with:
            The CID as the key.
            The actual block data as the value.


 Key-Value Data

    What:
        IPFS uses a key-value datastore to store various internal metadata and configurations.
        Examples:
            Pinsets: Information about pinned data.
            Object DAG links and metadata.
            Configuration settings for the node.

    In PostgreSQL:
        This is typically implemented as a table with:
            A key column for the unique key.
            A value column for the corresponding data



Pinsets

    What:
        Pinned data is data that the IPFS node ensures will not be garbage collected.
        This involves storing references to the CIDs of the pinned blocks.

    In PostgreSQL:
        Pin information can be stored as a special set of key-value pairs or in a dedicated table to keep track of pinned CIDs.

Indexing Metadata

    What:
        For performance optimization, IPFS may store additional indexing information about the blocks and metadata for faster retrieval.

    In PostgreSQL:
        Indexes are created on tables to speed up lookups, such as by CID or other commonly queried attributes.

*/

var initOnce sync.Once

func envString(t *testing.T, key string, defaultValue string) string {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	return v
}

// Automatically re-create the test datastore.
func initPG(t *testing.T) {

	//POSTGRES_PASSWORD=postgres
	//POSTGRES_USER=postgres
	//POSTGRES_HOST=127.0.0.1
	//POSTGRES_DB=postgres
	//POSTGRES_HOST_AUTH_METHOD=trust

	initOnce.Do(func() {
		connConf, err := pgx.ParseConfig(fmt.Sprintf(
			"postgres://%s:%s@%s/%s?sslmode=disable",
			envString(t, "PG_USER", "postgres"),
			envString(t, "PG_PASS", "postgres"),
			envString(t, "PG_HOST", "127.0.0.1"),
			envString(t, "PG_DB", envString(t, "PG_USER", "postgres")),
		))
		if err != nil {
			t.Fatal(err)
		}
		conn, err := pgx.ConnectConfig(context.Background(), connConf)
		if err != nil {
			t.Fatal(err)
		}
		_, err = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS test_datastore")
		if err != nil {
			t.Fatal(err)
		}
		_, err = conn.Exec(context.Background(), "CREATE DATABASE test_datastore")
		if err != nil {
			t.Fatal(err)
		}
		err = conn.Close(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	})
}

// returns datastore, and a function to call on exit.
//
//	d, close := newDS(t)
//	defer close()
func newDS(t *testing.T) (*Datastore, func()) {
	initPG(t)
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		envString(t, "PG_USER", "postgres"),
		envString(t, "PG_PASS", ""),
		envString(t, "PG_HOST", "127.0.0.1"),
		"test_datastore",
	)
	connConf, err := pgx.ParseConfig(connString)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.ConnectConfig(context.Background(), connConf)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS blocks (key TEXT NOT NULL UNIQUE, data BYTEA)")
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDatastore(context.Background(), connString)
	if err != nil {
		t.Fatal(err)
	}
	return d, func() {
		_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS blocks")
		_ = conn.Close(context.Background())
	}
}

func TestSuite(t *testing.T) {
	d, done := newDS(t)
	defer done()
	dstest.SubtestAll(t, d)
}
