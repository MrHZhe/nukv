# NuKV

NuKV is a learning-oriented C++17 distributed key-value store. It contains an
explicit Raft implementation, RocksDB for durable local state, Protobuf for
the wire protocols, and the self-written
[mymuduo](https://github.com/MrHZhe/mymuduo) network library.

## Architecture

```text
Client -- length-prefixed Protobuf/TCP --> mymuduo ClientServer
                                             | worker queue
                                             v
                                  RaftNode -> RaftStorage
                                      |          |
                                      v          v
                               CommandApplier -> RocksDB
```

Only the Leader accepts write requests. A Follower returns
`STATUS_NOT_LEADER`. Client request handling is moved from mymuduo I/O
callbacks to worker threads, so Raft submission and storage access do not
block the network event loop.

User data and Raft metadata share one RocksDB instance under
`<data-dir>/state_db`. Raft records use reserved `__raft/` keys. Applying a
committed command and installing a snapshot use one RocksDB write batch, so
user state and Raft progress are updated together.

## Project Structure

```text
include/                   public service, Raft, and storage headers
src/main.cpp               server entry point and CLI configuration
src/client_server.cpp      TCP framing, Protobuf parsing, client replies
src/raft_node.cpp          Raft state machine, RPC, replication, and ReadIndex
src/raft_storage.cpp       Raft metadata, log, snapshot, and deduplication storage
src/command_applier.cpp    deterministic command application
src/rocks_kv_store.cpp     RocksDB key-value wrapper
proto/                     client.proto, command.proto, and raft.proto
tests/                     unit tests and cluster integration test
third_party/mymuduo/       bundled mymuduo submodule
```

## Requirements

- Linux
- CMake 3.22 or newer
- C++17 compiler and pthread support
- Protobuf development package and compiler
- RocksDB development package

Clone with the mymuduo submodule:

```bash
git clone --recurse-submodules https://github.com/MrHZhe/nukv.git
cd nukv
```

For an existing clone:

```bash
git submodule update --init --recursive
```

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

If the default `cmake` is older than 3.22, select a newer binary explicitly,
for example `/snap/bin/cmake` on the development VM.

## Run A Local Three-Node Cluster

Run each command in a separate terminal:

```bash
./build/nukv_server 1
./build/nukv_server 2
./build/nukv_server 3
```

The default Raft endpoints are `127.0.0.1:19001`, `127.0.0.1:19002`, and
`127.0.0.1:19003`; client ports are `18001`, `18002`, and `18003`.

For a custom topology and isolated data directories:

```bash
PEERS='1=127.0.0.1:19001,2=127.0.0.1:19002,3=127.0.0.1:19003'
./build/nukv_server --node-id 1 --peers "$PEERS" --client-port 18001 --data-dir ./data/node1
./build/nukv_server --node-id 2 --peers "$PEERS" --client-port 18002 --data-dir ./data/node2
./build/nukv_server --node-id 3 --peers "$PEERS" --client-port 18003 --data-dir ./data/node3
```

The explicit form is required with custom peer addresses and prevents reusing
persisted data from another cluster.

### Configuration Files

The same options can be stored in a simple `key=value` file. The repository
includes `configs/node1.conf`, `configs/node2.conf`, and `configs/node3.conf`.
Each file sets the node ID, client port, and data directory; when `peers` is
omitted, the built-in local peer list is used.

Start the nodes separately with:

```bash
./build/nukv_server --config configs/node1.conf
./build/nukv_server --config configs/node2.conf
./build/nukv_server --config configs/node3.conf
```

Supported keys are `node_id`, `peers`, `client_port`, and `data_dir`.
Command-line options override values loaded from the configuration file. For
a custom multi-host cluster, put the same complete `peers` list in each
node's config, and set that node's own ID, client port, and data directory
separately.

## Client Protocol

Requests and responses use a four-byte big-endian payload length followed by a
serialized Protobuf message. Definitions are in `proto/client.proto`.
Write requests carry `client_id` and `request_id`. Replaying the same command
with the same pair succeeds without applying it twice; reusing the pair for a
different command is rejected. Deduplication records are included in
snapshots. The repository currently provides the protocol and an automated
client in the integration test rather than a separate interactive client
binary.

## Tests

```bash
cmake --build build --parallel
cd build
ctest --output-on-failure
```

The suite contains three CTest cases: RocksDB persistence, Protobuf
serialization, and a Linux-only three-node integration test. The integration
test automatically chooses free ports and temporary data directories, then
cleans up all child processes.

The cluster test covers Leader election, Follower rejection, `Put`/`Get`/
`Delete`, key validation, request replay and request-id conflict rejection, a
64 KiB value, Leader termination and re-election, restart recovery, log
conflict repair, snapshot creation and log-prefix compaction, a 384 KiB value
that exercises multi-chunk `InstallSnapshot`, follower snapshot catch-up,
post-snapshot log-tail recovery, and final RocksDB replica consistency.

## Raft Implementation

Raft nodes implement leader election, RequestVote, AppendEntries, quorum
commit, log conflict repair, restart recovery, follower catch-up, snapshots,
client request deduplication, and ReadIndex-based linearizable reads. A
ReadIndex request is confirmed by a majority in the current term and waits
until the local state machine has applied the confirmed commit index before
serving the read. Node RPCs use length-prefixed Protobuf messages over mymuduo
`TcpClient` and `TcpServer`; lower-numbered nodes initiate the peer connection
and `TcpClient` performs reconnect retries after failures.

## Verification

The build completes successfully and CTest reports:

```text
100% tests passed, 0 tests failed out of 3
```

The three-node integration test uses isolated temporary state and does not
modify the repository's default `data/` directory. The current tests do not
yet provide exhaustive malformed RPC, concurrent-client, cross-machine, or
long-running fault-injection coverage.

## Scope And Limitations

NuKV is a project-level distributed-storage implementation intended for
learning and interviews. The current test topology is a same-machine
three-node cluster. Snapshot creation, log-prefix compaction, restart
recovery, and follower catch-up through `InstallSnapshot` are implemented and
covered by the integration test, including a snapshot larger than one
transport chunk.

The implementation does not yet include dynamic membership changes, joint
consensus, pre-vote, leadership transfer, snapshot checksums, or a formal
durability benchmark. Cross-machine performance and production durability
still require separate validation.

## Related Project

- [mymuduo](https://github.com/MrHZhe/mymuduo)
