# NuKV

NuKV is a learning-oriented C++17 distributed key-value store. It combines
NuRaft for Raft replication, RocksDB for durable local state, Protobuf for the
client protocol, and the self-written [mymuduo](https://github.com/MrHZhe/mymuduo)
network library for client TCP connections.

## Architecture

```text
Client -- length-prefixed Protobuf/TCP --> mymuduo ClientServer
                                             | worker queue
                                             v
                                  RaftNode (NuRaft) -> RaftStateMachine -> RocksDB
```

Only the Leader accepts write requests. A Follower returns `STATUS_NOT_LEADER`.
Client request handling is moved from mymuduo I/O callbacks to worker threads,
so Raft submission and storage access do not block the network event loop.

## Project structure

```text
include/nukv/              public service, Raft, and storage headers
src/main.cpp               server entry point and CLI configuration
src/client_server.cpp      TCP framing, Protobuf parsing, client replies
src/raft_node.cpp          NuRaft lifecycle and command submission
src/raft_state_machine.cpp replicated command application
src/raft_state_manager.cpp peer and metadata persistence
src/raft_log_store.cpp     NuRaft log storage adapter
src/storage/               RocksDB key-value wrapper
proto/                     client.proto and command.proto
tests/                     unit tests and cluster integration test
third_party/               NuRaft and mymuduo submodules
```

## Requirements

- Linux
- CMake 3.26 or newer
- C++17 compiler and pthread support
- Protobuf development package and compiler
- RocksDB development package

Clone with submodules:

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

If the default `cmake` is older than 3.26, select a newer binary explicitly,
for example `/snap/bin/cmake` on the development VM.

## Run a local three-node cluster

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

### Configuration files

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

Supported keys are `node_id`, `peers`, `client_port`, and `data_dir`. Command-
line options override values loaded from the configuration file. For a custom
multi-host cluster, put the same complete `peers` list in each node's config,
and set that node's own ID, client port, and data directory separately.

## Client protocol

Requests and responses use a four-byte big-endian payload length followed by a
serialized Protobuf message. Definitions are in `proto/client.proto`. The
repository currently provides the protocol and an automated client in the
integration test rather than a separate interactive client binary.

## Tests

```bash
cmake --build build --parallel
cd build
ctest --output-on-failure
```

The suite covers RocksDB persistence, Protobuf serialization, state-machine
application, Raft log-store behavior, and a Linux-only three-node integration
test. The integration test automatically chooses free ports and temporary data
directories, then cleans up all child processes.

The cluster test covers Leader election, Follower rejection, `Put`/`Get`/
`Delete`, key validation, a 64 KiB value, Leader termination and re-election,
restarted-node log catch-up, and final RocksDB replica consistency.

## Verification results

On 2026-08-28, the Release build completed successfully and CTest reported:

```text
100% tests passed out of 5
```

The three-node integration test also passed three consecutive repeat runs. It
uses isolated temporary state and does not modify the repository's default
`data/` directory.

## Scope and limitations

NuKV is a project-level distributed-storage implementation. NuRaft supplies
the Raft core; this repository focuses on service integration, client framing,
RocksDB state application, node configuration, thread isolation, and automated
failure testing. The current test topology is a same-machine three-node
cluster; cross-machine performance and production durability require separate
validation.

## Related project

- [mymuduo](https://github.com/MrHZhe/mymuduo)
