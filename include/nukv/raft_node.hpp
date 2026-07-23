#pragma once

#include <libnuraft/nuraft.hxx>

#include <cstdint>
#include <string>

#include "nukv/raft_state_machine.hpp"
#include "nukv/raft_state_manager.hpp"
#include "nukv/storage/rocks_kv_store.hpp"

namespace nukv final
{
class RaftNode
{
public:
    RaftNode(
        int32_t server_id,
        std::string endpoint,
        int32_t listen_port,
        std::string db_path
    );

    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    void Start();

    void Stop();
private:
    int32_t server_id_;

    std::string endpoint_;

    int32_t listen_port_;

    RocksKVStore store_;

    nuraft::ptr<RaftStateMachine> state_machine_;

    nuraft::ptr<RaftStateManager> state_manager_;

    nuraft::raft_launcher launcher_;

    nuraft::ptr<nuraft::raft_server> raft_server_;
};
}