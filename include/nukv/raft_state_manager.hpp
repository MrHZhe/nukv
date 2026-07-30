#pragma once

#include <libnuraft/nuraft.hxx>
#include "nukv/storage/rocks_kv_store.hpp"

#include <cstdint>
#include <string>
#include <mutex>
#include <vector>

namespace nukv
{
class RaftLogStore;

struct RaftPeer
{
    int32_t id;
    std::string endpoint;
};

class RaftStateManager final : public  nuraft::state_mgr
{
public:
    explicit RaftStateManager(int32_t current_server_id,std::vector<RaftPeer> peers,std::string metadata_path);

    nuraft::ptr<nuraft::cluster_config> load_config() override;

    void save_config(const nuraft::cluster_config& config) override;

    void save_state(const nuraft::srv_state& state) override;

    nuraft::ptr<nuraft::srv_state> read_state() override;

    nuraft::ptr<nuraft::log_store> load_log_store() override;

    int32_t server_id() override;

    void system_exit(const int exit_code) override;
private:
    std::mutex mutex_;

    int32_t server_id_;

    RocksKVStore metadata_store_;

    nuraft::ptr<nuraft::srv_state> state_;

    nuraft::ptr<nuraft::cluster_config> cluster_config_;

    nuraft::ptr<nuraft::log_store> log_store_;
};
}