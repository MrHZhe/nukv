#pragma once

#include <libnuraft/nuraft.hxx>

#include <cstdint>
#include <string>
#include <mutex>

namespace nukv
{
class RaftLogStore;

class RaftStateManager final : public  nuraft::state_mgr
{
public:
    explicit RaftStateManager(int32_t server_id,std::string endpoint);

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

    std::string endpoint_;

    nuraft::ptr<nuraft::srv_state> state_;

    nuraft::ptr<nuraft::cluster_config> cluster_config_;

    nuraft::ptr<nuraft::log_store> log_store_;
};
}