#include "nukv/raft_state_manager.hpp"

#include <cstdlib>
#include <utility>

namespace nukv {

RaftStateManager::RaftStateManager(int32_t server_id,std::string endpoint)
    : server_id_(server_id),
      endpoint_(std::move(endpoint)),
      state_(nullptr),
      cluster_config_(nuraft::cs_new<nuraft::cluster_config>())
{
    auto self_config = nuraft::cs_new<nuraft::srv_config>(server_id_,endpoint_);

    cluster_config_->get_servers().push_back(self_config);
}

nuraft::ptr<nuraft::cluster_config> RaftStateManager::load_config() 
{
    return cluster_config_;
}

void RaftStateManager::save_config(const nuraft::cluster_config& config) 
{
    auto buffer = config.serialize();

    cluster_config_ =
        nuraft::cluster_config::deserialize(*buffer);
}

void RaftStateManager::save_state(const nuraft::srv_state& state) 
{
    auto buffer = state.serialize();

    state_ = nuraft::srv_state::deserialize(*buffer);
}

nuraft::ptr<nuraft::srv_state> RaftStateManager::read_state() 
{
    return state_;
}

nuraft::ptr<nuraft::log_store> RaftStateManager::load_log_store() 
{
    // 下一阶段实现 RaftLogStore 后替换。
    return nullptr;
}

int32_t RaftStateManager::server_id() 
{
    return server_id_;
}

void RaftStateManager::system_exit(const int exit_code) 
{
    std::exit(exit_code);
}

} 