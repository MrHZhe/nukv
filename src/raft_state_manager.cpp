#include "nukv/raft_state_manager.hpp"
#include "nukv/raft_log_store.hpp"

#include <cstdlib>
#include <utility>
#include <cstring>

namespace nukv {

RaftStateManager::RaftStateManager(int32_t current_server_id,std::vector<RaftPeer> peers,std::string metadata_path)
    : server_id_(current_server_id)
    , metadata_store_(std::move(metadata_path))
    , state_(nullptr)
    , cluster_config_(nuraft::cs_new<nuraft::cluster_config>())
    , log_store_(nuraft::cs_new<RaftLogStore>())
{
    for(const RaftPeer &peer : peers)
    {
        auto config = nuraft::cs_new<nuraft::srv_config>(peer.id,peer.endpoint);

        cluster_config_->get_servers().push_back(config);
    }
}

nuraft::ptr<nuraft::cluster_config> RaftStateManager::load_config() 
{
    std::lock_guard<std::mutex> lock(mutex_);

    if (!cluster_config_)
    {
        return nullptr;
    }

    auto buffer = cluster_config_->serialize();

    return nuraft::cluster_config::deserialize(*buffer);
}

void RaftStateManager::save_config(const nuraft::cluster_config& config) 
{
    auto buffer = config.serialize();

    auto new_config = nuraft::cluster_config::deserialize(*buffer);

    if (!new_config)
    {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    cluster_config_ = std::move(new_config);
}

void RaftStateManager::save_state(const nuraft::srv_state& state) 
{
    auto buffer = state.serialize();

    auto new_state = nuraft::srv_state::deserialize(*buffer);

    if (!new_state)
    {
        return;
    }

    const std::string serialized_state(reinterpret_cast<const char*>(buffer->data_begin()),buffer->size());

    std::lock_guard<std::mutex> lock(mutex_);

    metadata_store_.Put("__raft/state",serialized_state);

    state_ = std::move(new_state);
}

nuraft::ptr<nuraft::srv_state> RaftStateManager::read_state() 
{
    std::lock_guard<std::mutex> lock(mutex_);

    if (!state_)
    {
        const auto serialized_state = metadata_store_.Get("__raft/state");

        if (!serialized_state.has_value())
        {
            return nullptr;
        }

        auto buffer = nuraft::buffer::alloc(serialized_state->size());

        std::memcpy(
            buffer->data_begin(),
            serialized_state->data(),
            serialized_state->size()
        );

        buffer->pos(0);

        state_ = nuraft::srv_state::deserialize(*buffer);

        if (!state_)
        {
            return nullptr;
        }
    }

    auto buffer = state_->serialize();

    return nuraft::srv_state::deserialize(*buffer);
}

nuraft::ptr<nuraft::log_store> RaftStateManager::load_log_store() 
{
    return log_store_;
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