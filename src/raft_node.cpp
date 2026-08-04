#include "nukv/raft_node.hpp"
#include "command.pb.h"

#include <utility>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <cstring>

namespace nukv
{
    RaftNode::RaftNode
        (int32_t server_id,
        std::string endpoint,
        int32_t listen_port,
        std::string db_path,
        std::string metadata_path,
        std::vector<RaftPeer> peers)
        : server_id_(server_id)
        , endpoint_(std::move(endpoint))
        , listen_port_(listen_port)
        , store_(db_path)
        , state_machine_(nuraft::cs_new<RaftStateMachine>(store_))
        , state_manager_(nuraft::cs_new<RaftStateManager>(server_id_,std::move(peers),std::move(metadata_path)))
        , launcher_()
        , raft_server_(nullptr)
    {}

    RaftNode::~RaftNode()
    {
        Stop();
    }

    void RaftNode::Stop()
    {
        if(!raft_server_)
        {
            return;
        }
        launcher_.shutdown();
        raft_server_.reset();
    }

    void RaftNode::Start()
    {
        if (raft_server_)
        {
            return;
        }

        nuraft::asio_service::options asio_options;
        nuraft::raft_params raft_parameters;
        raft_parameters
        .with_snapshot_enabled(10)
        .with_reserved_log_items(5);

        raft_server_ = launcher_.init(
            state_machine_,
            state_manager_,
            nullptr,
            listen_port_,
            asio_options,
            raft_parameters
        );

        if (!raft_server_)
        {
            launcher_.shutdown();

            throw std::runtime_error(
                "failed to start Raft server"
            );
        }

        const auto deadline =
            std::chrono::steady_clock::now()
            + std::chrono::seconds(5);

        while (!raft_server_->is_initialized())
        {
            if (std::chrono::steady_clock::now()
                >= deadline)
            {
                Stop();

                throw std::runtime_error(
                    "timed out waiting for Raft server initialization"
                );
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(50)
            );
        }
    }

    bool RaftNode::IsLeader() const
    {
        if(!raft_server_)
        {
            return false;
        }
        return raft_server_->is_leader();
    }

    bool RaftNode::Submit(const proto::Command& command)
    {
        if (!raft_server_ || !raft_server_->is_leader())
        {
            return false;
        }

        std::string serialized_command;

        if (!command.SerializeToString(&serialized_command))
        {
            return false;
        }

        auto log = nuraft::buffer::alloc(serialized_command.size());

        std::memcpy(
            log->data_begin(),
            serialized_command.data(),
            serialized_command.size()
        );

        log->pos(0);

        auto result = raft_server_->append_entries({log});

        return result && result->get_result_code() == nuraft::cmd_result_code::OK;
    }

    std::optional<std::string> RaftNode::GetLocal(const std::string& key) const
    {
        return store_.Get(key);
    }
}