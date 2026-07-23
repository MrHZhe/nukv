#include "nukv/raft_node.hpp"

#include <utility>
#include <chrono>
#include <stdexcept>
#include <thread>

namespace nukv
{
    RaftNode::RaftNode
        (int32_t server_id,
        std::string endpoint,
        int32_t listen_port,
        std::string db_path)
        : server_id_(server_id)
        , endpoint_(std::move(endpoint))
        , listen_port_(listen_port)
        , store_(db_path)
        , state_machine_(nuraft::cs_new<RaftStateMachine>(store_))
        , state_manager_(nuraft::cs_new<RaftStateManager>(server_id_,endpoint_))
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
}