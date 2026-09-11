#include "raft_node.hpp"

#include "Buffer.h"
#include "EventLoop.h"
#include "InetAddress.h"
#include "TcpClient.h"
#include "TcpConnection.h"
#include "TcpServer.h"
#include "Timestamp.h"
#include "command.pb.h"
#include "command_applier.hpp"
#include "raft.pb.h"

#include <arpa/inet.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <future>
#include <limits>
#include <stdexcept>
#include <utility>

namespace
{
constexpr std::uint32_t kMaxFrameSize = 16U * 1024U * 1024U;
constexpr std::uint64_t kMaxEntriesPerRpc = 64;
constexpr std::uint64_t kSnapshotThreshold = 8;
constexpr std::size_t kSnapshotChunkSize = 256U * 1024U;
constexpr double kHeartbeatInterval = 0.10;
constexpr double kElectionTimeout = 0.70;

std::uint32_t ReadNetworkUint32(const char* data)
{
    std::uint32_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return ntohl(value);
}

std::string Frame(const std::string& payload)
{
    if (payload.size() > kMaxFrameSize)
    {
        throw std::runtime_error("Raft RPC frame is too large");
    }

    const std::uint32_t size =
        htonl(static_cast<std::uint32_t>(payload.size()));
    std::string frame(sizeof(size), '\0');
    std::memcpy(frame.data(), &size, sizeof(size));
    frame += payload;
    return frame;
}

std::pair<std::string, int> ParseEndpoint(const std::string& endpoint)
{
    const std::size_t separator = endpoint.rfind(':');
    if (separator == std::string::npos ||
        separator == 0 ||
        separator + 1 >= endpoint.size())
    {
        throw std::invalid_argument("Raft endpoint must use host:port format");
    }

    const int port = std::stoi(endpoint.substr(separator + 1));
    if (port < 1 || port > 65535)
    {
        throw std::invalid_argument("Raft endpoint port is out of range");
    }

    return {endpoint.substr(0, separator), port};
}

}

namespace nukv
{
RaftNode::RaftNode(
    int32_t server_id,
    std::string endpoint,
    int32_t listen_port,
    std::string db_path,
    std::vector<RaftPeer> peers)
    : server_id_(server_id)
    , endpoint_(std::move(endpoint))
    , listen_port_(listen_port)
    , peers_(std::move(peers))
    , store_(std::move(db_path))
    , raft_storage_(store_)
{
}

RaftNode::~RaftNode()
{
    Stop();
}

std::uint64_t RaftNode::LastIndex() const
{
    return snapshot_index_ + static_cast<std::uint64_t>(log_.size());
}

std::uint64_t RaftNode::LastTerm() const
{
    return log_.empty() ? snapshot_term_ : log_.back().term;
}

std::uint64_t RaftNode::TermAt(std::uint64_t index) const
{
    if (index == snapshot_index_)
    {
        return snapshot_term_;
    }
    if (index <= snapshot_index_ || index > LastIndex())
    {
        return 0;
    }
    return log_[LogOffset(index)].term;
}

std::size_t RaftNode::LogOffset(std::uint64_t index) const
{
    if (index <= snapshot_index_ || index > LastIndex())
    {
        throw std::out_of_range("Raft log index is outside retained log");
    }
    return static_cast<std::size_t>(index - snapshot_index_ - 1);
}

void RaftNode::LoadState()
{
    const PersistedRaftState state = raft_storage_.Load();
    snapshot_index_ = state.snapshot_index;
    snapshot_term_ = state.snapshot_term;
    snapshot_data_ = state.snapshot_data;
    current_term_ = state.current_term;
    voted_for_ = state.voted_for;
    commit_index_ = std::max(state.commit_index, snapshot_index_);
    last_applied_ = std::max(state.last_applied, snapshot_index_);
    log_ = state.log;

    commit_index_ = std::min(commit_index_, LastIndex());
    last_applied_ = std::min(last_applied_, commit_index_);
    ApplyCommitted();
}

void RaftNode::PersistState()
{
    raft_storage_.PersistState(
        current_term_,
        voted_for_,
        commit_index_,
        last_applied_,
        snapshot_index_,
        snapshot_term_);
}

void RaftNode::PersistLog()
{
    raft_storage_.PersistLog(snapshot_index_, log_);
}

void RaftNode::Start()
{
    std::unique_lock<std::mutex> lock(lifecycle_mutex_);
    if (raft_thread_.joinable())
    {
        return;
    }

    stopping_.store(false, std::memory_order_release);
    ready_.store(false, std::memory_order_release);
    leader_.store(false, std::memory_order_release);
    startup_finished_ = false;
    startup_error_ = nullptr;

    raft_thread_ = std::thread([this]()
    {
        try
        {
            EventLoop loop;
            {
                std::lock_guard<std::mutex> guard(lifecycle_mutex_);
                raft_loop_ = &loop;
            }

            loop.runInLoop([this]()
            {
                try
                {
                    StartInLoop();
                    {
                        std::lock_guard<std::mutex> guard(lifecycle_mutex_);
                        startup_finished_ = true;
                    }
                    lifecycle_condition_.notify_all();
                }
                catch (...)
                {
                    MarkStartFailure(std::current_exception());
                    StopInLoop();
                    raft_loop_->quit();
                }
            });
            loop.loop();

            server_.reset();
            clients_.clear();
            outbound_connections_.clear();
            inbound_connections_.clear();
            {
                std::lock_guard<std::mutex> guard(lifecycle_mutex_);
                raft_loop_ = nullptr;
            }
        }
        catch (...)
        {
            MarkStartFailure(std::current_exception());
        }
    });

    lifecycle_condition_.wait(
        lock,
        [this]()
        {
            return startup_finished_ || startup_error_ != nullptr;
        });

    if (startup_error_ != nullptr)
    {
        const std::exception_ptr error = startup_error_;
        lock.unlock();
        if (raft_thread_.joinable())
        {
            raft_thread_.join();
        }
        std::rethrow_exception(error);
    }
}

void RaftNode::Stop()
{
    EventLoop* loop = nullptr;
    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        if (!raft_thread_.joinable())
        {
            return;
        }
        stopping_.store(true, std::memory_order_release);
        loop = raft_loop_;
    }

    if (loop != nullptr)
    {
        loop->queueInLoop([this]()
        {
            StopInLoop();
            raft_loop_->quit();
        });
    }

    raft_thread_.join();
    leader_.store(false, std::memory_order_release);
    ready_.store(false, std::memory_order_release);
}

void RaftNode::MarkStartFailure(std::exception_ptr error)
{
    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        if (startup_error_ == nullptr)
        {
            startup_error_ = std::move(error);
        }
        startup_finished_ = true;
    }
    lifecycle_condition_.notify_all();
}

void RaftNode::StartInLoop()
{
    role_ = Role::Follower;
    votes_received_ = 0;
    granted_votes_.clear();
    LoadState();

    const auto [host, port] = ParseEndpoint(endpoint_);
    server_ = std::make_unique<TcpServer>(
        raft_loop_,
        InetAddress(static_cast<std::uint16_t>(port), host),
        "NuKVRaft");
    server_->setThreadNum(0);
    server_->setConnectionCallback(
        [this](const TcpConnectionPtr& connection)
        {
            HandleConnection(connection);
        });
    server_->setMessageCallback(
        [this](
            const TcpConnectionPtr& connection,
            Buffer* buffer,
            Timestamp timestamp)
        {
            HandleMessage(connection, buffer, timestamp);
        });
    server_->start();

    for (const RaftPeer& peer : peers_)
    {
        if (peer.id == server_id_)
        {
            continue;
        }

        if (server_id_ < peer.id)
        {
            CreatePeerClient(peer.id);
        }

        next_index_[peer.id] = LastIndex() + 1;
        match_index_[peer.id] = 0;
        rpc_inflight_[peer.id] = false;
        rpc_request_id_[peer.id] = 0;
        rpc_has_entries_[peer.id] = false;
        rpc_snapshot_[peer.id] = false;
        rpc_snapshot_offset_[peer.id] = 0;
        rpc_generation_[peer.id] = 0;
        rpc_request_id_[peer.id] = 0;
        rpc_has_entries_[peer.id] = false;
    }

    ready_.store(true, std::memory_order_release);
    ResetElectionTimer();
}

void RaftNode::StopInLoop()
{
    if (election_timer_.valid())
    {
        raft_loop_->cancel(election_timer_);
        election_timer_ = TimerId();
    }
    if (heartbeat_timer_.valid())
    {
        raft_loop_->cancel(heartbeat_timer_);
        heartbeat_timer_ = TimerId();
    }
    FailPendingReadIndexes();
    ready_.store(false, std::memory_order_release);
    leader_.store(false, std::memory_order_release);
    server_.reset();
    clients_.clear();
    outbound_connections_.clear();
    inbound_connections_.clear();
}

TcpConnectionPtr RaftNode::ConnectionForPeer(int32_t peer_id) const
{
    const auto& connections =
        server_id_ < peer_id ? outbound_connections_ : inbound_connections_;
    const auto connection = connections.find(peer_id);
    if (connection == connections.end())
    {
        return {};
    }
    return connection->second;
}

void RaftNode::ResetPeerRpcState(
    int32_t peer_id,
    const TcpConnectionPtr& connection)
{
    if (connection && ConnectionForPeer(peer_id) != connection)
    {
        return;
    }

    rpc_inflight_[peer_id] = false;
    rpc_request_id_[peer_id] = 0;
    rpc_has_entries_[peer_id] = false;
    rpc_snapshot_[peer_id] = false;
    rpc_snapshot_offset_[peer_id] = 0;
    ++rpc_generation_[peer_id];
}

void RaftNode::CreatePeerClient(int32_t peer_id)
{
    const auto peer = std::find_if(
        peers_.begin(),
        peers_.end(),
        [peer_id](const RaftPeer& value)
        {
            return value.id == peer_id;
        });
    if (peer == peers_.end() || server_id_ >= peer_id)
    {
        return;
    }

    const auto [host, port] = ParseEndpoint(peer->endpoint);
    auto client = std::make_unique<TcpClient>(
        raft_loop_,
        InetAddress(static_cast<std::uint16_t>(port), host),
        "NuKVRaftPeer" + std::to_string(peer_id));
    client->setConnectionCallback(
        [this, peer_id](const TcpConnectionPtr& connection)
        {
            OnPeerConnection(peer_id, connection);
        });
    client->setMessageCallback(
        [this](
            const TcpConnectionPtr& connection,
            Buffer* buffer,
            Timestamp timestamp)
        {
            HandleMessage(connection, buffer, timestamp);
        });
    client->connect();
    clients_[peer_id] = std::move(client);
}

void RaftNode::ResetElectionTimer()
{
    if (election_timer_.valid())
    {
        raft_loop_->cancel(election_timer_);
    }

    const double jitter =
        static_cast<double>(
            (server_id_ * 13 + static_cast<int32_t>(current_term_) * 7) % 5)
        * 0.05;
    election_timer_ = raft_loop_->runAfter(
        kElectionTimeout + jitter,
        [this]()
        {
            if (role_ != Role::Leader && !stopping_)
            {
                BecomeCandidate();
            }
        });
}

void RaftNode::BecomeCandidate()
{
    role_ = Role::Candidate;
    leader_.store(false, std::memory_order_release);
    ++current_term_;
    voted_for_ = server_id_;
    votes_received_ = 1;
    granted_votes_.clear();
    granted_votes_.insert(server_id_);
    PersistState();
    ResetElectionTimer();
    SendRequestVotes();

    if (votes_received_ >= peers_.size() / 2 + 1)
    {
        BecomeLeader();
    }
}

void RaftNode::BecomeLeader()
{
    role_ = Role::Leader;
    leader_.store(true, std::memory_order_release);
    if (election_timer_.valid())
    {
        raft_loop_->cancel(election_timer_);
        election_timer_ = TimerId();
    }
    if (heartbeat_timer_.valid())
    {
        raft_loop_->cancel(heartbeat_timer_);
        heartbeat_timer_ = TimerId();
    }

    for (const RaftPeer& peer : peers_)
    {
        if (peer.id == server_id_)
        {
            continue;
        }
        next_index_[peer.id] = LastIndex() + 1;
        match_index_[peer.id] = 0;
        rpc_inflight_[peer.id] = false;
    }
    SendHeartbeats();
}

void RaftNode::StepDown(std::uint64_t term)
{
    const bool term_changed = term > current_term_;
    if (term_changed)
    {
        current_term_ = term;
        voted_for_ = -1;
        PersistState();
    }

    if (role_ == Role::Leader && heartbeat_timer_.valid())
    {
        raft_loop_->cancel(heartbeat_timer_);
        heartbeat_timer_ = TimerId();
    }
    role_ = Role::Follower;
    leader_.store(false, std::memory_order_release);
    FailPendingReadIndexes();
    ResetElectionTimer();
}

void RaftNode::SendRequestVotes()
{
    for (const RaftPeer& peer : peers_)
    {
        if (peer.id != server_id_)
        {
            SendRequestVoteToPeer(peer.id);
        }
    }
}

void RaftNode::SendRequestVoteToPeer(int32_t peer_id)
{
    proto::RequestVoteRequest request;
    request.set_term(current_term_);
    request.set_candidate_id(server_id_);
    request.set_last_log_index(LastIndex());
    request.set_last_log_term(LastTerm());

    std::string payload;
    if (!request.SerializeToString(&payload))
    {
        return;
    }
    vote_request_id_[peer_id] = SendRpcToPeer(
        peer_id,
        proto::RaftRpc::REQUEST_VOTE,
        payload);
}

void RaftNode::SendHeartbeats()
{
    if (role_ != Role::Leader || stopping_)
    {
        return;
    }

    for (const RaftPeer& peer : peers_)
    {
        if (peer.id != server_id_)
        {
            SendAppend(peer.id);
        }
    }

    heartbeat_timer_ = raft_loop_->runAfter(
        kHeartbeatInterval,
        [this]()
        {
            SendHeartbeats();
    });
}

void RaftNode::StartNextReadIndex()
{
    if (active_read_index_ ||
        pending_read_indexes_.empty())
    {
        return;
    }
    if (role_ != Role::Leader || stopping_)
    {
        FailPendingReadIndexes();
        return;
    }

    active_read_index_ = pending_read_indexes_.front();
    pending_read_indexes_.pop_front();
    active_read_index_->term = current_term_;
    active_read_index_->read_index = commit_index_;
    active_read_index_->acknowledgements.clear();
    active_read_index_->acknowledgements.insert(server_id_);

    const std::size_t quorum = peers_.size() / 2 + 1;
    if (active_read_index_->acknowledgements.size() >= quorum)
    {
        FinishReadIndex(true);
        return;
    }

    SendReadIndexProbes();
    const auto active = active_read_index_;
    read_index_timer_ = raft_loop_->runAfter(
        0.35,
        [this, active]()
        {
            if (active_read_index_ == active)
            {
                FinishReadIndex(false);
            }
        });
}

void RaftNode::SendReadIndexProbes()
{
    if (!active_read_index_ ||
        role_ != Role::Leader ||
        active_read_index_->term != current_term_)
    {
        return;
    }

    proto::ReadIndexRequest request;
    request.set_term(current_term_);
    request.set_leader_id(server_id_);
    std::string payload;
    if (!request.SerializeToString(&payload))
    {
        FinishReadIndex(false);
        return;
    }

    for (const RaftPeer& peer : peers_)
    {
        if (peer.id != server_id_)
        {
            SendRpcToPeer(
                peer.id,
                proto::RaftRpc::READ_INDEX,
                payload,
                active_read_index_->request_id);
        }
    }
}

void RaftNode::FinishReadIndex(bool success)
{
    if (!active_read_index_)
    {
        return;
    }
    if (read_index_timer_.valid())
    {
        raft_loop_->cancel(read_index_timer_);
        read_index_timer_ = TimerId();
    }

    const auto active = active_read_index_;
    active_read_index_.reset();
    if (success &&
        role_ == Role::Leader &&
        current_term_ == active->term)
    {
        ApplyCommitted();
        success = last_applied_ >= active->read_index;
    }
    active->result->set_value(success);
    StartNextReadIndex();
}

void RaftNode::FailPendingReadIndexes()
{
    if (read_index_timer_.valid())
    {
        raft_loop_->cancel(read_index_timer_);
        read_index_timer_ = TimerId();
    }
    if (active_read_index_)
    {
        active_read_index_->result->set_value(false);
        active_read_index_.reset();
    }
    while (!pending_read_indexes_.empty())
    {
        pending_read_indexes_.front()->result->set_value(false);
        pending_read_indexes_.pop_front();
    }
}

void RaftNode::SendAppend(int32_t peer_id)
{
    if (role_ != Role::Leader || rpc_inflight_[peer_id])
    {
        return;
    }

    const TcpConnectionPtr connection = ConnectionForPeer(peer_id);
    if (!connection || !connection->connected())
    {
        return;
    }

    if (next_index_[peer_id] <= snapshot_index_)
    {
        SendSnapshot(peer_id);
        return;
    }

    std::uint64_t next = next_index_[peer_id];
    next = std::max<std::uint64_t>(1, std::min(next, LastIndex() + 1));

    proto::AppendEntriesRequest request;
    request.set_term(current_term_);
    request.set_leader_id(server_id_);
    request.set_prev_log_index(next - 1);
    request.set_prev_log_term(TermAt(next - 1));
    for (std::uint64_t index = next;
         index <= LastIndex() &&
         index < next + kMaxEntriesPerRpc;
         ++index)
    {
        const RaftLogEntry& source = log_[LogOffset(index)];
        auto* entry = request.add_entries();
        entry->set_index(index);
        entry->set_term(source.term);
        entry->set_command(source.command);
    }
    request.set_leader_commit(commit_index_);

    std::string payload;
    if (!request.SerializeToString(&payload))
    {
        return;
    }

    rpc_inflight_[peer_id] = true;
    const std::uint64_t generation = ++rpc_generation_[peer_id];
    rpc_has_entries_[peer_id] = request.entries_size() != 0;
    const std::uint64_t request_id = SendRpc(
        connection,
        proto::RaftRpc::APPEND_ENTRIES,
        payload);
    if (request_id == 0)
    {
        ResetPeerRpcState(peer_id, connection);
        return;
    }
    rpc_request_id_[peer_id] = request_id;
    raft_loop_->runAfter(
        0.35,
        [this, peer_id, generation, request_id, connection]()
        {
            if (role_ != Role::Leader ||
                !rpc_inflight_[peer_id] ||
                rpc_generation_[peer_id] != generation ||
                rpc_request_id_[peer_id] != request_id ||
                ConnectionForPeer(peer_id) != connection)
            {
                return;
            }

            ResetPeerRpcState(peer_id, connection);
            if (server_id_ < peer_id)
            {
                const auto client = clients_.find(peer_id);
                if (client != clients_.end())
                {
                    client->second->stop();
                    clients_.erase(client);
                }
                const auto current = outbound_connections_.find(peer_id);
                if (current != outbound_connections_.end() &&
                    current->second == connection)
                {
                    outbound_connections_.erase(current);
                }
                CreatePeerClient(peer_id);
            }
            else
            {
                connection->shutdown();
                const auto current = inbound_connections_.find(peer_id);
                if (current != inbound_connections_.end() &&
                    current->second == connection)
                {
                    inbound_connections_.erase(current);
                }
            }
        });
}

void RaftNode::SendSnapshot(int32_t peer_id)
{
    if (role_ != Role::Leader ||
        rpc_inflight_[peer_id] ||
        snapshot_index_ == 0)
    {
        return;
    }

    const TcpConnectionPtr connection = ConnectionForPeer(peer_id);
    if (!connection || !connection->connected())
    {
        return;
    }

    std::uint64_t offset = rpc_snapshot_offset_[peer_id];
    if (offset > snapshot_data_.size())
    {
        offset = 0;
    }

    const std::size_t remaining =
        snapshot_data_.size() - static_cast<std::size_t>(offset);
    const std::size_t chunk_size =
        std::min<std::size_t>(remaining, kSnapshotChunkSize);

    proto::InstallSnapshotRequest request;
    request.set_term(current_term_);
    request.set_leader_id(server_id_);
    request.set_last_included_index(snapshot_index_);
    request.set_last_included_term(snapshot_term_);
    request.set_offset(offset);
    request.set_data(snapshot_data_.data() + offset, chunk_size);
    request.set_done(chunk_size == remaining);

    std::string payload;
    if (!request.SerializeToString(&payload))
    {
        return;
    }

    rpc_inflight_[peer_id] = true;
    rpc_snapshot_[peer_id] = true;
    const std::uint64_t generation = ++rpc_generation_[peer_id];
    const std::uint64_t request_id = SendRpc(
        connection,
        proto::RaftRpc::INSTALL_SNAPSHOT,
        payload);
    if (request_id == 0)
    {
        ResetPeerRpcState(peer_id, connection);
        return;
    }
    rpc_request_id_[peer_id] = request_id;

    raft_loop_->runAfter(
        0.35,
        [this, peer_id, generation, request_id, connection]()
        {
            if (role_ != Role::Leader ||
                !rpc_inflight_[peer_id] ||
                !rpc_snapshot_[peer_id] ||
                rpc_generation_[peer_id] != generation ||
                rpc_request_id_[peer_id] != request_id ||
                ConnectionForPeer(peer_id) != connection)
            {
                return;
            }

            ResetPeerRpcState(peer_id, connection);
            if (server_id_ < peer_id)
            {
                const auto client = clients_.find(peer_id);
                if (client != clients_.end())
                {
                    client->second->stop();
                    clients_.erase(client);
                }
                const auto current = outbound_connections_.find(peer_id);
                if (current != outbound_connections_.end() &&
                    current->second == connection)
                {
                    outbound_connections_.erase(current);
                }
                CreatePeerClient(peer_id);
            }
            else
            {
                connection->shutdown();
                const auto current = inbound_connections_.find(peer_id);
                if (current != inbound_connections_.end() &&
                    current->second == connection)
                {
                    inbound_connections_.erase(current);
                }
            }
        });
}

void RaftNode::TryAdvanceCommit()
{
    if (role_ != Role::Leader)
    {
        return;
    }

    for (std::uint64_t index = LastIndex();
         index > commit_index_;
         --index)
    {
        if (TermAt(index) != current_term_)
        {
            continue;
        }

        std::size_t replicated = 1;
        for (const auto& [peer_id, match_index] : match_index_)
        {
            (void)peer_id;
            if (match_index >= index)
            {
                ++replicated;
            }
        }

        if (replicated > peers_.size() / 2)
        {
            commit_index_ = index;
            ApplyCommitted();
            return;
        }
    }
}

void RaftNode::HandleConnection(const TcpConnectionPtr& connection)
{
    if (connection->connected())
    {
        return;
    }

    for (auto it = inbound_connections_.begin();
         it != inbound_connections_.end();)
    {
        if (it->second == connection)
        {
            ResetPeerRpcState(it->first, connection);
            it = inbound_connections_.erase(it);
        }
        else
        {
            ++it;
        }
    }

    for (auto it = outbound_connections_.begin();
         it != outbound_connections_.end();)
    {
        if (it->second == connection)
        {
            ResetPeerRpcState(it->first, connection);
            it = outbound_connections_.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void RaftNode::HandleMessage(
    const TcpConnectionPtr& connection,
    Buffer* buffer,
    Timestamp)
{
    while (buffer->readableBytes() >= sizeof(std::uint32_t))
    {
        const std::uint32_t payload_size =
            ReadNetworkUint32(buffer->peek());
        if (payload_size > kMaxFrameSize)
        {
            connection->shutdown();
            return;
        }

        const std::size_t frame_size =
            sizeof(std::uint32_t) + payload_size;
        if (buffer->readableBytes() < frame_size)
        {
            return;
        }

        buffer->retrieve(sizeof(std::uint32_t));
        const std::string frame =
            buffer->retrieveAsString(payload_size);
        HandleRpc(connection, frame);
    }
}

void RaftNode::HandleRpc(
    const TcpConnectionPtr& connection,
    const std::string& frame)
{
    try
    {
        proto::RaftRpc rpc;
        if (!rpc.ParseFromString(frame))
        {
            connection->shutdown();
            return;
        }

        if (rpc.sender_id() != 0 &&
            rpc.sender_id() != static_cast<std::uint64_t>(server_id_))
        {
            const int32_t peer_id =
                static_cast<int32_t>(rpc.sender_id());
            if (server_id_ > peer_id)
            {
                const auto current = inbound_connections_.find(peer_id);
                if (current != inbound_connections_.end() &&
                    current->second != connection)
                {
                    ResetPeerRpcState(peer_id, current->second);
                    current->second->shutdown();
                }
                inbound_connections_[peer_id] = connection;
            }
            else
            {
                const auto current = outbound_connections_.find(peer_id);
                if (current == outbound_connections_.end())
                {
                    outbound_connections_[peer_id] = connection;
                }
                else if (current->second != connection)
                {
                    connection->shutdown();
                    return;
                }
            }
        }

        switch (rpc.type())
        {
            case proto::RaftRpc::IDENTIFY:
                if (role_ == Role::Leader)
                {
                    SendAppend(
                        static_cast<int32_t>(rpc.sender_id()));
                }
                break;
            case proto::RaftRpc::REQUEST_VOTE:
                HandleRequestVote(connection, rpc.payload(), rpc.request_id());
                break;
            case proto::RaftRpc::REQUEST_VOTE_RESPONSE:
                HandleRequestVoteResponse(
                    connection,
                    rpc.payload(),
                    rpc.request_id());
                break;
            case proto::RaftRpc::APPEND_ENTRIES:
                HandleAppendEntries(
                    connection,
                    rpc.payload(),
                    rpc.request_id());
                break;
            case proto::RaftRpc::APPEND_ENTRIES_RESPONSE:
                HandleAppendEntriesResponse(
                    connection,
                    rpc.payload(),
                    rpc.request_id());
                break;
            case proto::RaftRpc::INSTALL_SNAPSHOT:
                HandleInstallSnapshot(
                    connection,
                    rpc.payload(),
                    rpc.request_id());
                break;
            case proto::RaftRpc::INSTALL_SNAPSHOT_RESPONSE:
                HandleInstallSnapshotResponse(
                    connection,
                    rpc.payload(),
                    rpc.request_id());
                break;
            case proto::RaftRpc::READ_INDEX:
                HandleReadIndexRequest(
                    connection,
                    rpc.payload(),
                    rpc.request_id());
                break;
            case proto::RaftRpc::READ_INDEX_RESPONSE:
                HandleReadIndexResponse(
                    connection,
                    rpc.payload(),
                    rpc.request_id());
                break;
            case proto::RaftRpc::TYPE_UNSPECIFIED:
            default:
                connection->shutdown();
                break;
        }
    }
    catch (const std::exception&)
    {
        connection->shutdown();
    }
}

void RaftNode::HandleRequestVote(
    const TcpConnectionPtr& connection,
    const std::string& payload,
    std::uint64_t request_id)
{
    proto::RequestVoteRequest request;
    if (!request.ParseFromString(payload))
    {
        connection->shutdown();
        return;
    }

    if (request.term() > current_term_)
    {
        StepDown(request.term());
    }

    const bool grant =
        request.term() == current_term_ &&
        (voted_for_ == -1 ||
         voted_for_ == static_cast<std::int64_t>(request.candidate_id())) &&
        IsCandidateLogUpToDate(
            request.last_log_index(),
            request.last_log_term());

    if (grant)
    {
        voted_for_ = static_cast<std::int64_t>(request.candidate_id());
        PersistState();
        ResetElectionTimer();
    }

    proto::RequestVoteResponse response;
    response.set_term(current_term_);
    response.set_vote_granted(grant);
    response.set_responder_id(server_id_);

    std::string response_payload;
    if (response.SerializeToString(&response_payload))
    {
        SendRpc(
            connection,
            proto::RaftRpc::REQUEST_VOTE_RESPONSE,
            response_payload,
            request_id);
    }
}

void RaftNode::HandleRequestVoteResponse(
    const TcpConnectionPtr& connection,
    const std::string& payload,
    std::uint64_t request_id)
{
    proto::RequestVoteResponse response;
    if (!response.ParseFromString(payload))
    {
        return;
    }

    if (response.term() > current_term_)
    {
        StepDown(response.term());
        return;
    }
    if (role_ != Role::Candidate ||
        response.term() != current_term_ ||
        request_id == 0 ||
        request_id != vote_request_id_[static_cast<int32_t>(response.responder_id())] ||
        ConnectionForPeer(static_cast<int32_t>(response.responder_id())) != connection ||
        !response.vote_granted() ||
        !granted_votes_.insert(
            static_cast<int32_t>(response.responder_id())).second)
    {
        return;
    }

    votes_received_ = granted_votes_.size();
    if (votes_received_ >= peers_.size() / 2 + 1)
    {
        BecomeLeader();
    }
}

void RaftNode::HandleAppendEntries(
    const TcpConnectionPtr& connection,
    const std::string& payload,
    std::uint64_t request_id)
{
    proto::AppendEntriesRequest request;
    if (!request.ParseFromString(payload))
    {
        connection->shutdown();
        return;
    }

    proto::AppendEntriesResponse response;
    response.set_responder_id(server_id_);

    if (request.term() < current_term_)
    {
        response.set_term(current_term_);
        response.set_success(false);
        response.set_match_index(LastIndex());
    }
    else
    {
        if (request.term() > current_term_ ||
            role_ != Role::Follower)
        {
            StepDown(request.term());
        }
        ResetElectionTimer();

        bool success =
            request.prev_log_index() >= snapshot_index_ &&
            request.prev_log_index() <= LastIndex() &&
            TermAt(request.prev_log_index()) == request.prev_log_term();
        std::uint64_t index = request.prev_log_index();

        if (success)
        {
            for (const auto& incoming : request.entries())
            {
                ++index;
                if (incoming.index() != index)
                {
                    success = false;
                    break;
                }

                if (index <= LastIndex())
                {
                    if (TermAt(index) != incoming.term())
                    {
                        if (index <= commit_index_)
                        {
                            success = false;
                            break;
                        }
                        log_.resize(LogOffset(index));
                    }
                }

                if (success && index > LastIndex())
                {
                    log_.push_back(
                        {incoming.term(), incoming.command()});
                }
            }

            if (success)
            {
                PersistLog();
                commit_index_ = std::min(
                    request.leader_commit(),
                    LastIndex());
                ApplyCommitted();
            }
        }

        response.set_term(current_term_);
        response.set_success(success);
        response.set_match_index(success ? index : LastIndex());
    }

    std::string response_payload;
    if (response.SerializeToString(&response_payload))
    {
        SendRpc(
            connection,
            proto::RaftRpc::APPEND_ENTRIES_RESPONSE,
            response_payload,
            request_id);
    }
}

void RaftNode::HandleInstallSnapshot(
    const TcpConnectionPtr& connection,
    const std::string& payload,
    std::uint64_t request_id)
{
    proto::InstallSnapshotRequest request;
    if (!request.ParseFromString(payload))
    {
        connection->shutdown();
        return;
    }

    proto::InstallSnapshotResponse response;
    response.set_responder_id(server_id_);
    std::uint64_t next_offset = 0;

    if (request.term() < current_term_)
    {
        response.set_term(current_term_);
        response.set_success(false);
        next_offset = incoming_snapshot_active_
            ? incoming_snapshot_data_.size()
            : 0;
    }
    else
    {
        if (request.term() > current_term_ ||
            role_ != Role::Follower)
        {
            StepDown(request.term());
        }
        ResetElectionTimer();

        const bool valid_chunk =
            request.last_included_index() != 0 &&
            request.last_included_term() != 0 &&
            request.data().size() <= kSnapshotChunkSize &&
            request.offset() <=
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) &&
            request.offset() <=
                std::numeric_limits<std::uint64_t>::max() -
                    request.data().size();

        if (!valid_chunk)
        {
            response.set_term(current_term_);
            response.set_success(false);
            response.set_next_offset(0);
        }
        else if (request.last_included_index() == snapshot_index_ &&
                 request.last_included_term() == snapshot_term_)
        {
            response.set_term(current_term_);
            response.set_success(true);
            response.set_next_offset(snapshot_data_.size());
        }
        else
        {
        bool success =
            request.last_included_index() > snapshot_index_ &&
            request.last_included_term() != 0;

        if (success)
        {
            if (!incoming_snapshot_active_ ||
                incoming_snapshot_index_ != request.last_included_index() ||
                incoming_snapshot_term_ != request.last_included_term() ||
                request.offset() == 0)
            {
                if (request.offset() != 0)
                {
                    success = false;
                }
                else
                {
                    incoming_snapshot_index_ = request.last_included_index();
                    incoming_snapshot_term_ = request.last_included_term();
                    incoming_snapshot_data_.clear();
                    incoming_snapshot_active_ = true;
                }
            }

            if (success &&
                request.offset() != incoming_snapshot_data_.size())
            {
                success = false;
            }

            if (success)
            {
                incoming_snapshot_data_.append(request.data());
                next_offset = incoming_snapshot_data_.size();
                if (request.done())
                {
                    try
                    {
                        const SnapshotContents snapshot =
                            ParseSnapshotState(incoming_snapshot_data_);

                        std::vector<RaftLogEntry> suffix;
                        if (incoming_snapshot_index_ <= LastIndex() &&
                            TermAt(incoming_snapshot_index_) ==
                                incoming_snapshot_term_)
                        {
                            const std::size_t suffix_begin =
                                incoming_snapshot_index_ == LastIndex()
                                    ? log_.size()
                                    : LogOffset(incoming_snapshot_index_ + 1);
                            suffix.assign(
                                log_.begin() + suffix_begin,
                                log_.end());
                        }

                        const std::uint64_t new_commit_index =
                            std::max(commit_index_, incoming_snapshot_index_);
                        raft_storage_.ApplySnapshot(
                            snapshot,
                            current_term_,
                            voted_for_,
                            new_commit_index,
                            incoming_snapshot_index_,
                            incoming_snapshot_index_,
                            incoming_snapshot_term_,
                            suffix);

                        snapshot_index_ = incoming_snapshot_index_;
                        snapshot_term_ = incoming_snapshot_term_;
                        snapshot_data_ = incoming_snapshot_data_;
                        log_ = std::move(suffix);
                        commit_index_ = new_commit_index;
                        last_applied_ = snapshot_index_;
                        ApplyCommitted();

                        next_offset = incoming_snapshot_data_.size();
                        incoming_snapshot_data_.clear();
                        incoming_snapshot_active_ = false;
                    }
                    catch (const std::exception&)
                    {
                        success = false;
                        incoming_snapshot_data_.clear();
                        incoming_snapshot_active_ = false;
                        next_offset = 0;
                    }
                }
            }
        }

        response.set_term(current_term_);
        response.set_success(success);
        response.set_next_offset(next_offset);
        }
    }

    if (response.next_offset() == 0)
    {
        response.set_next_offset(next_offset);
    }

    std::string response_payload;
    if (response.SerializeToString(&response_payload))
    {
        SendRpc(
            connection,
            proto::RaftRpc::INSTALL_SNAPSHOT_RESPONSE,
            response_payload,
            request_id);
    }
}

void RaftNode::HandleInstallSnapshotResponse(
    const TcpConnectionPtr& connection,
    const std::string& payload,
    std::uint64_t request_id)
{
    proto::InstallSnapshotResponse response;
    if (!response.ParseFromString(payload))
    {
        return;
    }

    if (response.term() > current_term_)
    {
        StepDown(response.term());
        return;
    }
    if (role_ != Role::Leader ||
        response.term() != current_term_)
    {
        return;
    }

    const int32_t peer_id =
        static_cast<int32_t>(response.responder_id());
    if (request_id == 0 ||
        next_index_.find(peer_id) == next_index_.end() ||
        request_id != rpc_request_id_[peer_id] ||
        !rpc_snapshot_[peer_id] ||
        ConnectionForPeer(peer_id) != connection)
    {
        return;
    }

    if (response.next_offset() > snapshot_data_.size())
    {
        ResetPeerRpcState(peer_id, connection);
        rpc_snapshot_offset_[peer_id] = 0;
        SendSnapshot(peer_id);
        return;
    }

    const bool success = response.success();
    ResetPeerRpcState(peer_id, connection);
    if (!success)
    {
        rpc_snapshot_offset_[peer_id] = response.next_offset();
        SendSnapshot(peer_id);
        return;
    }

    const std::uint64_t next_offset = response.next_offset();
    if (next_offset < snapshot_data_.size())
    {
        rpc_snapshot_offset_[peer_id] = next_offset;
        SendSnapshot(peer_id);
        return;
    }

    rpc_snapshot_offset_[peer_id] = 0;
    match_index_[peer_id] = snapshot_index_;
    next_index_[peer_id] = snapshot_index_ + 1;
    SendAppend(peer_id);
}

void RaftNode::HandleReadIndexRequest(
    const TcpConnectionPtr& connection,
    const std::string& payload,
    std::uint64_t request_id)
{
    proto::ReadIndexRequest request;
    if (!request.ParseFromString(payload))
    {
        connection->shutdown();
        return;
    }

    proto::ReadIndexResponse response;
    response.set_responder_id(server_id_);
    if (request.term() < current_term_)
    {
        response.set_term(current_term_);
        response.set_success(false);
    }
    else
    {
        if (request.term() > current_term_ ||
            role_ != Role::Follower)
        {
            StepDown(request.term());
        }
        ResetElectionTimer();
        response.set_term(current_term_);
        response.set_success(request.term() == current_term_);
    }

    std::string response_payload;
    if (response.SerializeToString(&response_payload))
    {
        SendRpc(
            connection,
            proto::RaftRpc::READ_INDEX_RESPONSE,
            response_payload,
            request_id);
    }
}

void RaftNode::HandleReadIndexResponse(
    const TcpConnectionPtr& connection,
    const std::string& payload,
    std::uint64_t request_id)
{
    proto::ReadIndexResponse response;
    if (!response.ParseFromString(payload))
    {
        return;
    }
    if (response.term() > current_term_)
    {
        StepDown(response.term());
        return;
    }
    if (!active_read_index_ ||
        role_ != Role::Leader ||
        response.term() != current_term_ ||
        request_id != active_read_index_->request_id)
    {
        return;
    }

    const int32_t peer_id =
        static_cast<int32_t>(response.responder_id());
    if (next_index_.find(peer_id) == next_index_.end() ||
        ConnectionForPeer(peer_id) != connection ||
        !response.success())
    {
        return;
    }
    active_read_index_->acknowledgements.insert(peer_id);
    if (active_read_index_->acknowledgements.size() >=
        peers_.size() / 2 + 1)
    {
        FinishReadIndex(true);
    }
}

void RaftNode::HandleAppendEntriesResponse(
    const TcpConnectionPtr& connection,
    const std::string& payload,
    std::uint64_t request_id)
{
    proto::AppendEntriesResponse response;
    if (!response.ParseFromString(payload))
    {
        return;
    }

    if (response.term() > current_term_)
    {
        StepDown(response.term());
        return;
    }
    if (role_ != Role::Leader ||
        response.term() != current_term_)
    {
        return;
    }

    const int32_t peer_id =
        static_cast<int32_t>(response.responder_id());
    if (request_id == 0 ||
        request_id != rpc_request_id_[peer_id] ||
        ConnectionForPeer(peer_id) != connection)
    {
        return;
    }

    const bool had_entries = rpc_has_entries_[peer_id];
    ResetPeerRpcState(peer_id, connection);
    if (response.success())
    {
        match_index_[peer_id] =
            std::min(response.match_index(), LastIndex());
        next_index_[peer_id] = match_index_[peer_id] + 1;
        TryAdvanceCommit();
    }
    else if (next_index_[peer_id] > snapshot_index_ + 1)
    {
        --next_index_[peer_id];
    }
    if (!response.success() ||
        (had_entries && next_index_[peer_id] <= LastIndex()))
    {
        SendAppend(peer_id);
    }
}

void RaftNode::OnPeerConnection(
    int32_t peer_id,
    const TcpConnectionPtr& connection)
{
    if (connection->connected())
    {
        const auto current = outbound_connections_.find(peer_id);
        if (current != outbound_connections_.end() &&
            current->second != connection)
        {
            ResetPeerRpcState(peer_id, current->second);
            current->second->shutdown();
        }
        outbound_connections_[peer_id] = connection;
        ResetPeerRpcState(peer_id, connection);

        if (role_ == Role::Leader)
        {
            SendAppend(peer_id);
        }
        else if (role_ == Role::Candidate)
        {
            SendRequestVoteToPeer(peer_id);
        }
        if (active_read_index_)
        {
            SendReadIndexProbes();
        }
        SendRpc(connection, proto::RaftRpc::IDENTIFY, {});
    }
    else
    {
        const auto current = outbound_connections_.find(peer_id);
        if (current != outbound_connections_.end() &&
            current->second == connection)
        {
            ResetPeerRpcState(peer_id, connection);
            outbound_connections_.erase(current);
        }
    }
}

std::uint64_t RaftNode::SendRpc(
    const TcpConnectionPtr& connection,
    int type,
    const std::string& payload,
    std::uint64_t request_id)
{
    if (!connection || !connection->connected())
    {
        return 0;
    }

    proto::RaftRpc rpc;
    rpc.set_type(static_cast<proto::RaftRpc::Type>(type));
    if (request_id == 0)
    {
        request_id = next_rpc_id_++;
    }
    rpc.set_request_id(request_id);
    rpc.set_sender_id(server_id_);
    rpc.set_payload(payload);

    std::string serialized;
    if (rpc.SerializeToString(&serialized))
    {
        connection->send(Frame(serialized));
        return request_id;
    }
    return 0;
}

std::uint64_t RaftNode::SendRpcToPeer(
    int32_t peer_id,
    int type,
    const std::string& payload,
    std::uint64_t request_id)
{
    const TcpConnectionPtr connection = ConnectionForPeer(peer_id);
    if (!connection || !connection->connected())
    {
        return 0;
    }

    return SendRpc(connection, type, payload, request_id);
}

void RaftNode::ApplyCommitted()
{
    CommandApplier applier(store_);
    if (last_applied_ < snapshot_index_)
    {
        last_applied_ = snapshot_index_;
    }
    while (last_applied_ < commit_index_)
    {
        const RaftLogEntry& entry = log_[LogOffset(last_applied_ + 1)];
        proto::Command command;
        if (!command.ParseFromString(entry.command))
        {
            throw std::runtime_error("invalid committed Raft command");
        }
        applier.ApplyAtomically(
            command,
            last_applied_ + 1,
            commit_index_);
        ++last_applied_;
    }
    PersistState();
    CreateSnapshotIfNeeded();
}

void RaftNode::CreateSnapshotIfNeeded()
{
    if (snapshot_in_progress_ ||
        commit_index_ <= snapshot_index_ ||
        commit_index_ - snapshot_index_ < kSnapshotThreshold)
    {
        return;
    }

    snapshot_in_progress_ = true;
    try
    {
        const std::uint64_t new_snapshot_index = commit_index_;
        const std::uint64_t new_snapshot_term =
            TermAt(new_snapshot_index);
        if (new_snapshot_term == 0)
        {
            throw std::runtime_error("cannot snapshot an empty Raft term");
        }

        const auto entries = store_.GetAllUserEntries();
        const auto applied_requests = raft_storage_.GetAppliedRequests();
        const std::string metadata = SerializeSnapshotMetadata(
            new_snapshot_index,
            new_snapshot_term);
        const std::string data =
            SerializeSnapshotState(entries, applied_requests);

        const std::size_t erase_count = static_cast<std::size_t>(
            new_snapshot_index - snapshot_index_);
        if (erase_count > log_.size())
        {
            throw std::runtime_error("snapshot exceeds retained Raft log");
        }
        std::vector<RaftLogEntry> suffix(
            log_.begin() + erase_count,
            log_.end());

        raft_storage_.PersistSnapshot(
            current_term_,
            voted_for_,
            commit_index_,
            last_applied_,
            new_snapshot_index,
            new_snapshot_term,
            metadata,
            data,
            suffix);

        snapshot_index_ = new_snapshot_index;
        snapshot_term_ = new_snapshot_term;
        snapshot_data_ = data;
        log_ = std::move(suffix);

    }
    catch (...)
    {
        snapshot_in_progress_ = false;
        throw;
    }
    snapshot_in_progress_ = false;
}

bool RaftNode::Submit(const proto::Command& command)
{
    std::string serialized;
    if (!command.SerializeToString(&serialized) ||
        raft_loop_ == nullptr ||
        !IsReady())
    {
        return false;
    }

    auto result = std::make_shared<std::promise<bool>>();
    std::future<bool> future = result->get_future();
    const std::uint64_t client_id = command.client_id();
    const std::uint64_t request_id = command.request_id();

    raft_loop_->queueInLoop(
        [this,
         serialized = std::move(serialized),
         client_id,
         request_id,
         result]()
        {
            try
            {
                if (role_ != Role::Leader || stopping_)
                {
                    result->set_value(false);
                    return;
                }

                if (request_id != 0)
                {
                    const auto applied =
                        raft_storage_.GetAppliedCommand(client_id, request_id);
                    if (applied.has_value())
                    {
                        result->set_value(applied.value() == serialized);
                        return;
                    }
                }

                log_.push_back({current_term_, serialized});
                PersistLog();
                const std::uint64_t target = LastIndex();

                TryAdvanceCommit();
                for (const RaftPeer& peer : peers_)
                {
                    if (peer.id != server_id_)
                    {
                        SendAppend(peer.id);
                    }
                }

                auto wait_for_commit =
                    std::make_shared<std::function<void()>>();
                *wait_for_commit =
                    [this, target, result, wait_for_commit]()
                    {
                        if (stopping_ || role_ != Role::Leader)
                        {
                            result->set_value(false);
                            return;
                        }
                        if (commit_index_ >= target)
                        {
                            result->set_value(true);
                            return;
                        }
                        raft_loop_->runAfter(0.01, *wait_for_commit);
                    };
                (*wait_for_commit)();
            }
            catch (const std::exception&)
            {
                result->set_value(false);
            }
        });

    if (future.wait_for(std::chrono::seconds(2)) !=
        std::future_status::ready)
    {
        return false;
    }
    return future.get();
}

bool RaftNode::ReadIndex()
{
    if (raft_loop_ == nullptr || !IsReady())
    {
        return false;
    }

    auto result = std::make_shared<std::promise<bool>>();
    std::future<bool> future = result->get_future();
    raft_loop_->queueInLoop(
        [this, result]()
        {
            if (role_ != Role::Leader || stopping_)
            {
                result->set_value(false);
                return;
            }

            auto request = std::make_shared<PendingReadIndex>();
            request->request_id = next_rpc_id_++;
            request->result = result;
            pending_read_indexes_.push_back(std::move(request));
            StartNextReadIndex();
        });

    if (future.wait_for(std::chrono::seconds(2)) !=
        std::future_status::ready)
    {
        return false;
    }
    return future.get();
}

std::optional<std::string> RaftNode::GetLocal(const std::string& key) const
{
    return store_.Get(key);
}

bool RaftNode::IsCandidateLogUpToDate(
    std::uint64_t index,
    std::uint64_t term) const
{
    return term > LastTerm() ||
           (term == LastTerm() && index >= LastIndex());
}
}
