#pragma once

#include "TimerQueue.h"
#include "rocks_kv_store.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class Buffer;
class EventLoop;
class TcpClient;
class TcpConnection;
class TcpServer;
class Timestamp;

using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

namespace nukv
{
namespace proto
{
class Command;
}

struct RaftPeer
{
    int32_t id;
    std::string endpoint;
};

class RaftNode final
{
public:
    RaftNode(
        int32_t server_id,
        std::string endpoint,
        int32_t listen_port,
        std::string db_path,
        std::string metadata_path,
        std::vector<RaftPeer> peers);

    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    void Start();
    void Stop();

    bool IsReady() const { return ready_.load(std::memory_order_acquire); }
    bool IsLeader() const { return leader_.load(std::memory_order_acquire); }

    bool Submit(const proto::Command& command);
    std::optional<std::string> GetLocal(const std::string& key) const;

private:
    struct LogEntry
    {
        std::uint64_t term;
        std::string command;
    };

    enum class Role
    {
        Follower,
        Candidate,
        Leader
    };

    void StartInLoop();
    void StopInLoop();
    void MarkStartFailure(std::exception_ptr error);

    void ResetElectionTimer();
    void BecomeCandidate();
    void BecomeLeader();
    void StepDown(std::uint64_t term);
    void SendRequestVotes();
    void SendRequestVoteToPeer(int32_t peer_id);
    void SendHeartbeats();
    void SendAppend(int32_t peer_id);
    void SendSnapshot(int32_t peer_id);
    void TryAdvanceCommit();
    void CreateSnapshotIfNeeded();

    void HandleConnection(const TcpConnectionPtr& connection);
    void HandleMessage(
        const TcpConnectionPtr& connection,
        Buffer* buffer,
        Timestamp timestamp);
    void HandleRpc(
        const TcpConnectionPtr& connection,
        const std::string& frame);
    void HandleRequestVote(
        const TcpConnectionPtr& connection,
        const std::string& payload,
        std::uint64_t request_id);
    void HandleRequestVoteResponse(
        const TcpConnectionPtr& connection,
        const std::string& payload,
        std::uint64_t request_id);
    void HandleAppendEntries(
        const TcpConnectionPtr& connection,
        const std::string& payload,
        std::uint64_t request_id);
    void HandleAppendEntriesResponse(
        const TcpConnectionPtr& connection,
        const std::string& payload,
        std::uint64_t request_id);
    void HandleInstallSnapshot(
        const TcpConnectionPtr& connection,
        const std::string& payload,
        std::uint64_t request_id);
    void HandleInstallSnapshotResponse(
        const TcpConnectionPtr& connection,
        const std::string& payload,
        std::uint64_t request_id);
    void OnPeerConnection(
        int32_t peer_id,
        const TcpConnectionPtr& connection);

    std::uint64_t SendRpc(
        const TcpConnectionPtr& connection,
        int type,
        const std::string& payload,
        std::uint64_t request_id = 0);
    std::uint64_t SendRpcToPeer(
        int32_t peer_id,
        int type,
        const std::string& payload);
    void CreatePeerClient(int32_t peer_id);
    TcpConnectionPtr ConnectionForPeer(int32_t peer_id) const;
    void ResetPeerRpcState(
        int32_t peer_id,
        const TcpConnectionPtr& connection);

    void ApplyCommitted();
    void PersistState();
    void LoadState();
    void PersistLog();

    std::uint64_t LastIndex() const;
    std::uint64_t LastTerm() const;
    std::uint64_t TermAt(std::uint64_t index) const;
    std::size_t LogOffset(std::uint64_t index) const;
    bool IsCandidateLogUpToDate(
        std::uint64_t index,
        std::uint64_t term) const;

    int32_t server_id_;
    std::string endpoint_;
    int32_t listen_port_;
    std::vector<RaftPeer> peers_;

    RocksKVStore store_;
    RocksKVStore metadata_store_;

    EventLoop* raft_loop_{nullptr};
    std::thread raft_thread_;
    std::unique_ptr<TcpServer> server_;
    std::unordered_map<int32_t, std::unique_ptr<TcpClient>> clients_;
    std::unordered_map<int32_t, TcpConnectionPtr> outbound_connections_;
    std::unordered_map<int32_t, TcpConnectionPtr> inbound_connections_;

    std::vector<LogEntry> log_;
    std::uint64_t snapshot_index_{0};
    std::uint64_t snapshot_term_{0};
    std::string snapshot_data_;
    std::uint64_t incoming_snapshot_index_{0};
    std::uint64_t incoming_snapshot_term_{0};
    std::string incoming_snapshot_data_;
    bool incoming_snapshot_active_{false};
    bool snapshot_in_progress_{false};
    std::uint64_t current_term_{0};
    std::int64_t voted_for_{-1};
    std::uint64_t commit_index_{0};
    std::uint64_t last_applied_{0};

    std::unordered_map<int32_t, std::uint64_t> next_index_;
    std::unordered_map<int32_t, std::uint64_t> match_index_;
    std::unordered_map<int32_t, bool> rpc_inflight_;
    std::unordered_map<int32_t, std::uint64_t> rpc_generation_;
    std::unordered_map<int32_t, std::uint64_t> rpc_request_id_;
    std::unordered_map<int32_t, bool> rpc_has_entries_;
    std::unordered_map<int32_t, bool> rpc_snapshot_;
    std::unordered_map<int32_t, std::uint64_t> rpc_snapshot_offset_;
    std::unordered_map<int32_t, std::uint64_t> vote_request_id_;
    std::unordered_set<int32_t> granted_votes_;

    std::uint64_t next_rpc_id_{1};
    std::size_t votes_received_{0};
    Role role_{Role::Follower};

    TimerId election_timer_;
    TimerId heartbeat_timer_;

    std::atomic_bool ready_{false};
    std::atomic_bool leader_{false};
    std::atomic_bool stopping_{false};

    std::mutex lifecycle_mutex_;
    std::condition_variable lifecycle_condition_;
    bool startup_finished_{false};
    std::exception_ptr startup_error_;
};
}
