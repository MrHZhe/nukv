#pragma once

#include "rocks_kv_store.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace nukv
{
struct RaftLogEntry
{
    std::uint64_t term;
    std::string command;
};

struct PersistedRaftState
{
    std::uint64_t current_term{0};
    std::int64_t voted_for{-1};
    std::uint64_t commit_index{0};
    std::uint64_t last_applied{0};
    std::uint64_t snapshot_index{0};
    std::uint64_t snapshot_term{0};
    std::string snapshot_data;
    std::vector<RaftLogEntry> log;
};

struct SnapshotContents
{
    std::vector<std::pair<std::string, std::string>> entries;
    std::vector<std::pair<std::string, std::string>> applied_requests;
};

std::string AppliedRequestKey(
    std::uint64_t client_id,
    std::uint64_t request_id);

std::string SerializeSnapshotMetadata(
    std::uint64_t last_included_index,
    std::uint64_t last_included_term);

std::string SerializeSnapshotState(
    const std::vector<std::pair<std::string, std::string>>& entries,
    const std::vector<std::pair<std::string, std::string>>& applied_requests);

SnapshotContents ParseSnapshotState(
    const std::string& serialized);

class RaftStorage final
{
public:
    explicit RaftStorage(RocksKVStore& store);

    PersistedRaftState Load() const;

    void PersistState(
        std::uint64_t current_term,
        std::int64_t voted_for,
        std::uint64_t commit_index,
        std::uint64_t last_applied,
        std::uint64_t snapshot_index,
        std::uint64_t snapshot_term);

    void PersistLog(
        std::uint64_t snapshot_index,
        const std::vector<RaftLogEntry>& log);

    std::optional<std::string> GetAppliedCommand(
        std::uint64_t client_id,
        std::uint64_t request_id) const;

    std::vector<std::pair<std::string, std::string>>
    GetAppliedRequests() const;

    void PersistSnapshot(
        std::uint64_t current_term,
        std::int64_t voted_for,
        std::uint64_t commit_index,
        std::uint64_t last_applied,
        std::uint64_t snapshot_index,
        std::uint64_t snapshot_term,
        const std::string& metadata,
        const std::string& data,
        const std::vector<RaftLogEntry>& log);

    void ApplySnapshot(
        const SnapshotContents& snapshot,
        std::uint64_t current_term,
        std::int64_t voted_for,
        std::uint64_t commit_index,
        std::uint64_t last_applied,
        std::uint64_t snapshot_index,
        std::uint64_t snapshot_term,
        const std::vector<RaftLogEntry>& log);

private:
    RocksKVStore& store_;
};
}
