#include "raft_storage.hpp"

#include "raft.pb.h"

#include <limits>
#include <stdexcept>
#include <unordered_set>

namespace
{
std::uint64_t ParseUint64(
    const std::optional<std::string>& value,
    const std::string& key)
{
    if (!value.has_value())
    {
        return 0;
    }

    std::size_t parsed = 0;
    const std::uint64_t result = std::stoull(value.value(), &parsed);
    if (parsed != value->size())
    {
        throw std::runtime_error("invalid persisted Raft value: " + key);
    }
    return result;
}

std::int64_t ParseInt64(
    const std::optional<std::string>& value,
    const std::string& key)
{
    if (!value.has_value())
    {
        return -1;
    }

    std::size_t parsed = 0;
    const std::int64_t result = std::stoll(value.value(), &parsed);
    if (parsed != value->size())
    {
        throw std::runtime_error("invalid persisted Raft value: " + key);
    }
    return result;
}
}

namespace nukv
{
std::string AppliedRequestKey(
    std::uint64_t client_id,
    std::uint64_t request_id)
{
    return "__raft/client/" +
        std::to_string(client_id) + "/" +
        std::to_string(request_id);
}

std::string SerializeSnapshotMetadata(
    std::uint64_t last_included_index,
    std::uint64_t last_included_term)
{
    proto::SnapshotMetadata metadata;
    metadata.set_last_included_index(last_included_index);
    metadata.set_last_included_term(last_included_term);

    std::string serialized;
    if (!metadata.SerializeToString(&serialized))
    {
        throw std::runtime_error("failed to serialize Raft snapshot metadata");
    }
    return serialized;
}

std::string SerializeSnapshotState(
    const std::vector<std::pair<std::string, std::string>>& entries,
    const std::vector<std::pair<std::string, std::string>>& applied_requests)
{
    proto::SnapshotState state;
    for (const auto& [key, value] : entries)
    {
        auto* entry = state.add_entries();
        entry->set_key(key);
        entry->set_value(value);
    }
    for (const auto& [key, value] : applied_requests)
    {
        auto* entry = state.add_applied_requests();
        entry->set_key(key);
        entry->set_value(value);
    }

    std::string serialized;
    if (!state.SerializeToString(&serialized))
    {
        throw std::runtime_error("failed to serialize Raft snapshot state");
    }
    return serialized;
}

SnapshotContents ParseSnapshotState(
    const std::string& serialized)
{
    proto::SnapshotState state;
    if (!state.ParseFromString(serialized))
    {
        throw std::runtime_error("invalid Raft snapshot state");
    }

    SnapshotContents contents;
    contents.entries.reserve(state.entries_size());
    contents.applied_requests.reserve(state.applied_requests_size());
    std::unordered_set<std::string> keys;
    keys.reserve(state.entries_size());

    for (const auto& entry : state.entries())
    {
        if (entry.key().rfind("__raft/", 0) == 0 ||
            !keys.insert(entry.key()).second)
        {
            throw std::runtime_error("invalid Raft snapshot key set");
        }
        contents.entries.emplace_back(entry.key(), entry.value());
    }
    std::unordered_set<std::string> applied_keys;
    applied_keys.reserve(state.applied_requests_size());
    for (const auto& entry : state.applied_requests())
    {
        if (entry.key().empty() ||
            entry.key().find('/') == std::string::npos ||
            !applied_keys.insert(entry.key()).second)
        {
            throw std::runtime_error("invalid applied request set");
        }
        contents.applied_requests.emplace_back(entry.key(), entry.value());
    }
    return contents;
}

RaftStorage::RaftStorage(RocksKVStore& store)
    : store_(store)
{}

PersistedRaftState RaftStorage::Load() const
{
    PersistedRaftState state;
    const auto persisted_snapshot = store_.LoadSnapshot();
    if (persisted_snapshot.has_value())
    {
        proto::SnapshotMetadata metadata;
        if (!metadata.ParseFromString(persisted_snapshot->first) ||
            metadata.last_included_index() == 0 ||
            metadata.last_included_term() == 0)
        {
            throw std::runtime_error("invalid persisted Raft snapshot metadata");
        }
        ParseSnapshotState(persisted_snapshot->second);
        state.snapshot_index = metadata.last_included_index();
        state.snapshot_term = metadata.last_included_term();
        state.snapshot_data = persisted_snapshot->second;
    }

    state.current_term = ParseUint64(
        store_.Get("__raft/current_term"),
        "__raft/current_term");
    state.voted_for = ParseInt64(
        store_.Get("__raft/voted_for"),
        "__raft/voted_for");
    state.commit_index = ParseUint64(
        store_.Get("__raft/commit_index"),
        "__raft/commit_index");
    state.last_applied = ParseUint64(
        store_.Get("__raft/last_applied"),
        "__raft/last_applied");

    for (std::uint64_t index = state.snapshot_index + 1; ; ++index)
    {
        const auto serialized = store_.Get(
            "__raft/log/" + std::to_string(index));
        if (!serialized.has_value())
        {
            break;
        }

        proto::RaftLogEntry entry;
        if (!entry.ParseFromString(serialized.value()) ||
            entry.index() != index)
        {
            throw std::runtime_error("corrupted persisted Raft log");
        }

        state.log.push_back({entry.term(), entry.command()});
        if (index == std::numeric_limits<std::uint64_t>::max())
        {
            throw std::runtime_error("Raft log index overflow");
        }
    }
    return state;
}

std::optional<std::string> RaftStorage::GetAppliedCommand(
    std::uint64_t client_id,
    std::uint64_t request_id) const
{
    return store_.Get(AppliedRequestKey(client_id, request_id));
}

std::vector<std::pair<std::string, std::string>>
RaftStorage::GetAppliedRequests() const
{
    constexpr const char* prefix = "__raft/client/";
    const auto persisted = store_.GetAllPrefixedEntries(prefix);
    std::vector<std::pair<std::string, std::string>> requests;
    requests.reserve(persisted.size());
    for (const auto& [key, value] : persisted)
    {
        requests.emplace_back(key.substr(std::char_traits<char>::length(prefix)), value);
    }
    return requests;
}

void RaftStorage::PersistState(
    std::uint64_t current_term,
    std::int64_t voted_for,
    std::uint64_t commit_index,
    std::uint64_t last_applied,
    std::uint64_t snapshot_index,
    std::uint64_t snapshot_term)
{
    store_.WriteAtomically(
        {
            {"__raft/current_term", std::to_string(current_term)},
            {"__raft/voted_for", std::to_string(voted_for)},
            {"__raft/commit_index", std::to_string(commit_index)},
            {"__raft/last_applied", std::to_string(last_applied)},
            {"__raft/snapshot_index", std::to_string(snapshot_index)},
            {"__raft/snapshot_term", std::to_string(snapshot_term)}
        },
        {});
}

void RaftStorage::PersistLog(
    std::uint64_t snapshot_index,
    const std::vector<RaftLogEntry>& log)
{
    std::vector<std::pair<std::string, std::string>> puts;
    puts.reserve(log.size());

    for (std::size_t offset = 0; offset < log.size(); ++offset)
    {
        proto::RaftLogEntry entry;
        const std::uint64_t index =
            snapshot_index + static_cast<std::uint64_t>(offset + 1);
        entry.set_index(index);
        entry.set_term(log[offset].term);
        entry.set_command(log[offset].command);

        std::string serialized;
        if (!entry.SerializeToString(&serialized))
        {
            throw std::runtime_error("failed to serialize Raft log entry");
        }
        puts.emplace_back(
            "__raft/log/" + std::to_string(index),
            std::move(serialized));
    }

    std::vector<std::string> deletes;
    for (std::uint64_t index = 1; index <= snapshot_index; ++index)
    {
        deletes.push_back("__raft/log/" + std::to_string(index));
    }
    const std::uint64_t last_index =
        snapshot_index + static_cast<std::uint64_t>(log.size());
    for (std::uint64_t index = last_index + 1; ; ++index)
    {
        const std::string key = "__raft/log/" + std::to_string(index);
        if (!store_.Get(key).has_value())
        {
            break;
        }
        deletes.push_back(key);
    }

    if (!puts.empty() || !deletes.empty())
    {
        store_.WriteAtomically(puts, deletes);
    }
}

void RaftStorage::PersistSnapshot(
    std::uint64_t current_term,
    std::int64_t voted_for,
    std::uint64_t commit_index,
    std::uint64_t last_applied,
    std::uint64_t snapshot_index,
    std::uint64_t snapshot_term,
    const std::string& metadata,
    const std::string& data,
    const std::vector<RaftLogEntry>& log)
{
    std::vector<std::pair<std::string, std::string>> puts = {
        {"__raft/current_term", std::to_string(current_term)},
        {"__raft/voted_for", std::to_string(voted_for)},
        {"__raft/commit_index", std::to_string(commit_index)},
        {"__raft/last_applied", std::to_string(last_applied)},
        {"__raft/snapshot_index", std::to_string(snapshot_index)},
        {"__raft/snapshot_term", std::to_string(snapshot_term)},
        {"__raft/snapshot_metadata", metadata},
        {"__raft/snapshot_data", data}
    };
    std::vector<std::string> deletes;
    for (std::uint64_t index = 1; index <= snapshot_index; ++index)
    {
        deletes.push_back("__raft/log/" + std::to_string(index));
    }
    const std::uint64_t last_index =
        snapshot_index + static_cast<std::uint64_t>(log.size());
    for (std::uint64_t index = last_index + 1; ; ++index)
    {
        const std::string key = "__raft/log/" + std::to_string(index);
        if (!store_.Get(key).has_value())
        {
            break;
        }
        deletes.push_back(key);
    }
    for (std::size_t offset = 0; offset < log.size(); ++offset)
    {
        proto::RaftLogEntry entry;
        const std::uint64_t index =
            snapshot_index + static_cast<std::uint64_t>(offset + 1);
        entry.set_index(index);
        entry.set_term(log[offset].term);
        entry.set_command(log[offset].command);
        std::string serialized;
        if (!entry.SerializeToString(&serialized))
        {
            throw std::runtime_error("failed to serialize Raft log entry");
        }
        puts.emplace_back("__raft/log/" + std::to_string(index), std::move(serialized));
    }
    store_.WriteAtomically(puts, deletes);
}

void RaftStorage::ApplySnapshot(
    const SnapshotContents& snapshot,
    std::uint64_t current_term,
    std::int64_t voted_for,
    std::uint64_t commit_index,
    std::uint64_t last_applied,
    std::uint64_t snapshot_index,
    std::uint64_t snapshot_term,
    const std::vector<RaftLogEntry>& log)
{
    std::vector<std::pair<std::string, std::string>> puts = {
        {"__raft/current_term", std::to_string(current_term)},
        {"__raft/voted_for", std::to_string(voted_for)},
        {"__raft/commit_index", std::to_string(commit_index)},
        {"__raft/last_applied", std::to_string(last_applied)},
        {"__raft/snapshot_index", std::to_string(snapshot_index)},
        {"__raft/snapshot_term", std::to_string(snapshot_term)},
        {"__raft/snapshot_metadata",
            SerializeSnapshotMetadata(snapshot_index, snapshot_term)},
        {"__raft/snapshot_data",
            SerializeSnapshotState(snapshot.entries, snapshot.applied_requests)}
    };
    std::vector<std::string> deletes;
    for (const auto& [key, value] : store_.GetAllUserEntries())
    {
        (void)value;
        deletes.push_back(key);
    }
    for (const auto& [key, value] :
         store_.GetAllPrefixedEntries("__raft/client/"))
    {
        (void)value;
        deletes.push_back(key);
    }
    for (const auto& [key, value] :
         store_.GetAllPrefixedEntries("__raft/log/"))
    {
        (void)value;
        deletes.push_back(key);
    }
    for (const auto& [key, value] : snapshot.entries)
    {
        puts.emplace_back(key, value);
    }
    for (const auto& [key, value] : snapshot.applied_requests)
    {
        puts.emplace_back("__raft/client/" + key, value);
    }
    for (std::size_t offset = 0; offset < log.size(); ++offset)
    {
        proto::RaftLogEntry entry;
        const std::uint64_t index =
            snapshot_index + static_cast<std::uint64_t>(offset + 1);
        entry.set_index(index);
        entry.set_term(log[offset].term);
        entry.set_command(log[offset].command);
        std::string serialized;
        if (!entry.SerializeToString(&serialized))
        {
            throw std::runtime_error("failed to serialize Raft log entry");
        }
        puts.emplace_back("__raft/log/" + std::to_string(index), std::move(serialized));
    }
    store_.WriteAtomically(puts, deletes);
}
}
