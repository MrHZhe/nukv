#include "nukv/raft_state_machine.hpp"
#include <command.pb.h>

#include <cstddef>
#include <exception>
#include <limits>
#include <stdexcept>
#include <string>
#include <cstdint>
#include <iostream>
#include <cstring>

namespace nukv
{
RaftStateMachine::RaftStateMachine(RocksKVStore& store)
    : store_(store)
    , applier_(store)
{
    const auto persisted_index = store_.Get("__raft/last_commit_index");

    if (persisted_index.has_value())
    {
        std::size_t parsed_length = 0;

        const unsigned long long restored_index =
            std::stoull(persisted_index.value(), &parsed_length);

        if (parsed_length != persisted_index->size())
        {
            throw std::runtime_error("invalid persisted last commit index");
        }

        last_commit_index_.store(
            static_cast<nuraft::ulong>(restored_index),
            std::memory_order_release);
    }

    RestoreSnapshot();
}

nuraft::ptr<nuraft::buffer> RaftStateMachine::commit(nuraft::ulong log_idx,nuraft::buffer& data)
{
    if (data.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) 
    {
        throw std::runtime_error("Raft command is too large");
    }

    proto::Command command;

    const bool parse_ok = command.ParseFromArray(
        data.data_begin(),
        static_cast<int>(data.size())
    );

    if (!parse_ok) {
        throw std::runtime_error(
            "failed to parse Raft command"
        );
    }

    std::lock_guard<std::mutex> lock(snapshot_mutex_);

    applier_.ApplyAtomically(command, log_idx);
    last_commit_index_.store(log_idx, std::memory_order_release);

    return nullptr;
}

nuraft::ulong RaftStateMachine::last_commit_index()
{
    return last_commit_index_.load();
}

bool RaftStateMachine::apply_snapshot(nuraft::snapshot& snapshot)
{
    std::lock_guard<std::mutex> lock(snapshot_mutex_);

    const nuraft::ulong snapshot_index = snapshot.get_last_log_idx();

    if (!incoming_snapshot_ready_ || incoming_snapshot_index_ != snapshot_index)
    {
        return false;
    }

    try
    {
        auto snapshot_buffer = snapshot.serialize();

        if (!snapshot_buffer)
        {
            return false;
        }

        auto cloned_snapshot = nuraft::snapshot::deserialize(*snapshot_buffer);

        if (!cloned_snapshot)
        {
            return false;
        }

        const std::size_t max_size = std::numeric_limits<std::size_t>::max();
        const std::size_t max_u32 = std::numeric_limits<std::uint32_t>::max();

        if (incoming_snapshot_entries_.size() > max_u32)
        {
            return false;
        }

        std::size_t data_size = sizeof(std::uint32_t);

        for (const auto& [key, value] : incoming_snapshot_entries_)
        {
            if (key.size() > max_u32 || value.size() > max_u32)
            {
                return false;
            }

            if (data_size > max_size - sizeof(std::uint32_t) ||
                key.size() > max_size - data_size - sizeof(std::uint32_t))
            {
                return false;
            }

            data_size += sizeof(std::uint32_t) + key.size();

            if (data_size > max_size - sizeof(std::uint32_t) ||
                value.size() > max_size - data_size - sizeof(std::uint32_t))
            {
                return false;
            }

            data_size += sizeof(std::uint32_t) + value.size();
        }

        auto data_buffer = nuraft::buffer::alloc(data_size);

        if (!data_buffer)
        {
            return false;
        }

        nuraft::buffer_serializer writer(data_buffer);
        writer.put_u32(static_cast<std::uint32_t>(incoming_snapshot_entries_.size()));

        for (const auto& [key, value] : incoming_snapshot_entries_)
        {
            writer.put_str(key);
            writer.put_str(value);
        }

        const std::string metadata(
            reinterpret_cast<const char*>(snapshot_buffer->data_begin()),
            snapshot_buffer->size());

        const std::string data(
            reinterpret_cast<const char*>(data_buffer->data_begin()),
            data_buffer->size());

        store_.ApplySnapshotAtomically(
            incoming_snapshot_entries_,
            snapshot_index,
            metadata,
            data);

        snapshot_entries_ = std::move(incoming_snapshot_entries_);
        last_snapshot_ = std::move(cloned_snapshot);

        incoming_snapshot_entries_.clear();
        incoming_snapshot_index_ = 0;
        incoming_snapshot_ready_ = false;

        last_commit_index_.store(snapshot_index, std::memory_order_release);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

nuraft::ptr<nuraft::snapshot> RaftStateMachine::last_snapshot()
{
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    return last_snapshot_;
}

void RaftStateMachine::create_snapshot(
    nuraft::snapshot& snapshot,
    nuraft::async_result<bool>::handler_type& when_done)
{
    bool success = false;
    nuraft::ptr<std::exception> error = nullptr;

    try
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);

        const nuraft::ulong snapshot_index = snapshot.get_last_log_idx();
        const nuraft::ulong commit_index = last_commit_index_.load(std::memory_order_acquire);

        if (snapshot_index != commit_index)
        {
            throw std::runtime_error("snapshot index does not match state machine");
        }

        auto entries = store_.GetAllUserEntries();

        if (entries.size() > std::numeric_limits<std::uint32_t>::max())
        {
            throw std::runtime_error("too many snapshot entries");
        }

        auto snapshot_buffer = snapshot.serialize();

        if (!snapshot_buffer)
        {
            throw std::runtime_error("failed to serialize Raft snapshot");
        }

        auto cloned_snapshot = nuraft::snapshot::deserialize(*snapshot_buffer);

        if (!cloned_snapshot)
        {
            throw std::runtime_error("failed to clone Raft snapshot");
        }

        std::size_t data_size = sizeof(std::uint32_t);

        for (const auto& [key, value] : entries)
        {
            if (key.size() > std::numeric_limits<std::uint32_t>::max() ||
                value.size() > std::numeric_limits<std::uint32_t>::max())
            {
                throw std::runtime_error("snapshot key or value is too large");
            }

            const std::size_t entry_size =
                sizeof(std::uint32_t) + key.size() +
                sizeof(std::uint32_t) + value.size();

            if (entry_size > std::numeric_limits<std::size_t>::max() - data_size)
            {
                throw std::runtime_error("snapshot data size overflow");
            }

            data_size += entry_size;
        }

        auto data_buffer = nuraft::buffer::alloc(data_size);

        if (!data_buffer)
        {
            throw std::runtime_error("failed to allocate snapshot data buffer");
        }

        nuraft::buffer_serializer writer(data_buffer);

        writer.put_u32(static_cast<std::uint32_t>(entries.size()));

        for (const auto& [key, value] : entries)
        {
            writer.put_str(key);
            writer.put_str(value);
        }

        const std::string metadata(
            reinterpret_cast<const char*>(snapshot_buffer->data_begin()),
            snapshot_buffer->size());

        const std::string data(
            reinterpret_cast<const char*>(data_buffer->data_begin()),
            data_buffer->size());

        store_.SaveSnapshotAtomically(metadata, data);

        snapshot_entries_ = std::move(entries);
        last_snapshot_ = std::move(cloned_snapshot);
        success = true;
    }
    catch (const std::exception& exception)
    {
        error = nuraft::cs_new<std::runtime_error>(exception.what());
    }
    catch (...)
    {
        error = nuraft::cs_new<std::runtime_error>("unknown snapshot creation failure");
    }

    when_done(success, error);
}

bool RaftStateMachine::chk_create_snapshot()
{
    return true;
}

//发送快照，目前是单对象，可优化为分块传送
int RaftStateMachine::read_logical_snp_obj(
    nuraft::snapshot& snapshot,
    void*& user_snp_ctx,
    nuraft::ulong obj_id,
    nuraft::ptr<nuraft::buffer>& data_out,
    bool& is_last_obj)
{
    (void)user_snp_ctx;

    if (obj_id != 0)
    {
        return -1;
    }

    std::lock_guard<std::mutex> lock(snapshot_mutex_);

    if (!last_snapshot_ ||
        last_snapshot_->get_last_log_idx() != snapshot.get_last_log_idx())
    {
        return -1;
    }

    const std::size_t max_size = std::numeric_limits<std::size_t>::max();
    const std::size_t max_u32 = std::numeric_limits<std::uint32_t>::max();

    if (snapshot_entries_.size() > max_u32)
    {
        return -1;
    }

    std::size_t data_size = sizeof(std::uint32_t);

    for (const auto& [key, value] : snapshot_entries_)
    {
        if (key.size() > max_u32 || value.size() > max_u32)
        {
            return -1;
        }

        if (data_size > max_size - sizeof(std::uint32_t) ||
            key.size() > max_size - data_size - sizeof(std::uint32_t))
        {
            return -1;
        }

        data_size += sizeof(std::uint32_t) + key.size();

        if (data_size > max_size - sizeof(std::uint32_t) ||
            value.size() > max_size - data_size - sizeof(std::uint32_t))
        {
            return -1;
        }

        data_size += sizeof(std::uint32_t) + value.size();
    }

    try
    {
        data_out = nuraft::buffer::alloc(data_size);

        if (!data_out)
        {
            return -1;
        }

        nuraft::buffer_serializer writer(data_out);
        writer.put_u32(static_cast<std::uint32_t>(snapshot_entries_.size()));

        for (const auto& [key, value] : snapshot_entries_)
        {
            writer.put_str(key);
            writer.put_str(value);
        }
    }
    catch (...)
    {
        data_out = nullptr;
        return -1;
    }

    is_last_obj = true;
    return 0;
}

    void RaftStateMachine::save_logical_snp_obj(
        nuraft::snapshot& snapshot,
        nuraft::ulong& obj_id,
        nuraft::buffer& data,
        bool is_first_obj,
        bool is_last_obj)
    {
        if (obj_id != 0 || !is_first_obj || !is_last_obj)
        {
            throw std::runtime_error("unsupported snapshot object");
        }

        {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            incoming_snapshot_entries_.clear();
            incoming_snapshot_index_ = 0;
            incoming_snapshot_ready_ = false;
        }

        if (data.size() < sizeof(std::uint32_t))
        {
            throw std::runtime_error("invalid snapshot data");
        }

        nuraft::buffer_serializer reader(data);
        const std::uint32_t entry_count = reader.get_u32();
        const std::size_t minimum_entry_size = sizeof(std::uint32_t) * 2;

        if (entry_count > (data.size() - sizeof(std::uint32_t)) / minimum_entry_size)
        {
            throw std::runtime_error("invalid snapshot entry count");
        }

        std::vector<std::pair<std::string, std::string>> entries;
        entries.reserve(entry_count);

        for (std::uint32_t i = 0; i < entry_count; ++i)
        {
            std::string key = reader.get_str();
            std::string value = reader.get_str();
            if (key.rfind("__raft/", 0) == 0)
            {
                throw std::runtime_error("snapshot contains reserved key");
            }
            entries.emplace_back(std::move(key), std::move(value));
        }

        if (reader.pos() != reader.size())
        {
            throw std::runtime_error("unexpected trailing snapshot data");
        }

        {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            incoming_snapshot_entries_ = std::move(entries);
            incoming_snapshot_index_ = snapshot.get_last_log_idx();
            incoming_snapshot_ready_ = true;
        }

        obj_id = 1;
    }

    void RaftStateMachine::RestoreSnapshot()
    {
        const auto persisted_snapshot = store_.LoadSnapshot();

        if (!persisted_snapshot.has_value())
        {
            return;
        }

        const auto& [metadata, data] = persisted_snapshot.value();

        if (metadata.empty() || data.size() < sizeof(std::uint32_t))
        {
            throw std::runtime_error("invalid persisted snapshot");
        }

        auto metadata_buffer = nuraft::buffer::alloc(metadata.size());
        if (!metadata_buffer)
        {
            throw std::runtime_error("failed to allocate snapshot metadata buffer");
        }
        std::memcpy(metadata_buffer->data_begin(), metadata.data(), metadata.size());

        auto restored_snapshot = nuraft::snapshot::deserialize(*metadata_buffer);

        if (!restored_snapshot)
        {
            throw std::runtime_error("failed to restore snapshot metadata");
        }

        auto data_buffer = nuraft::buffer::alloc(data.size());
        if (!data_buffer)
        {
            throw std::runtime_error("failed to allocate snapshot data buffer");
        }
        std::memcpy(data_buffer->data_begin(), data.data(), data.size());

        nuraft::buffer_serializer reader(data_buffer);
        const std::uint32_t entry_count = reader.get_u32();
        const std::size_t minimum_entry_size = sizeof(std::uint32_t) * 2;

        if (entry_count > (data.size() - sizeof(std::uint32_t)) / minimum_entry_size)
        {
            throw std::runtime_error("invalid persisted snapshot entry count");
        }

        std::vector<std::pair<std::string, std::string>> entries;
        entries.reserve(entry_count);

        for (std::uint32_t i = 0; i < entry_count; ++i)
        {
            std::string key = reader.get_str();
            std::string value = reader.get_str();
            entries.emplace_back(std::move(key), std::move(value));
        }

        if (reader.pos() != reader.size())
        {
            throw std::runtime_error("unexpected trailing persisted snapshot data");
        }

        const nuraft::ulong snapshot_index = restored_snapshot->get_last_log_idx();
        const nuraft::ulong commit_index = last_commit_index_.load(std::memory_order_acquire);

        if (snapshot_index > commit_index)
        {
            throw std::runtime_error("persisted snapshot is ahead of state machine");
        }

        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        snapshot_entries_ = std::move(entries);
        last_snapshot_ = std::move(restored_snapshot);
    }
}