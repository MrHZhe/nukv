#include "nukv/raft_log_store.hpp"

#include <cstring>
#include <stdexcept>


namespace nukv
{

RaftLogStore::RaftLogStore(RocksKVStore& storage)
    : storage_(storage)
    , start_index_(1)
{
    // 索引 0 永久保留为 dummy 日志。
    auto dummy_buffer =
        nuraft::buffer::alloc(sizeof(nuraft::ulong));

    logs_[0] =
        nuraft::cs_new<nuraft::log_entry>(
            0,
            dummy_buffer);

    Restore();
}

nuraft::ulong RaftLogStore::NextSlotUnlocked() const
{
    if (logs_.empty() ||
        (logs_.size() == 1 &&
         logs_.find(0) != logs_.end()))
    {
        return start_index_;
    }

    return logs_.rbegin()->first + 1;
}

void RaftLogStore::PersistUnlocked()
{
    std::vector<std::pair<nuraft::ulong,nuraft::ptr<nuraft::buffer>>> serialized_entries;

    std::size_t total_size = sizeof(nuraft::ulong) + sizeof(int32_t);

    for (const auto& [index, entry] : logs_)
    {
        if (index == 0)
        {
            continue;
        }

        auto serialized = entry->serialize();

        if (!serialized)
        {
            throw std::runtime_error(
                "failed to serialize Raft log entry"
            );
        }

        total_size +=
            sizeof(nuraft::ulong) +
            sizeof(int32_t) +
            serialized->size();

        serialized_entries.emplace_back(index,std::move(serialized));
    }

    auto output = nuraft::buffer::alloc(total_size);

    output->pos(0);

    output->put(start_index_);

    output->put(
        static_cast<int32_t>(
            serialized_entries.size()
        )
    );

    for (auto& [index, serialized] :
         serialized_entries)
    {
        output->put(index);

        output->put(
            static_cast<int32_t>(
                serialized->size()
            )
        );

        output->put(*serialized);
    }

    const std::string data(
        reinterpret_cast<const char*>(
            output->data_begin()
        ),
        output->size()
    );

    storage_.Put("__raft/logs", data);
}

void RaftLogStore::Restore()
{
    const auto data =
        storage_.Get("__raft/logs");

    if (!data.has_value())
    {
        return;
    }

    const std::size_t header_size =
        sizeof(nuraft::ulong) +
        sizeof(int32_t);

    if (data->size() < header_size)
    {
        throw std::runtime_error(
            "corrupted persisted Raft log header"
        );
    }

    auto input =
        nuraft::buffer::alloc(data->size());

    std::memcpy(
        input->data_begin(),
        data->data(),
        data->size()
    );

    input->pos(0);

    const nuraft::ulong restored_start_index =
        input->get_ulong();

    const int32_t count =
        input->get_int();

    if (restored_start_index == 0)
    {
        throw std::runtime_error(
            "invalid persisted Raft start index"
        );
    }

    if (count < 0)
    {
        throw std::runtime_error(
            "invalid persisted Raft log count"
        );
    }

    std::map<
        nuraft::ulong,
        nuraft::ptr<nuraft::log_entry>>
        restored_logs;

    auto dummy_buffer =
        nuraft::buffer::alloc(
            sizeof(nuraft::ulong)
        );

    restored_logs[0] =
        nuraft::cs_new<nuraft::log_entry>(
            0,
            dummy_buffer
        );

    nuraft::ulong previous_index = 0;

    for (int32_t i = 0; i < count; ++i)
    {
        const std::size_t entry_header_size =
            sizeof(nuraft::ulong) +
            sizeof(int32_t);

        if (input->pos() > input->size() ||
            input->size() - input->pos() <
                entry_header_size)
        {
            throw std::runtime_error(
                "corrupted persisted Raft log entry header"
            );
        }

        const nuraft::ulong index =
            input->get_ulong();

        const int32_t entry_size =
            input->get_int();

        if (index == 0)
        {
            throw std::runtime_error(
                "invalid persisted Raft log index"
            );
        }

        if (previous_index != 0 &&
            index <= previous_index)
        {
            throw std::runtime_error(
                "persisted Raft log indexes are not ordered"
            );
        }

        if (entry_size <= 0)
        {
            throw std::runtime_error(
                "invalid persisted Raft log entry size"
            );
        }

        const std::size_t serialized_size =
            static_cast<std::size_t>(
                entry_size
            );

        if (input->pos() > input->size() ||
            input->size() - input->pos() <
                serialized_size)
        {
            throw std::runtime_error(
                "corrupted persisted Raft log entry data"
            );
        }

        auto entry_buffer =
            nuraft::buffer::alloc(
                serialized_size
            );

        input->get(entry_buffer);
        entry_buffer->pos(0);

        auto entry =
            nuraft::log_entry::deserialize(
                *entry_buffer
            );

        if (!entry)
        {
            throw std::runtime_error(
                "failed to deserialize persisted Raft log entry"
            );
        }

        restored_logs[index] =
            std::move(entry);

        previous_index = index;
    }

    if (input->pos() != input->size())
    {
        throw std::runtime_error(
            "unexpected trailing data in persisted Raft logs"
        );
    }

    const auto first_real_entry =
        restored_logs.upper_bound(0);

    if (first_real_entry != restored_logs.end() &&
        first_real_entry->first <
            restored_start_index)
    {
        throw std::runtime_error(
            "persisted Raft log index is before start index"
        );
    }

    logs_ = std::move(restored_logs);
    start_index_ = restored_start_index;
}

nuraft::ulong RaftLogStore::next_slot() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return NextSlotUnlocked();
}

nuraft::ulong RaftLogStore::start_index() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return start_index_;
}

nuraft::ptr<nuraft::log_entry>
RaftLogStore::last_entry() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return CloneEntry(logs_.rbegin()->second);
}

nuraft::ptr<nuraft::log_entry>
RaftLogStore::CloneEntry(
    const nuraft::ptr<nuraft::log_entry>& entry)
{
    if (!entry)
    {
        return nullptr;
    }

    nuraft::ptr<nuraft::buffer> cloned_buffer;

    if (!entry->is_buf_null())
    {
        cloned_buffer =
            nuraft::buffer::clone(entry->get_buf());
    }

    return nuraft::cs_new<nuraft::log_entry>(
        entry->get_term(),
        cloned_buffer,
        entry->get_val_type(),
        entry->get_timestamp(),
        entry->has_crc32(),
        entry->get_crc32(),
        false);
}

nuraft::ulong RaftLogStore::append(
    nuraft::ptr<nuraft::log_entry>& entry)
{
    auto cloned_entry = CloneEntry(entry);

    if (!cloned_entry)
    {
        return 0;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    const nuraft::ulong index = NextSlotUnlocked();

    logs_[index] = std::move(cloned_entry);

    try
    {
        PersistUnlocked();
    }
    catch (...)
    {
        logs_.erase(index);
        throw;
    }

    return index;
}

void RaftLogStore::write_at(nuraft::ulong index,nuraft::ptr<nuraft::log_entry>& entry)
{
    if (index == 0)
    {
        return;
    }

    auto cloned_entry = CloneEntry(entry);

    if (!cloned_entry)
    {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    const auto old_logs = logs_;

    auto erase_begin = logs_.lower_bound(index);

    logs_.erase(erase_begin,logs_.end());

    logs_[index] = std::move(cloned_entry);

    try
    {
        PersistUnlocked();
    }
    catch (...)
    {
        logs_ = old_logs;
        throw;
    }
}

nuraft::ptr<
    std::vector<nuraft::ptr<nuraft::log_entry>>>
RaftLogStore::log_entries(
    nuraft::ulong start,
    nuraft::ulong end)
{
    if (start > end)
    {
        return nullptr;
    }

    auto result = nuraft::cs_new<
        std::vector<nuraft::ptr<nuraft::log_entry>>>();

    result->reserve(
        static_cast<std::size_t>(end - start));

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = logs_.find(start);

    for (nuraft::ulong expected = start;
         expected < end;
         ++expected)
    {
        if (it == logs_.end() ||
            it->first != expected)
        {
            return nullptr;
        }

        auto cloned_entry = CloneEntry(it->second);

        if (!cloned_entry)
        {
            return nullptr;
        }

        result->push_back(std::move(cloned_entry));
        ++it;
    }

    return result;
}

nuraft::ptr<nuraft::log_entry>
RaftLogStore::entry_at(nuraft::ulong index)
{
    std::lock_guard<std::mutex> lock(mutex_);

    const nuraft::ulong next_index =
        NextSlotUnlocked();

    // 该索引尚不存在。
    if (index >= next_index)
    {
        return nullptr;
    }

    auto it = logs_.find(index);

    // 该索引位于已经 compact 的日志范围中。
    if (it == logs_.end())
    {
        return CloneEntry(logs_.at(0));
    }

    return CloneEntry(it->second);
}

nuraft::ulong RaftLogStore::term_at(
    nuraft::ulong index)
{
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = logs_.find(index);

    if (it == logs_.end())
    {
        return 0;
    }

    return it->second->get_term();
}

bool RaftLogStore::compact(
    nuraft::ulong last_log_index)
{
    std::lock_guard<std::mutex> lock(mutex_);

    const auto old_logs = logs_;
    const nuraft::ulong old_start_index = start_index_;

    // 跳过索引 0 的 dummy 日志。
    auto erase_begin = logs_.upper_bound(0);

    // 删除的右边界不包含该迭代器，
    // 因此 upper_bound 会让 last_log_index 也被删除。
    auto erase_end =
        logs_.upper_bound(last_log_index);

    logs_.erase(erase_begin, erase_end);

    if (start_index_ <= last_log_index)
    {
        start_index_ = last_log_index + 1;
    }

    try
    {
        PersistUnlocked();
    }
    catch (...)
    {
        logs_ = old_logs;
        start_index_ = old_start_index;
        return false;
    }

    return true;
}

bool RaftLogStore::flush()
{
    std::lock_guard<std::mutex> lock(mutex_);

    try
    {
        PersistUnlocked();
    }
    catch (...)
    {
        return false;
    }

    return true;
}

nuraft::ptr<nuraft::buffer>
RaftLogStore::pack(
    nuraft::ulong index,
    int32_t count)
{
    if (count < 0)
    {
        return nullptr;
    }

    std::vector<nuraft::ptr<nuraft::log_entry>>
        entries;

    entries.reserve(
        static_cast<std::size_t>(count));

    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = logs_.find(index);

        for (int32_t offset = 0;
             offset < count;
             ++offset)
        {
            const nuraft::ulong expected_index =
                index +
                static_cast<nuraft::ulong>(offset);

            if (it == logs_.end() ||
                it->first != expected_index)
            {
                return nullptr;
            }

            auto cloned_entry =
                CloneEntry(it->second);

            if (!cloned_entry)
            {
                return nullptr;
            }

            entries.push_back(
                std::move(cloned_entry));

            ++it;
        }
    }

    std::vector<nuraft::ptr<nuraft::buffer>>
        serialized_entries;

    serialized_entries.reserve(entries.size());

    std::size_t total_entry_size = 0;

    for (const auto& entry : entries)
    {
        // log_entry::serialize() 会访问其内部 buffer。
        if (entry->is_buf_null())
        {
            return nullptr;
        }

        auto serialized = entry->serialize();

        if (!serialized)
        {
            return nullptr;
        }

        total_entry_size += serialized->size();

        serialized_entries.push_back(
            std::move(serialized));
    }

    const std::size_t output_size =
        sizeof(int32_t) +
        serialized_entries.size() *
            sizeof(int32_t) +
        total_entry_size;

    auto output =
        nuraft::buffer::alloc(output_size);

    output->pos(0);

    output->put(
        static_cast<int32_t>(
            serialized_entries.size()));

    for (auto& serialized : serialized_entries)
    {
        output->put(
            static_cast<int32_t>(
                serialized->size()));

        output->put(*serialized);
    }

    return output;
}

void RaftLogStore::apply_pack(nuraft::ulong index,nuraft::buffer& pack)
{
    // 索引 0 是永久保留的 dummy 日志。
    if (index == 0)
    {
        return;
    }

    pack.pos(0);

    const int32_t count = pack.get_int();

    if (count <= 0)
    {
        return;
    }

    std::vector<nuraft::ptr<nuraft::log_entry>> entries;

    entries.reserve(static_cast<std::size_t>(count));

    // 先完整反序列化，全部成功后再修改 logs_，
    // 避免只应用一部分日志。
    for (int32_t offset = 0; offset < count; ++offset)
    {
        const int32_t entry_size = pack.get_int();

        if (entry_size <= 0)
        {
            return;
        }

        auto entry_buffer =
            nuraft::buffer::alloc(
                static_cast<std::size_t>(
                    entry_size));

        pack.get(entry_buffer);

        auto entry =
            nuraft::log_entry::deserialize(
                *entry_buffer);

        if (!entry)
        {
            return;
        }

        entries.push_back(std::move(entry));
    }

    std::lock_guard<std::mutex> lock(mutex_);

    const auto old_logs = logs_;
    const nuraft::ulong old_start_index = start_index_;

    auto erase_begin = logs_.lower_bound(index);

    logs_.erase(erase_begin,logs_.end());


    for (std::size_t offset = 0;
         offset < entries.size();
         ++offset)
    {
        const nuraft::ulong current_index =
            index +
            static_cast<nuraft::ulong>(offset);

        logs_[current_index] =
            std::move(entries[offset]);
    }

    // 第一条真实日志决定当前逻辑起点。
    auto first_entry = logs_.upper_bound(0);

    if (first_entry != logs_.end())
    {
        start_index_ = first_entry->first;
    }

    try
    {
        PersistUnlocked();
    }
    catch (...)
    {
        logs_ = old_logs;
        start_index_ = old_start_index;
        throw;
    }
}

}  // namespace nukv