#pragma once

#include <libnuraft/nuraft.hxx>

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <utility>
#include <vector>

namespace nukv
{

class RaftLogStore final : public nuraft::log_store
{
public:
    RaftLogStore();
    ~RaftLogStore() override = default;

    RaftLogStore(const RaftLogStore&) = delete;
    RaftLogStore& operator=(const RaftLogStore&) = delete;

    nuraft::ulong next_slot() const override;

    nuraft::ulong start_index() const override;

    nuraft::ptr<nuraft::log_entry> last_entry() const override;

    nuraft::ulong append(
        nuraft::ptr<nuraft::log_entry>& entry) override;

    void write_at(
        nuraft::ulong index,
        nuraft::ptr<nuraft::log_entry>& entry) override;

    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
    log_entries(
        nuraft::ulong start,
        nuraft::ulong end) override;

    nuraft::ptr<nuraft::log_entry> entry_at(
        nuraft::ulong index) override;

    nuraft::ulong term_at(
        nuraft::ulong index) override;

    nuraft::ptr<nuraft::buffer> pack(
        nuraft::ulong index,
        int32_t count) override;

    void apply_pack(
        nuraft::ulong index,
        nuraft::buffer& pack) override;

    bool compact(
        nuraft::ulong last_log_index) override;

    bool flush() override;

private:
    static nuraft::ptr<nuraft::log_entry> CloneEntry(
        const nuraft::ptr<nuraft::log_entry>& entry);

    // 调用该函数前，调用者必须已经持有 mutex_。
    nuraft::ulong NextSlotUnlocked() const;

    mutable std::mutex mutex_;

    std::map<
        nuraft::ulong,
        nuraft::ptr<nuraft::log_entry>> logs_;

    nuraft::ulong start_index_;
};

}  // namespace nukv