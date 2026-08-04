#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include <libnuraft/nuraft.hxx>

#include "nukv/command_applier.hpp"

namespace nukv
{
class RaftStateMachine final : public nuraft::state_machine
{
public:
    explicit RaftStateMachine(RocksKVStore& store);

    nuraft::ptr<nuraft::buffer> commit(nuraft::ulong log_idx,nuraft::buffer& data) override;
    nuraft::ulong last_commit_index() override;
    bool apply_snapshot(nuraft::snapshot& snapshot) override;
    nuraft::ptr<nuraft::snapshot> last_snapshot() override;
    void create_snapshot(nuraft::snapshot& snapshot,nuraft::async_result<bool>::handler_type& when_done) override;
    bool chk_create_snapshot() override;

    int read_logical_snp_obj(
        nuraft::snapshot& snapshot,
        void*& user_snp_ctx,
        nuraft::ulong obj_id,
        nuraft::ptr<nuraft::buffer>& data_out,
        bool& is_last_obj) override;

    void save_logical_snp_obj(
        nuraft::snapshot& snapshot,
        nuraft::ulong& obj_id,
        nuraft::buffer& data,
        bool is_first_obj,
        bool is_last_obj) override;
private:
    RocksKVStore& store_;
    CommandApplier applier_;
    std::atomic<nuraft::ulong> last_commit_index_{0};
    std::mutex snapshot_mutex_;
    nuraft::ptr<nuraft::snapshot> last_snapshot_;
    std::vector<std::pair<std::string, std::string>> snapshot_entries_;
    std::vector<std::pair<std::string, std::string>> incoming_snapshot_entries_;
    nuraft::ulong incoming_snapshot_index_{0};
    bool incoming_snapshot_ready_{false};

    void RestoreSnapshot();
};
}// namespace nukv