#pragma once
#include <atomic>
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
private:
    CommandApplier applier_;
    std::atomic<nuraft::ulong> last_commit_index_{0};
};
}// namespace nukv