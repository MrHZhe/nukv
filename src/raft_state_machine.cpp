#include "nukv/raft_state_machine.hpp"
#include <command.pb.h>

#include <cstddef>
#include <exception>
#include <limits>
#include <stdexcept>

namespace nukv
{
RaftStateMachine::RaftStateMachine(RocksKVStore& store)
    : applier_(store)
{

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

    applier_.Apply(command);

    last_commit_index_.store(log_idx);

    return nullptr;
}

nuraft::ulong RaftStateMachine::last_commit_index()
{
    return last_commit_index_.load();
}

bool RaftStateMachine::apply_snapshot(nuraft::snapshot& snapshot)
{
    return false;
}

nuraft::ptr<nuraft::snapshot> RaftStateMachine::last_snapshot()
{
    return nullptr;
}

void RaftStateMachine::create_snapshot(nuraft::snapshot& snapshot,nuraft::async_result<bool>::handler_type& when_done)
{
    bool success = false;
    nuraft::ptr<std::exception> error = nullptr;
    when_done(success, error);
}

bool RaftStateMachine::chk_create_snapshot()
{
    return false;
}
}