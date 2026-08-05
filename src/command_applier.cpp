#include "nukv/command_applier.hpp"

#include <stdexcept>
#include <string>

namespace nukv
{
CommandApplier::CommandApplier(RocksKVStore& store)
    : store_(store)
{}

void CommandApplier::Apply(const proto::Command& command)
{
    switch(command.type())
    {
        case proto::COMMAND_TYPE_PUT:
            store_.Put(command.key(),command.value());
            return;
        
        case proto::COMMAND_TYPE_DELETE:
            store_.Delete(command.key());
            return;
        
        case proto::COMMAND_TYPE_GET:
            throw std::invalid_argument(
            "GET command cannot be applied to the state machine"
            );

        case proto::COMMAND_TYPE_UNSPECIFIED:
        default:
            throw std::invalid_argument(
                "unsupported command type"
            );
    }
}

void CommandApplier::ApplyAtomically(
    const proto::Command& command,
    std::uint64_t log_idx)
{
    if (command.key().rfind("__raft/", 0) == 0)
    {
        throw std::invalid_argument("keys beginning with __raft/ are reserved");
    }
    const std::string persisted_index =
        std::to_string(log_idx);

    switch (command.type())
    {
        case proto::COMMAND_TYPE_PUT:
            store_.WriteAtomically(
                {
                    {
                        command.key(),
                        command.value()
                    },
                    {
                        "__raft/last_commit_index",
                        persisted_index
                    }
                },
                {}
            );
            return;

        case proto::COMMAND_TYPE_DELETE:
            store_.WriteAtomically(
                {
                    {
                        "__raft/last_commit_index",
                        persisted_index
                    }
                },
                {
                    command.key()
                }
            );
            return;

        case proto::COMMAND_TYPE_GET:
            store_.WriteAtomically(
            {
                {
                    "__raft/last_commit_index",
                    persisted_index
                }
            },
            {}
            );
            return;

        case proto::COMMAND_TYPE_UNSPECIFIED:
        default:
            throw std::invalid_argument(
                "unsupported command type"
            );
    }
}
}