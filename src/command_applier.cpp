#include "command_applier.hpp"

#include <stdexcept>
#include <string>

namespace nukv
{
namespace
{
std::string RequestKey(const proto::Command& command)
{
    return "__raft/client/" +
        std::to_string(command.client_id()) + "/" +
        std::to_string(command.request_id());
}

std::string SerializeCommand(const proto::Command& command)
{
    std::string serialized;
    if (!command.SerializeToString(&serialized))
    {
        throw std::runtime_error("failed to serialize Raft command");
    }
    return serialized;
}
}

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

bool CommandApplier::IsApplied(const proto::Command& command) const
{
    return command.request_id() != 0 &&
        store_.Get(RequestKey(command)).has_value();
}

void CommandApplier::ApplyAtomically(
    const proto::Command& command,
    std::uint64_t log_idx,
    std::uint64_t commit_index)
{
    if (command.key().rfind("__raft/", 0) == 0)
    {
        throw std::invalid_argument("keys beginning with __raft/ are reserved");
    }

    const std::string serialized_command = SerializeCommand(command);
    const bool has_request_id = command.request_id() != 0;
    const std::string request_key = RequestKey(command);
    if (has_request_id)
    {
        const auto previous = store_.Get(request_key);
        if (previous.has_value() && previous.value() != serialized_command)
        {
            throw std::invalid_argument(
                "client request id was reused with a different command");
        }
        if (previous.has_value())
        {
            store_.WriteAtomically(
                {
                    {"__raft/commit_index", std::to_string(commit_index)},
                    {"__raft/last_applied", std::to_string(log_idx)}
                },
                {});
            return;
        }
    }

    std::vector<std::pair<std::string, std::string>> puts = {
        {"__raft/commit_index", std::to_string(commit_index)},
        {"__raft/last_applied", std::to_string(log_idx)}
    };

    switch (command.type())
    {
        case proto::COMMAND_TYPE_PUT:
            puts.emplace_back(command.key(), command.value());
            break;

        case proto::COMMAND_TYPE_DELETE:
            break;

        case proto::COMMAND_TYPE_GET:
            throw std::invalid_argument(
                "GET command cannot be applied to the state machine");

        case proto::COMMAND_TYPE_UNSPECIFIED:
        default:
            throw std::invalid_argument(
                "unsupported command type"
            );
    }

    std::vector<std::string> deletes;
    if (command.type() == proto::COMMAND_TYPE_DELETE)
    {
        deletes.push_back(command.key());
    }
    if (has_request_id)
    {
        puts.emplace_back(request_key, serialized_command);
    }
    store_.WriteAtomically(puts, deletes);
}
}
