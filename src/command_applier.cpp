#include "nukv/command_applier.hpp"

#include <stdexcept>
namespace nukv
{
explicit CommandApplier::CommandApplier(RocksKVStore& store)
    srore_(store)
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
}