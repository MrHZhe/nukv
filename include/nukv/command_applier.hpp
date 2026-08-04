#pragma once

#include "nukv/storage/rocks_kv_store.hpp"
#include "command.pb.h"

#include <cstdint>

namespace nukv
{
class CommandApplier
{
public:
    explicit CommandApplier(RocksKVStore& store);
    
    void Apply(const proto::Command& command);

    void ApplyAtomically(const proto::Command& command,std::uint64_t log_idx);
private:
    RocksKVStore& store_;
};
}