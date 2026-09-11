#pragma once

#include "rocks_kv_store.hpp"
#include "command.pb.h"

#include <cstdint>

namespace nukv
{
class CommandApplier
{
public:
    explicit CommandApplier(RocksKVStore& store);

    void Apply(const proto::Command& command);

    bool IsApplied(const proto::Command& command) const;

    void ApplyAtomically(
        const proto::Command& command,
        std::uint64_t log_idx,
        std::uint64_t commit_index);
private:
    RocksKVStore& store_;
};
}
