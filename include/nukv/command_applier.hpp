#pragma once

#include "nukv/storage/rocks_kv_store.hpp"
#include "proto/command.pb.h"

namespace nukv
{
class CommandApplier
{
public:
    explicit CommandApplier(RocksKVStore& store);
    
    void Apply(const proto::Command& command);
private:
    RocksKVStore& store_;
}
}