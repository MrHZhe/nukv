#pragma once

#include <string>
#include <optional>
#include <memory>

namespace rocksdb
{
    class DB;
}

namespace nukv
{
class RocksKVStore
{
public:
    explicit RocksKVStore(const std::string& db_path);
    ~RocksKVStore();

    RocksKVStore(const RocksKVStore&) = delete;
    RocksKVStore& operator=(const RocksKVStore&) = delete;

    void Put(const std::string& key, const std::string& value);
    std::optional<std::string> Get(const std::string& key) const;
    bool Delete(const std::string& key);

private:
    std::string db_path_;
    std::unique_ptr<rocksdb::DB> db_;
};
}