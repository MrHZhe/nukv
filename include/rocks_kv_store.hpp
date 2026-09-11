#pragma once

#include <string>
#include <optional>
#include <memory>
#include <utility>
#include <vector>
#include <cstdint>

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
    void WriteAtomically(
        const std::vector<std::pair<std::string, std::string>>& puts,
        const std::vector<std::string>& deletes);
    std::vector<std::pair<std::string, std::string>> GetAllUserEntries() const;


    void SaveSnapshotAtomically(const std::string& metadata, const std::string& data);
    std::optional<std::pair<std::string, std::string>> LoadSnapshot() const;

    void ApplySnapshotAtomically(
    const std::vector<std::pair<std::string, std::string>>& entries,
    std::uint64_t last_commit_index,
    const std::string& metadata,
    const std::string& data);

private:
    std::string db_path_;
    std::unique_ptr<rocksdb::DB> db_;
};
}