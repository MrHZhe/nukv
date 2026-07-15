#include <nukv/storage/rocks_kv_store.hpp>

#include <rocksdb/db.h>

#include <stdexcept>

namespace nukv
{
    RocksKVStore::RocksKVStore(const std::string& db_path)
        : db_path_(db_path)
    {
        rocksdb::Options options;
        options.create_if_missing = true;

        rocksdb::DB* raw_db = nullptr;
        const rocksdb::Status status = rocksdb::DB::Open(options,db_path_,&raw_db);
        if(!status.ok())
        {
            throw std::runtime_error(
                "failed to open RocksDB at " + db_path + status.ToString()
            );
        }
        db_.reset(raw_db);
    }

    RocksKVStore::~RocksKVStore() = default;

    void RocksKVStore::Put(const std::string& key, const std::string& value)
    {
        const rocksdb::Status status = db_->Put(rocksdb::WriteOptions(),key,value);
        if(!status.ok())
        {
            throw std::runtime_error(
                "RocksDB Put failed: "  + status.ToString()
            );
        }
    }

    std::optional<std::string> RocksKVStore::Get(const std::string& key) const
    {
        std::string value;
        const rocksdb::Status status = db_->Get(rocksdb::ReadOptions(),key,&value);
        if(status.IsNotFound())
        {
            return std::nullopt;
        }
        if(!status.ok())
        {
            throw std::runtime_error(
                "RocksDB Get failed: " + status.ToString()
            );
        }
        return value;
    }

    //If the key exists, return true; otherwise, return false.
    bool RocksKVStore::Delete(const std::string& key)
    {
        //先查找再删除不是原子操作，多线程会出错
        const auto existing_value = Get(key);
        if(!existing_value.has_value())
        {
            return false;
        }
        const rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(),key);
        if(!status.ok())
        {
            throw std::runtime_error(
                "RocksDB Delete failed: " + status.ToString()
            );
        }
        return true;
    }
}