#include <nukv/storage/rocks_kv_store.hpp>

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/iterator.h>

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

    void RocksKVStore::WriteAtomically(
        const std::vector<std::pair<std::string, std::string>>& puts,
        const std::vector<std::string>& deletes)
    {
        rocksdb::WriteBatch batch;

        for (const auto& [key, value] : puts)
        {
            batch.Put(key, value);
        }

        for (const auto& key : deletes)
        {
            batch.Delete(key);
        }

        const rocksdb::Status status = db_->Write(rocksdb::WriteOptions(),&batch);

        if (!status.ok())
        {
            throw std::runtime_error(
                "RocksDB atomic write failed: " +
                status.ToString()
            );
        }
    }

    std::vector<std::pair<std::string, std::string>> RocksKVStore::GetAllUserEntries() const
    {
        std::vector<std::pair<std::string, std::string>> entries;

        const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
        if (snapshot == nullptr)
        {
            throw std::runtime_error("failed to create RocksDB snapshot");
        }

        rocksdb::ReadOptions options;
        options.snapshot = snapshot;

        std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(options));
        rocksdb::Status status = rocksdb::Status::OK();

        try
        {
            for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next())
            {
                std::string key = iterator->key().ToString();

                if (key.rfind("__raft/", 0) == 0)
                {
                    continue;
                }

                entries.emplace_back(std::move(key), iterator->value().ToString());
            }

            status = iterator->status();
        }
        catch (...)
        {
            iterator.reset();
            db_->ReleaseSnapshot(snapshot);
            throw;
        }

        iterator.reset();
        db_->ReleaseSnapshot(snapshot);

        if (!status.ok())
        {
            throw std::runtime_error("RocksDB iteration failed: " + status.ToString());
        }

        return entries;
    }


    void RocksKVStore::SaveSnapshotAtomically(const std::string& metadata, const std::string& data)
    {
        rocksdb::WriteBatch batch;

        batch.Put("__raft/snapshot_metadata", metadata);
        batch.Put("__raft/snapshot_data", data);

        const rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);

        if (!status.ok())
        {
            throw std::runtime_error("RocksDB snapshot persistence failed: " + status.ToString());
        }
    }

    std::optional<std::pair<std::string, std::string>> RocksKVStore::LoadSnapshot() const
    {
        std::string metadata;
        std::string data;

        const rocksdb::Status metadata_status =
            db_->Get(rocksdb::ReadOptions(), "__raft/snapshot_metadata", &metadata);

        const rocksdb::Status data_status =
            db_->Get(rocksdb::ReadOptions(), "__raft/snapshot_data", &data);

        if (metadata_status.IsNotFound() && data_status.IsNotFound())
        {
            return std::nullopt;
        }

        if (!metadata_status.ok() || !data_status.ok())
        {
            throw std::runtime_error("incomplete or corrupted persisted snapshot");
        }

        return std::make_pair(std::move(metadata), std::move(data));
    }

    void RocksKVStore::ApplySnapshotAtomically(
    const std::vector<std::pair<std::string, std::string>>& entries,
    std::uint64_t last_commit_index,
    const std::string& metadata,
    const std::string& data)
    {
        rocksdb::WriteBatch batch;
        rocksdb::ReadOptions options;
        std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(options));

        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next())
        {
            const std::string key = iterator->key().ToString();

            if (key.rfind("__raft/", 0) != 0)
            {
                batch.Delete(key);
            }
        }

        const rocksdb::Status iterator_status = iterator->status();

        if (!iterator_status.ok())
        {
            throw std::runtime_error("RocksDB iteration failed: " + iterator_status.ToString());
        }

        for (const auto& [key, value] : entries)
        {
            if (key.rfind("__raft/", 0) == 0)
            {
                throw std::runtime_error("snapshot contains reserved key");
            }

            batch.Put(key, value);
        }

        batch.Put("__raft/last_commit_index", std::to_string(last_commit_index));
        batch.Put("__raft/snapshot_metadata", metadata);
        batch.Put("__raft/snapshot_data", data);

        const rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);

        if (!status.ok())
        {
            throw std::runtime_error("RocksDB snapshot apply failed: " + status.ToString());
        }
    }
}