#include <rocksdb/db.h>

#include <iostream>
#include <memory>
#include <string>

int main() {
    const std::string db_path = "./data/rocksdb_smoke";

    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status status =
        rocksdb::DB::Open(options, db_path, &raw_db);

    if (!status.ok()) {
        std::cerr << "打开 RocksDB 失败: "
                  << status.ToString() << '\n';
        return 1;
    }

    std::unique_ptr<rocksdb::DB> db(raw_db);

    status = db->Put(
        rocksdb::WriteOptions(),
        "name",
        "Alice"
    );

    if (!status.ok()) {
        std::cerr << "写入失败: "
                  << status.ToString() << '\n';
        return 1;
    }

    std::string value;
    status = db->Get(
        rocksdb::ReadOptions(),
        "name",
        &value
    );

    if (!status.ok()) {
        std::cerr << "读取失败: "
                  << status.ToString() << '\n';
        return 1;
    }

    std::cout << "name = " << value << '\n';
    return 0;
}
