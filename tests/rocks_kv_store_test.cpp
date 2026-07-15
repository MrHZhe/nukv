#include <nukv/storage/rocks_kv_store.hpp>

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

int main() {
    const std::string db_path =
    (std::filesystem::temp_directory_path() /
     "nukv_rocks_kv_store_test").string();

    // 保证每次测试都从空数据库开始。
    std::filesystem::remove_all(db_path);

    {
        nukv::RocksKVStore store(db_path);

        store.Put("name", "Alice");

        const auto value = store.Get("name");
        assert(value.has_value());
        assert(*value == "Alice");

        assert(!store.Get("unknown").has_value());
    }

    // 重新打开数据库，验证数据确实已经持久化。
    {
        nukv::RocksKVStore store(db_path);

        const auto value = store.Get("name");
        assert(value.has_value());
        assert(*value == "Alice");

        assert(store.Delete("name"));
        assert(!store.Get("name").has_value());

        assert(!store.Delete("name"));
    }

    std::filesystem::remove_all(db_path);

    std::cout << "RocksKVStore test passed\n";
    return 0;
}