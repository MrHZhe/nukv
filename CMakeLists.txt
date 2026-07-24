cmake_minimum_required(VERSION 3.26)

project(nukv LANGUAGES CXX)

# ============================================================
# 全局 C++ 配置
# ============================================================

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 关闭 NuRaft 自己的示例和测试。
# NuRaft 已经单独验证过，这里只构建库。
set(BUILD_EXAMPLES OFF)
set(BUILD_TESTING OFF)

# ============================================================
# NuRaft
# ============================================================

add_subdirectory(
    third_party/NuRaft
    third_party/NuRaft-build
)

# ============================================================
# 通用依赖
# ============================================================

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)

find_path(
    ROCKSDB_INCLUDE_DIR
    NAMES rocksdb/db.h
    REQUIRED
)

find_library(
    ROCKSDB_LIBRARY
    NAMES rocksdb
    REQUIRED
)

# ============================================================
# RocksDB 基础验证程序
# ============================================================

add_executable(
    rocksdb_smoke
    src/rocksdb_smoke.cpp
)

target_include_directories(
    rocksdb_smoke
    PRIVATE
    ${ROCKSDB_INCLUDE_DIR}
)

target_link_libraries(
    rocksdb_smoke
    PRIVATE
    ${ROCKSDB_LIBRARY}
    Threads::Threads
)

add_executable(
    rocksdb_read
    src/rocksdb_read.cpp
)

target_include_directories(
    rocksdb_read
    PRIVATE
    ${ROCKSDB_INCLUDE_DIR}
)

target_link_libraries(
    rocksdb_read
    PRIVATE
    ${ROCKSDB_LIBRARY}
    Threads::Threads
)

# ============================================================
# RocksDB KV 存储模块
# ============================================================

add_library(
    nukv_storage
    src/storage/rocks_kv_store.cpp
)

target_include_directories(
    nukv_storage
    PUBLIC
    ${PROJECT_SOURCE_DIR}/include

    PRIVATE
    ${ROCKSDB_INCLUDE_DIR}
)

target_link_libraries(
    nukv_storage
    PUBLIC
    ${ROCKSDB_LIBRARY}
    Threads::Threads
)

# ============================================================
# 项目测试支持
# ============================================================

enable_testing()

add_executable(
    rocks_kv_store_test
    tests/rocks_kv_store_test.cpp
)

target_link_libraries(
    rocks_kv_store_test
    PRIVATE
    nukv_storage
)

add_test(
    NAME rocks_kv_store_test
    COMMAND rocks_kv_store_test
)

# ============================================================
# Protobuf 命令协议
# ============================================================

add_library(
    nukv_proto
    proto/command.proto
)

protobuf_generate(
    TARGET nukv_proto
    LANGUAGE cpp
    IMPORT_DIRS ${PROJECT_SOURCE_DIR}/proto
)

# 生成文件位置：
#
# build/proto/command.pb.h
# build/proto/command.pb.cc
#
# 源代码中统一使用：
#
# #include "proto/command.pb.h"
#
target_include_directories(
    nukv_proto
    PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(
    nukv_proto
    PUBLIC
    protobuf::libprotobuf
)

add_executable(
    command_proto_test
    tests/command_proto_test.cpp
)

target_link_libraries(
    command_proto_test
    PRIVATE
    nukv_proto
)

# command_proto_test 编译时需要使用生成的 command.pb.h。
add_dependencies(
    command_proto_test
    nukv_proto
)

add_test(
    NAME command_proto_test
    COMMAND command_proto_test
)

# ============================================================
# CommandApplier 模块
# ============================================================

add_library(
    nukv_command
    src/command_applier.cpp
)

target_include_directories(
    nukv_command
    PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(
    nukv_command
    PUBLIC
    nukv_storage
    nukv_proto
)

# 防止并行全量构建时：
# command_applier.cpp 已开始编译，但 command.pb.h 尚未生成。
add_dependencies(
    nukv_command
    nukv_proto
)

# ============================================================
# Raft 模块
# ============================================================

add_library(
    nukv_raft
    src/raft_state_machine.cpp
    src/raft_state_manager.cpp
    src/raft_log_store.cpp
    src/raft_node.cpp
)

target_include_directories(
    nukv_raft
    PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(
    nukv_raft
    PUBLIC
    nukv_command
    NuRaft::static_lib
)

# raft_state_machine.cpp 间接或直接使用生成的 Protobuf 头文件。
add_dependencies(
    nukv_raft
    nukv_proto
)

# ============================================================
# RaftStateMachine 测试
# ============================================================

add_executable(
    raft_state_machine_test
    tests/raft_state_machine_test.cpp
)

target_link_libraries(
    raft_state_machine_test
    PRIVATE
    nukv_raft
)

add_dependencies(
    raft_state_machine_test
    nukv_proto
)

add_test(
    NAME raft_state_machine_test
    COMMAND raft_state_machine_test
)

# ============================================================
# RaftLogStore 测试
# ============================================================

add_executable(
    raft_log_store_test
    tests/raft_log_store_test.cpp
)

target_link_libraries(
    raft_log_store_test
    PRIVATE
    nukv_raft
)

add_dependencies(
    raft_log_store_test
    nukv_proto
)

add_test(
    NAME raft_log_store_test
    COMMAND raft_log_store_test
)

add_executable(
    nukv_server
    src/main.cpp
)

target_link_libraries(
    nukv_server
    PRIVATE
    nukv_raft
)

