cmake_minimum_required(VERSION 3.26)

project(nukv LANGUAGES CXX)

# ============================================================
# 全局 C++ 配置
# ============================================================

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 关闭 NuRaft 自带的示例和测试。
set(BUILD_EXAMPLES OFF)
set(BUILD_TESTING OFF)

# ============================================================
# NuRaft
# ============================================================

add_subdirectory(
    third_party/NuRaft
    third_party/NuRaft-build
)

# Build the bundled mymuduo dependency from the Git submodule.
set(MY_MUDUO_BUILD_EXAMPLES OFF CACHE BOOL "Build mymuduo examples" FORCE)
add_subdirectory(
    third_party/mymuduo
    third_party/mymuduo-build
    EXCLUDE_FROM_ALL
)

# ============================================================
# 第三方依赖
# ============================================================

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED MODULE)

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
# 测试支持
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

set(
    NUKV_PROTO_SOURCE_DIR
    "${PROJECT_SOURCE_DIR}/proto"
)

set(
    NUKV_PROTO_GENERATED_DIR
    "${PROJECT_BINARY_DIR}/proto"
)

file(
    MAKE_DIRECTORY
    "${NUKV_PROTO_GENERATED_DIR}"
)

protobuf_generate(
    LANGUAGE cpp

    PROTOS
    "${NUKV_PROTO_SOURCE_DIR}/command.proto"
    "${NUKV_PROTO_SOURCE_DIR}/client.proto"

    IMPORT_DIRS
    "${NUKV_PROTO_SOURCE_DIR}"

    PROTOC_OUT_DIR
    "${NUKV_PROTO_GENERATED_DIR}"

    APPEND_PATH

    OUT_VAR
    NUKV_PROTO_GENERATED_FILES
)

add_library(
    nukv_proto
    STATIC
    ${NUKV_PROTO_GENERATED_FILES}
)

target_include_directories(
    nukv_proto
    PUBLIC
    "${NUKV_PROTO_GENERATED_DIR}"
)

target_link_libraries(
    nukv_proto
    PUBLIC
    protobuf::libprotobuf
)

# ============================================================
# Protobuf 测试
# ============================================================

add_executable(
    command_proto_test
    tests/command_proto_test.cpp
)

target_link_libraries(
    command_proto_test
    PRIVATE
    nukv_proto
)

# command_proto_test 直接包含 command.pb.h，
# 明确保证生成文件先完成。
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

# command_applier.hpp 直接包含 command.pb.h，
# 明确保证生成文件先完成。
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

add_dependencies(
    nukv_raft
    nukv_proto
)

# ============================================================
# NuKV 服务程序
# ============================================================

add_executable(
    nukv_server
    src/main.cpp
    src/client_server.cpp
)

target_link_libraries(
    nukv_server
    PRIVATE
    nukv_raft
    mymuduo::mymuduo
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

# ============================================================
# 三节点端到端集成测试
# ============================================================

if(UNIX)
    add_executable(
        cluster_integration_test
        tests/cluster_integration_test.cpp
    )

    target_link_libraries(
        cluster_integration_test
        PRIVATE
        nukv_storage
        nukv_proto
    )

    add_dependencies(
        cluster_integration_test
        nukv_server
    )

    add_test(
        NAME cluster_integration_test
        COMMAND cluster_integration_test $<TARGET_FILE:nukv_server>
    )

    set_tests_properties(
        cluster_integration_test
        PROPERTIES
        LABELS integration
        RUN_SERIAL TRUE
        TIMEOUT 120
    )
endif()
