cmake_minimum_required(VERSION 3.22)

project(nukv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(MY_MUDUO_BUILD_EXAMPLES OFF CACHE BOOL "Build mymuduo examples" FORCE)
add_subdirectory(
    third_party/mymuduo
    third_party/mymuduo-build
    EXCLUDE_FROM_ALL
)

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

add_library(
    nukv_storage
    src/rocks_kv_store.cpp
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
    "${NUKV_PROTO_SOURCE_DIR}/raft.proto"

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

add_executable(
    command_proto_test
    tests/command_proto_test.cpp
)

target_link_libraries(
    command_proto_test
    PRIVATE
    nukv_proto
)

add_dependencies(
    command_proto_test
    nukv_proto
)

add_test(
    NAME command_proto_test
    COMMAND command_proto_test
)

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

add_dependencies(
    nukv_command
    nukv_proto
)

add_library(
    nukv_raft
    src/raft_node.cpp
    src/raft_storage.cpp
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
    mymuduo::mymuduo
)

add_dependencies(
    nukv_raft
    nukv_proto
)

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
