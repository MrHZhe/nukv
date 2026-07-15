cmake_minimum_required(VERSION 3.26)

project(nukv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# NuRaft's examples and tests have already been verified separately.
set(BUILD_EXAMPLES OFF)
set(BUILD_TESTING OFF)

add_subdirectory(
    third_party/NuRaft
    third_party/NuRaft-build
)

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
    pthread
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
    pthread
)

add_library(
    nukv_storage
    src/storage/rocks_kv_store.cpp
)

target_include_directories(
    nukv_storage
    PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(
    nukv_storage
    PUBLIC
    ${ROCKSDB_LIBRARY}
    pthread
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
