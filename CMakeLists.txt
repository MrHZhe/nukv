cmake_minimum_required(VERSION 3.26)

project(nukv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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
