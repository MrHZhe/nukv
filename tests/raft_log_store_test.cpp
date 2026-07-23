#include "nukv/raft_log_store.hpp"

#include <cassert>
#include <cstring>

int main()
{
    nukv::RaftLogStore log_store;

    // 初始状态：没有真实日志，第一条日志从索引 1 开始。
    assert(log_store.start_index() == 1);
    assert(log_store.next_slot() == 1);

    // 初始状态下，最后一条日志是索引 0 的 dummy 日志。
    auto last_entry = log_store.last_entry();

    assert(last_entry != nullptr);
    assert(last_entry->get_term() == 0);

    // 索引 0 可以读到 dummy 日志。
    auto dummy_entry = log_store.entry_at(0);

    assert(dummy_entry != nullptr);
    assert(dummy_entry->get_term() == 0);
    assert(log_store.term_at(0) == 0);

    // 索引 1 尚未写入。
    assert(log_store.entry_at(1) == nullptr);
    assert(log_store.term_at(1) == 0);

    // 追加第一条真实日志。
    auto buffer1 = nuraft::buffer::alloc(3);

    std::memcpy(
        buffer1->data_begin(),
        "one",
        3
    );

    auto entry1 =
        nuraft::cs_new<nuraft::log_entry>(
            1,
            buffer1
        );

    const nuraft::ulong index1 =
        log_store.append(entry1);

    assert(index1 == 1);
    assert(log_store.start_index() == 1);
    assert(log_store.next_slot() == 2);
    assert(log_store.term_at(1) == 1);

    auto stored_entry1 =
        log_store.entry_at(1);

    assert(stored_entry1 != nullptr);
    assert(stored_entry1->get_term() == 1);
    assert(stored_entry1->get_buf().size() == 3);

    assert(
        std::memcmp(
            stored_entry1->get_buf().data_begin(),
            "one",
            3
        ) == 0
    );

    // 追加第二条日志。
    auto buffer2 = nuraft::buffer::alloc(3);

    std::memcpy(
        buffer2->data_begin(),
        "two",
        3
    );

    auto entry2 =
        nuraft::cs_new<nuraft::log_entry>(
            2,
            buffer2
        );

    const nuraft::ulong index2 =
        log_store.append(entry2);

    assert(index2 == 2);
    assert(log_store.start_index() == 1);
    assert(log_store.next_slot() == 3);
    assert(log_store.term_at(2) == 2);

    auto stored_entry2 =
        log_store.entry_at(2);

    assert(stored_entry2 != nullptr);
    assert(stored_entry2->get_term() == 2);
    assert(stored_entry2->get_buf().size() == 3);

    assert(
        std::memcmp(
            stored_entry2->get_buf().data_begin(),
            "two",
            3
        ) == 0
    );

    // last_entry() 应返回最新追加的第二条日志。
    auto last_real_entry =
        log_store.last_entry();

    assert(last_real_entry != nullptr);
    assert(last_real_entry->get_term() == 2);

    return 0;
}