#include "nukv/raft_state_machine.hpp"

#include <cassert>
#include <cstring>
#include <filesystem>
#include <string>

#include "proto/command.pb.h"

namespace {

nuraft::ptr<nuraft::buffer> ToRaftBuffer(
    const nukv::proto::Command& command
) {
    std::string serialized;

    const bool success =
        command.SerializeToString(&serialized);

    assert(success);

    auto buffer =
        nuraft::buffer::alloc(serialized.size());

    std::memcpy(
        buffer->data_begin(),
        serialized.data(),
        serialized.size()
    );

    return buffer;
}

}  // namespace

int main() {
    const auto db_path =
        std::filesystem::temp_directory_path()
        / "nukv_raft_state_machine_test";

    std::filesystem::remove_all(db_path);

    {
        nukv::RocksKVStore store(db_path.string());
        nukv::RaftStateMachine state_machine(store);

        nukv::proto::Command put_command;
        put_command.set_type(
            nukv::proto::COMMAND_TYPE_PUT
        );
        put_command.set_key("name");
        put_command.set_value("Alice");

        auto put_buffer = ToRaftBuffer(put_command);

        state_machine.commit(10, *put_buffer);

        const auto value = store.Get("name");

        assert(value.has_value());
        assert(value.value() == "Alice");
        assert(state_machine.last_commit_index() == 10);

        nukv::proto::Command delete_command;
        delete_command.set_type(
            nukv::proto::COMMAND_TYPE_DELETE
        );
        delete_command.set_key("name");

        auto delete_buffer =
            ToRaftBuffer(delete_command);

        state_machine.commit(11, *delete_buffer);

        assert(!store.Get("name").has_value());
        assert(state_machine.last_commit_index() == 11);
    }

    std::filesystem::remove_all(db_path);

    return 0;
}