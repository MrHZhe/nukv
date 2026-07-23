#include <proto/command.pb.h>

#include <cassert>
#include <iostream>
#include <string>

int main() {
    nukv::proto::Command original;

    original.set_type(nukv::proto::COMMAND_TYPE_PUT);
    original.set_client_id(1001);
    original.set_request_id(8);
    original.set_key("name");
    original.set_value("Alice");

    std::string serialized;

    const bool serialize_ok =
        original.SerializeToString(&serialized);

    assert(serialize_ok);
    assert(!serialized.empty());

    nukv::proto::Command decoded;

    const bool parse_ok =
        decoded.ParseFromString(serialized);

    assert(parse_ok);
    assert(decoded.type() ==
           nukv::proto::COMMAND_TYPE_PUT);
    assert(decoded.client_id() == 1001);
    assert(decoded.request_id() == 8);
    assert(decoded.key() == "name");
    assert(decoded.value() == "Alice");

    std::cout << "Command protobuf test passed\n";
    return 0;
}