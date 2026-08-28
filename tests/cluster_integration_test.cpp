#include "client.pb.h"
#include "nukv/storage/rocks_kv_store.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace
{
using namespace std::chrono_literals;

constexpr std::size_t kNodeCount = 3;
constexpr std::uint32_t kMaxFrameSize = 16U * 1024U * 1024U;

void Require(bool condition, const std::string& message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

std::string LastError(const std::string& operation)
{
    return operation + ": " + std::strerror(errno);
}

void CloseFd(int& fd)
{
    if (fd >= 0)
    {
        ::close(fd);
        fd = -1;
    }
}

void WriteAll(int fd, const void* data, std::size_t size)
{
    const auto* cursor = static_cast<const char*>(data);
    while (size > 0)
    {
        const ssize_t written = ::send(fd, cursor, size, MSG_NOSIGNAL);
        if (written < 0 && errno == EINTR)
        {
            continue;
        }
        if (written <= 0)
        {
            throw std::runtime_error(LastError("send"));
        }
        cursor += written;
        size -= static_cast<std::size_t>(written);
    }
}

void ReadAll(int fd, void* data, std::size_t size)
{
    auto* cursor = static_cast<char*>(data);
    while (size > 0)
    {
        const ssize_t received = ::recv(fd, cursor, size, 0);
        if (received < 0 && errno == EINTR)
        {
            continue;
        }
        if (received == 0)
        {
            throw std::runtime_error("connection closed before a complete response");
        }
        if (received < 0)
        {
            throw std::runtime_error(LastError("recv"));
        }
        cursor += received;
        size -= static_cast<std::size_t>(received);
    }
}

std::vector<int> FindAvailablePorts(std::size_t count)
{
    std::vector<int> sockets;
    std::vector<int> ports;
    sockets.reserve(count);
    ports.reserve(count);

    try
    {
        for (std::size_t index = 0; index < count; ++index)
        {
            const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (fd < 0)
            {
                throw std::runtime_error(LastError("socket"));
            }
            sockets.push_back(fd);

            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            address.sin_port = 0;
            if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0)
            {
                throw std::runtime_error(LastError("bind"));
            }

            socklen_t address_size = sizeof(address);
            if (::getsockname(
                    fd,
                    reinterpret_cast<sockaddr*>(&address),
                    &address_size) < 0)
            {
                throw std::runtime_error(LastError("getsockname"));
            }
            ports.push_back(ntohs(address.sin_port));
        }
    }
    catch (...)
    {
        for (int fd : sockets)
        {
            ::close(fd);
        }
        throw;
    }

    for (int fd : sockets)
    {
        ::close(fd);
    }
    return ports;
}

class Socket final
{
public:
    explicit Socket(int fd) : fd_(fd) {}
    ~Socket() { CloseFd(fd_); }

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    int Get() const { return fd_; }

private:
    int fd_;
};

nukv::proto::ClientResponse SendRequest(
    int port,
    std::uint64_t request_id,
    nukv::proto::ClientRequest::Operation operation,
    const std::string& key,
    const std::string& value = {})
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        throw std::runtime_error(LastError("socket"));
    }
    Socket socket(fd);

    timeval timeout{};
    timeout.tv_sec = 2;
    if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0 ||
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0)
    {
        throw std::runtime_error(LastError("setsockopt"));
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(static_cast<std::uint16_t>(port));
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0)
    {
        throw std::runtime_error(LastError("connect"));
    }

    nukv::proto::ClientRequest request;
    request.set_request_id(request_id);
    request.set_operation(operation);
    request.set_key(key);
    request.set_value(value);

    std::string payload;
    Require(request.SerializeToString(&payload), "failed to serialize client request");
    Require(payload.size() <= kMaxFrameSize, "client request is too large");

    const std::uint32_t network_size =
        htonl(static_cast<std::uint32_t>(payload.size()));
    WriteAll(fd, &network_size, sizeof(network_size));
    WriteAll(fd, payload.data(), payload.size());

    std::uint32_t response_size = 0;
    ReadAll(fd, &response_size, sizeof(response_size));
    response_size = ntohl(response_size);
    Require(response_size <= kMaxFrameSize, "server returned an oversized frame");

    std::string response_payload(response_size, '\0');
    ReadAll(fd, response_payload.data(), response_payload.size());

    nukv::proto::ClientResponse response;
    Require(
        response.ParseFromString(response_payload),
        "failed to parse client response");
    Require(
        response.request_id() == request_id,
        "response request_id does not match request");
    return response;
}

struct NodeProcess
{
    int id = 0;
    int raft_port = 0;
    int client_port = 0;
    pid_t pid = -1;
    int stdin_fd = -1;
    std::filesystem::path data_directory;
    std::filesystem::path log_path;
};

class Cluster final
{
public:
    explicit Cluster(std::filesystem::path server_path)
        : server_path_(std::move(server_path))
        , root_directory_(
              std::filesystem::temp_directory_path() /
              ("nukv_cluster_integration_test_" + std::to_string(::getpid())))
    {
        Require(
            std::filesystem::exists(server_path_),
            "nukv_server does not exist: " + server_path_.string());

        std::filesystem::remove_all(root_directory_);
        std::filesystem::create_directories(root_directory_);

        const std::vector<int> ports = FindAvailablePorts(kNodeCount * 2);
        std::ostringstream peers;
        for (std::size_t index = 0; index < kNodeCount; ++index)
        {
            if (index != 0)
            {
                peers << ',';
            }
            peers << (index + 1) << "=127.0.0.1:" << ports[index];
        }
        peers_ = peers.str();

        for (std::size_t index = 0; index < kNodeCount; ++index)
        {
            NodeProcess node;
            node.id = static_cast<int>(index + 1);
            node.raft_port = ports[index];
            node.client_port = ports[kNodeCount + index];
            node.data_directory =
                root_directory_ / ("node" + std::to_string(node.id));
            node.log_path =
                root_directory_ / ("node" + std::to_string(node.id) + ".log");
            nodes_[index] = std::move(node);
        }
    }

    ~Cluster()
    {
        StopAllNoThrow();
        std::error_code error;
        std::filesystem::remove_all(root_directory_, error);
    }

    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;

    void StartAll()
    {
        for (int id = 1; id <= static_cast<int>(kNodeCount); ++id)
        {
            Start(id);
        }
    }

    void Start(int id)
    {
        NodeProcess& node = Node(id);
        Require(node.pid < 0, "node is already running");
        std::filesystem::create_directories(node.data_directory);

        int input_pipe[2] = {-1, -1};
        if (::pipe(input_pipe) < 0)
        {
            throw std::runtime_error(LastError("pipe"));
        }

        const pid_t pid = ::fork();
        if (pid < 0)
        {
            ::close(input_pipe[0]);
            ::close(input_pipe[1]);
            throw std::runtime_error(LastError("fork"));
        }

        if (pid == 0)
        {
            ::close(input_pipe[1]);
            for (const NodeProcess& existing_node : nodes_)
            {
                if (existing_node.stdin_fd >= 0)
                {
                    ::close(existing_node.stdin_fd);
                }
            }

            if (::dup2(input_pipe[0], STDIN_FILENO) < 0)
            {
                _exit(126);
            }
            ::close(input_pipe[0]);

            const int log_fd = ::open(
                node.log_path.c_str(),
                O_CREAT | O_WRONLY | O_APPEND,
                0600);
            if (log_fd < 0 ||
                ::dup2(log_fd, STDOUT_FILENO) < 0 ||
                ::dup2(log_fd, STDERR_FILENO) < 0)
            {
                _exit(126);
            }
            if (log_fd > STDERR_FILENO)
            {
                ::close(log_fd);
            }

            const std::string node_id = std::to_string(node.id);
            const std::string client_port = std::to_string(node.client_port);
            const std::string data_directory = node.data_directory.string();
            ::execl(
                server_path_.c_str(),
                server_path_.c_str(),
                "--node-id",
                node_id.c_str(),
                "--peers",
                peers_.c_str(),
                "--client-port",
                client_port.c_str(),
                "--data-dir",
                data_directory.c_str(),
                static_cast<char*>(nullptr));
            _exit(127);
        }

        ::close(input_pipe[0]);
        node.pid = pid;
        node.stdin_fd = input_pipe[1];
    }

    void StopAbruptly(int id)
    {
        NodeProcess& node = Node(id);
        if (node.pid < 0)
        {
            return;
        }
        ::kill(node.pid, SIGTERM);
        CloseFd(node.stdin_fd);
        if (!WaitForExit(node, 5s))
        {
            ::kill(node.pid, SIGKILL);
            Require(WaitForExit(node, 5s), "failed to stop node " + std::to_string(id));
        }
    }

    void StopAll()
    {
        for (NodeProcess& node : nodes_)
        {
            CloseFd(node.stdin_fd);
        }
        for (NodeProcess& node : nodes_)
        {
            if (node.pid >= 0 && !WaitForExit(node, 10s))
            {
                ::kill(node.pid, SIGTERM);
                if (!WaitForExit(node, 5s))
                {
                    ::kill(node.pid, SIGKILL);
                    Require(
                        WaitForExit(node, 5s),
                        "failed to stop node " + std::to_string(node.id));
                }
            }
        }
    }

    int WaitForLeader(
        const std::vector<int>& candidates,
        std::chrono::seconds timeout = 30s)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            std::vector<int> leaders;
            for (int id : candidates)
            {
                if (Node(id).pid < 0)
                {
                    continue;
                }
                try
                {
                    const auto response = Request(
                        id,
                        nukv::proto::ClientRequest::OPERATION_GET,
                        "integration/leader-probe");
                    if (response.status() == nukv::proto::ClientResponse::STATUS_OK ||
                        response.status() == nukv::proto::ClientResponse::STATUS_NOT_FOUND)
                    {
                        leaders.push_back(id);
                    }
                }
                catch (const std::exception&)
                {
                }
            }

            if (leaders.size() == 1)
            {
                return leaders.front();
            }
            std::this_thread::sleep_for(200ms);
        }
        throw std::runtime_error("timed out waiting for exactly one Raft leader");
    }

    nukv::proto::ClientResponse Request(
        int id,
        nukv::proto::ClientRequest::Operation operation,
        const std::string& key,
        const std::string& value = {})
    {
        return SendRequest(
            Node(id).client_port,
            next_request_id_++,
            operation,
            key,
            value);
    }

    void PrintLogs() const
    {
        for (const NodeProcess& node : nodes_)
        {
            std::cerr << "\n--- node " << node.id << " log ---\n";
            std::ifstream log(node.log_path);
            if (log)
            {
                std::cerr << log.rdbuf();
            }
            else
            {
                std::cerr << "<no log>\n";
            }
        }
    }

    void VerifyPersistedData(
        const std::string& expected_value,
        const std::string& large_value) const
    {
        for (const NodeProcess& node : nodes_)
        {
            nukv::RocksKVStore store((node.data_directory / "state_db").string());

            const auto value = store.Get("integration/key");
            Require(
                value.has_value() && value.value() == expected_value,
                "node " + std::to_string(node.id) +
                    " does not contain the final replicated value");

            Require(
                !store.Get("integration/deleted").has_value(),
                "node " + std::to_string(node.id) +
                    " still contains the deleted key");

            const auto stored_large_value = store.Get("integration/large");
            Require(
                stored_large_value.has_value() &&
                    stored_large_value.value() == large_value,
                "node " + std::to_string(node.id) +
                    " does not contain the replicated large value");

            const auto catchup_marker = store.Get("integration/catchup-marker");
            Require(
                catchup_marker.has_value() && catchup_marker.value() == "committed",
                "node " + std::to_string(node.id) +
                    " did not catch up after restart");
        }
    }

private:
    NodeProcess& Node(int id)
    {
        Require(id >= 1 && id <= static_cast<int>(kNodeCount), "invalid node id");
        return nodes_[static_cast<std::size_t>(id - 1)];
    }

    const NodeProcess& Node(int id) const
    {
        Require(id >= 1 && id <= static_cast<int>(kNodeCount), "invalid node id");
        return nodes_[static_cast<std::size_t>(id - 1)];
    }

    static bool WaitForExit(NodeProcess& node, std::chrono::seconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            int status = 0;
            const pid_t result = ::waitpid(node.pid, &status, WNOHANG);
            if (result == node.pid || (result < 0 && errno == ECHILD))
            {
                node.pid = -1;
                CloseFd(node.stdin_fd);
                return true;
            }
            if (result < 0 && errno != EINTR)
            {
                throw std::runtime_error(LastError("waitpid"));
            }
            std::this_thread::sleep_for(50ms);
        }
        return false;
    }

    void StopAllNoThrow() noexcept
    {
        for (NodeProcess& node : nodes_)
        {
            CloseFd(node.stdin_fd);
            if (node.pid >= 0)
            {
                ::kill(node.pid, SIGKILL);
                int status = 0;
                while (::waitpid(node.pid, &status, 0) < 0 && errno == EINTR)
                {
                }
                node.pid = -1;
            }
        }
    }

    std::filesystem::path server_path_;
    std::filesystem::path root_directory_;
    std::string peers_;
    std::array<NodeProcess, kNodeCount> nodes_{};
    std::uint64_t next_request_id_{1};
};

void ExpectStatus(
    const nukv::proto::ClientResponse& response,
    nukv::proto::ClientResponse::Status expected,
    const std::string& context)
{
    if (response.status() != expected)
    {
        std::ostringstream message;
        message << context << ": expected status " << static_cast<int>(expected)
                << ", got " << static_cast<int>(response.status())
                << " (" << response.message() << ')';
        throw std::runtime_error(message.str());
    }
}

void RunTest(Cluster& cluster)
{
    std::cout << "[1/6] Starting three-node cluster\n";
    cluster.StartAll();
    const int first_leader = cluster.WaitForLeader({1, 2, 3});
    std::cout << "      Leader is node " << first_leader << '\n';

    std::cout << "[2/6] Checking client validation and follower rejection\n";
    ExpectStatus(
        cluster.Request(
            first_leader,
            nukv::proto::ClientRequest::OPERATION_GET,
            ""),
        nukv::proto::ClientResponse::STATUS_INVALID_REQUEST,
        "empty key");
    ExpectStatus(
        cluster.Request(
            first_leader,
            nukv::proto::ClientRequest::OPERATION_GET,
            "__raft/reserved"),
        nukv::proto::ClientResponse::STATUS_INVALID_REQUEST,
        "reserved key");

    const int follower = first_leader == 1 ? 2 : 1;
    ExpectStatus(
        cluster.Request(
            follower,
            nukv::proto::ClientRequest::OPERATION_GET,
            "integration/key"),
        nukv::proto::ClientResponse::STATUS_NOT_LEADER,
        "follower request");

    std::cout << "[3/6] Checking PUT, GET, DELETE and a 64 KiB value\n";
    ExpectStatus(
        cluster.Request(
            first_leader,
            nukv::proto::ClientRequest::OPERATION_PUT,
            "integration/key",
            "before-failover"),
        nukv::proto::ClientResponse::STATUS_OK,
        "put");

    auto response = cluster.Request(
        first_leader,
        nukv::proto::ClientRequest::OPERATION_GET,
        "integration/key");
    ExpectStatus(response, nukv::proto::ClientResponse::STATUS_OK, "get");
    Require(response.value() == "before-failover", "get returned the wrong value");

    ExpectStatus(
        cluster.Request(
            first_leader,
            nukv::proto::ClientRequest::OPERATION_PUT,
            "integration/deleted",
            "temporary"),
        nukv::proto::ClientResponse::STATUS_OK,
        "put before delete");
    ExpectStatus(
        cluster.Request(
            first_leader,
            nukv::proto::ClientRequest::OPERATION_DELETE,
            "integration/deleted"),
        nukv::proto::ClientResponse::STATUS_OK,
        "delete");
    ExpectStatus(
        cluster.Request(
            first_leader,
            nukv::proto::ClientRequest::OPERATION_GET,
            "integration/deleted"),
        nukv::proto::ClientResponse::STATUS_NOT_FOUND,
        "get deleted key");

    const std::string large_value(64U * 1024U, 'L');
    ExpectStatus(
        cluster.Request(
            first_leader,
            nukv::proto::ClientRequest::OPERATION_PUT,
            "integration/large",
            large_value),
        nukv::proto::ClientResponse::STATUS_OK,
        "large put");
    response = cluster.Request(
        first_leader,
        nukv::proto::ClientRequest::OPERATION_GET,
        "integration/large");
    ExpectStatus(response, nukv::proto::ClientResponse::STATUS_OK, "large get");
    Require(response.value() == large_value, "large get returned the wrong value");

    std::cout << "[4/6] Stopping the leader and waiting for failover\n";
    cluster.StopAbruptly(first_leader);
    std::vector<int> survivors;
    for (int id = 1; id <= 3; ++id)
    {
        if (id != first_leader)
        {
            survivors.push_back(id);
        }
    }
    const int second_leader = cluster.WaitForLeader(survivors);
    Require(second_leader != first_leader, "leader did not change after failure");
    std::cout << "      New leader is node " << second_leader << '\n';

    response = cluster.Request(
        second_leader,
        nukv::proto::ClientRequest::OPERATION_GET,
        "integration/key");
    ExpectStatus(
        response,
        nukv::proto::ClientResponse::STATUS_OK,
        "get after leader failover");
    Require(
        response.value() == "before-failover",
        "committed value was lost during leader failover");
    ExpectStatus(
        cluster.Request(
            second_leader,
            nukv::proto::ClientRequest::OPERATION_PUT,
            "integration/key",
            "after-failover"),
        nukv::proto::ClientResponse::STATUS_OK,
        "put after leader failover");

    std::cout << "[5/6] Restarting nodes and checking log catch-up\n";
    cluster.Start(first_leader);
    const int other_follower = 6 - first_leader - second_leader;
    std::this_thread::sleep_for(2s);
    cluster.StopAbruptly(other_follower);

    const int catchup_leader =
        cluster.WaitForLeader({first_leader, second_leader});
    ExpectStatus(
        cluster.Request(
            catchup_leader,
            nukv::proto::ClientRequest::OPERATION_PUT,
            "integration/catchup-marker",
            "committed"),
        nukv::proto::ClientResponse::STATUS_OK,
        "put with restarted node in the quorum");

    cluster.Start(other_follower);
    const int final_leader = cluster.WaitForLeader({1, 2, 3});
    ExpectStatus(
        cluster.Request(
            final_leader,
            nukv::proto::ClientRequest::OPERATION_GET,
            "integration/catchup-marker"),
        nukv::proto::ClientResponse::STATUS_OK,
        "get catch-up marker after follower restart");
    std::this_thread::sleep_for(3s);

    std::cout << "[6/6] Stopping cluster and checking every RocksDB replica\n";
    cluster.StopAll();
    cluster.VerifyPersistedData("after-failover", large_value);
}
}

int main(int argc, char* argv[])
{
    if (argc != 2)
    {
        std::cerr << "Usage: cluster_integration_test <path-to-nukv_server>\n";
        return 2;
    }

    ::signal(SIGPIPE, SIG_IGN);
    Cluster cluster(argv[1]);
    try
    {
        RunTest(cluster);
        std::cout << "NuKV cluster integration test passed\n";
        return 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << "NuKV cluster integration test failed: "
                  << error.what() << '\n';
        cluster.PrintLogs();
        return 1;
    }
}
