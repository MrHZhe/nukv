#pragma once

#include "nukv/raft_node.hpp"

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

class Buffer;
class EventLoop;
class InetAddress;
class TcpConnection;
class TcpServer;
class Timestamp;

namespace nukv
{
namespace proto
{
class ClientRequest;
class ClientResponse;
}

class ClientServer final
{
public:
    ClientServer(EventLoop* loop, const InetAddress& listen_address, RaftNode& node);
    ~ClientServer();

    ClientServer(const ClientServer&) = delete;
    ClientServer& operator=(const ClientServer&) = delete;

    void Start();

private:
    using ConnectionPtr = std::shared_ptr<TcpConnection>;

    void OnConnection(const std::shared_ptr<TcpConnection>& connection);

    void OnMessage(
        const std::shared_ptr<TcpConnection>& connection,
        Buffer* buffer,
        Timestamp timestamp);

    void HandleRequest(
        const ConnectionPtr& connection,
        proto::ClientRequest request);

    void Enqueue(std::function<void()> task);
    void WorkerLoop();

    void SendResponse(
        const std::shared_ptr<TcpConnection>& connection,
        const proto::ClientResponse& response);

    EventLoop* loop_;
    RaftNode& node_;
    std::unique_ptr<TcpServer> server_;
    std::mutex work_mutex_;
    std::condition_variable work_condition_;
    std::deque<std::function<void()>> work_queue_;
    std::vector<std::thread> workers_;
    bool stopping_{false};
};
}
