#include "nukv/client_server.hpp"

#include "Buffer.h"
#include "InetAddress.h"
#include "TcpConnection.h"
#include "TcpServer.h"
#include "Timestamp.h"
#include "client.pb.h"
#include "command.pb.h"

#include <arpa/inet.h>

#include <cstdint>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string>

namespace
{
constexpr std::uint32_t kMaxFrameSize = 16U * 1024U * 1024U;

std::uint32_t ReadUint32(const char* data)
{
    std::uint32_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return ntohl(value);
}

std::string Frame(const std::string& payload)
{
    if (payload.size() > kMaxFrameSize)
    {
        throw std::runtime_error("client response is too large");
    }

    const std::uint32_t length =
        htonl(static_cast<std::uint32_t>(payload.size()));

    std::string framed(sizeof(length), '\0');
    std::memcpy(framed.data(), &length, sizeof(length));
    framed += payload;
    return framed;
}

nukv::proto::ClientResponse MakeError(
    std::uint64_t request_id,
    nukv::proto::ClientResponse::Status status,
    const std::string& message)
{
    nukv::proto::ClientResponse response;
    response.set_request_id(request_id);
    response.set_status(status);
    response.set_message(message);
    return response;
}
}

namespace nukv
{
ClientServer::ClientServer(
    EventLoop* loop,
    const InetAddress& listen_address,
    RaftNode& node)
    : loop_(loop)
    , node_(node)
    , server_(new TcpServer(loop, listen_address, "NuKVClientServer"))
{
    server_->setThreadNum(2);
    server_->setConnectionCallback(
        std::bind(&ClientServer::OnConnection, this, std::placeholders::_1));
    server_->setMessageCallback(
        std::bind(
            &ClientServer::OnMessage,
            this,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3));

    workers_.reserve(2);
    for (int i = 0; i < 2; ++i)
    {
        workers_.emplace_back(&ClientServer::WorkerLoop, this);
    }
}

ClientServer::~ClientServer()
{
    {
        std::lock_guard<std::mutex> lock(work_mutex_);
        stopping_ = true;
    }
    work_condition_.notify_all();

    for (auto& worker : workers_)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }
}

void ClientServer::Start()
{
    server_->start();
}

void ClientServer::OnConnection(const std::shared_ptr<TcpConnection>&)
{}

void ClientServer::OnMessage(
    const std::shared_ptr<TcpConnection>& connection,
    Buffer* buffer,
    Timestamp)
{
    while (buffer->readableBytes() >= sizeof(std::uint32_t))
    {
        const std::uint32_t payload_size = ReadUint32(buffer->peek());

        if (payload_size > kMaxFrameSize)
        {
            SendResponse(
                connection,
                MakeError(
                    0,
                    proto::ClientResponse::STATUS_INVALID_REQUEST,
                    "request frame is too large"));
            connection->shutdown();
            return;
        }

        const std::size_t frame_size = sizeof(std::uint32_t) + payload_size;
        if (buffer->readableBytes() < frame_size)
        {
            return;
        }

        buffer->retrieve(sizeof(std::uint32_t));
        const std::string payload = buffer->retrieveAsString(payload_size);

        proto::ClientRequest request;
        if (!request.ParseFromString(payload))
        {
            SendResponse(
                connection,
                MakeError(
                    0,
                    proto::ClientResponse::STATUS_INVALID_REQUEST,
                    "invalid protobuf request"));
            continue;
        }

        if (request.key().empty() || request.key().rfind("__raft/", 0) == 0)
        {
            SendResponse(
                connection,
                MakeError(
                request.request_id(),
                proto::ClientResponse::STATUS_INVALID_REQUEST,
                request.key().empty() ? "key must not be empty" : "reserved key"));
            continue;
        }

        Enqueue([this, connection, request = std::move(request)]() mutable
        {
            HandleRequest(connection, std::move(request));
        });
    }
}

void ClientServer::HandleRequest(
    const ConnectionPtr& connection,
    proto::ClientRequest request)
{
    proto::ClientResponse response;
    response.set_request_id(request.request_id());

    if (!node_.IsReady())
    {
        response = MakeError(
            request.request_id(),
            proto::ClientResponse::STATUS_NOT_READY,
            "Raft node is still starting");
    }
    else if (!node_.IsLeader())
    {
        response = MakeError(
            request.request_id(),
            proto::ClientResponse::STATUS_NOT_LEADER,
            "request must be sent to the leader");
    }
    else
    {
        try
        {
            proto::Command command;
            command.set_request_id(request.request_id());
            command.set_key(request.key());
            command.set_value(request.value());

            switch (request.operation())
            {
                case proto::ClientRequest::OPERATION_PUT:
                    command.set_type(proto::COMMAND_TYPE_PUT);
                    if (node_.Submit(command))
                    {
                        response.set_status(proto::ClientResponse::STATUS_OK);
                    }
                    else
                    {
                        response = MakeError(
                            request.request_id(),
                            proto::ClientResponse::STATUS_ERROR,
                            "put failed");
                    }
                    break;

                case proto::ClientRequest::OPERATION_GET:
                    command.set_type(proto::COMMAND_TYPE_GET);
                    if (!node_.Submit(command))
                    {
                        response = MakeError(
                            request.request_id(),
                            proto::ClientResponse::STATUS_ERROR,
                            "get failed");
                        break;
                    }

                    {
                        const auto value = node_.GetLocal(request.key());
                        if (value.has_value())
                        {
                            response.set_status(proto::ClientResponse::STATUS_OK);
                            response.set_value(value.value());
                        }
                        else
                        {
                            response.set_status(
                                proto::ClientResponse::STATUS_NOT_FOUND);
                        }
                    }
                    break;

                case proto::ClientRequest::OPERATION_DELETE:
                    command.set_type(proto::COMMAND_TYPE_DELETE);
                    if (node_.Submit(command))
                    {
                        response.set_status(proto::ClientResponse::STATUS_OK);
                    }
                    else
                    {
                        response = MakeError(
                            request.request_id(),
                            proto::ClientResponse::STATUS_ERROR,
                            "delete failed");
                    }
                    break;

                case proto::ClientRequest::OPERATION_UNSPECIFIED:
                default:
                    response = MakeError(
                        request.request_id(),
                        proto::ClientResponse::STATUS_INVALID_REQUEST,
                        "unsupported operation");
                    break;
            }
        }
        catch (const std::exception& error)
        {
            response = MakeError(
                request.request_id(),
                proto::ClientResponse::STATUS_ERROR,
                error.what());
        }
    }

    connection->getLoop()->runInLoop(
        [this, connection, response = std::move(response)]() mutable
        {
            SendResponse(connection, response);
        });
}

void ClientServer::Enqueue(std::function<void()> task)
{
    {
        std::lock_guard<std::mutex> lock(work_mutex_);
        if (stopping_)
        {
            return;
        }
        work_queue_.emplace_back(std::move(task));
    }
    work_condition_.notify_one();
}

void ClientServer::WorkerLoop()
{
    while (true)
    {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(work_mutex_);
            work_condition_.wait(
                lock,
                [this]
                {
                    return stopping_ || !work_queue_.empty();
                });

            if (stopping_ && work_queue_.empty())
            {
                return;
            }

            task = std::move(work_queue_.front());
            work_queue_.pop_front();
        }

        task();
    }
}

void ClientServer::SendResponse(
    const std::shared_ptr<TcpConnection>& connection,
    const proto::ClientResponse& response)
{
    std::string payload;
    if (!response.SerializeToString(&payload))
    {
        throw std::runtime_error("failed to serialize client response");
    }

    connection->send(Frame(payload));
}
}
