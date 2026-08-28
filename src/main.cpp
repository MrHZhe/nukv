#include "nukv/raft_node.hpp"
#include "nukv/client_server.hpp"
#include "EventLoop.h"
#include "InetAddress.h"
#include "command.pb.h"

#include <exception>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <future>
#include <thread>
#include <vector>

namespace
{
struct ServerOptions
{
    int node_id = 0;
    int client_port = 0;
    std::string data_directory;
    std::vector<nukv::RaftPeer> peers;
};

const std::vector<nukv::RaftPeer> kDefaultPeers = {
    {1, "127.0.0.1:19001"},
    {2, "127.0.0.1:19002"},
    {3, "127.0.0.1:19003"}
};

void PrintUsage()
{
    std::cerr
        << "Usage:\n"
        << "  nukv_server <node_id> [client_port]\n"
        << "  nukv_server --node-id <id> [options]\n\n"
        << "Options:\n"
        << "  --config <path>\n"
        << "  --node-id <1|2|3>\n"
        << "  --peers <id=host:port,id=host:port,id=host:port>\n"
        << "  --client-port <port>\n"
        << "  --data-dir <path>\n"
        << "  --help\n\n"
        << "Without --peers, the default local cluster is used:\n"
        << "  1=127.0.0.1:19001,2=127.0.0.1:19002,3=127.0.0.1:19003\n";
}

std::string Trim(const std::string& value)
{
    const std::size_t begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos)
    {
        return {};
    }
    const std::size_t end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

int ParseInteger(const std::string& value, const std::string& name)
{
    std::size_t parsed_length = 0;
    const int parsed = std::stoi(value, &parsed_length);
    if (parsed_length != value.size())
    {
        throw std::invalid_argument(name + " must be an integer");
    }
    return parsed;
}

int ParsePort(const std::string& value, const std::string& name)
{
    const int port = ParseInteger(value, name);
    if (port < 1 || port > 65535)
    {
        throw std::invalid_argument(name + " must be between 1 and 65535");
    }
    return port;
}

std::vector<nukv::RaftPeer> ParsePeers(const std::string& value)
{
    std::vector<nukv::RaftPeer> peers;
    std::set<int> ids;
    std::size_t begin = 0;

    while (begin < value.size())
    {
        const std::size_t end = value.find(',', begin);
        const std::string item = value.substr(
            begin,
            end == std::string::npos ? std::string::npos : end - begin);
        const std::size_t separator = item.find('=');

        if (separator == std::string::npos ||
            separator == 0 ||
            separator + 1 >= item.size())
        {
            throw std::invalid_argument(
                "peers must use id=host:port format");
        }

        const int id = ParseInteger(item.substr(0, separator), "peer id");
        const std::string endpoint = item.substr(separator + 1);

        if (id < 1 || id > 3 || !ids.insert(id).second)
        {
            throw std::invalid_argument("peer ids must be unique values 1, 2, or 3");
        }

        const std::size_t colon = endpoint.rfind(':');
        if (colon == std::string::npos || colon == 0 || colon + 1 >= endpoint.size())
        {
            throw std::invalid_argument("peer endpoint must use host:port format");
        }
        ParsePort(endpoint.substr(colon + 1), "peer port");
        peers.push_back({id, endpoint});

        if (end == std::string::npos)
        {
            break;
        }
        begin = end + 1;
    }

    if (peers.size() != 3 || ids.size() != 3)
    {
        throw std::invalid_argument("exactly three peers are required");
    }

    return peers;
}

std::string RequireValue(int& index, int argc, char* argv[], const std::string& option)
{
    if (index + 1 >= argc)
    {
        throw std::invalid_argument(option + " requires a value");
    }
    ++index;
    return argv[index];
}

void LoadConfig(
    const std::string& path,
    ServerOptions& options,
    bool& node_id_set,
    bool& peers_set,
    bool& client_port_set,
    bool& data_directory_set)
{
    std::ifstream input(path);
    if (!input)
    {
        throw std::invalid_argument("failed to open config file: " + path);
    }

    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line))
    {
        ++line_number;
        const std::size_t comment = line.find('#');
        if (comment != std::string::npos)
        {
            line.erase(comment);
        }
        line = Trim(line);
        if (line.empty())
        {
            continue;
        }

        const std::size_t separator = line.find('=');
        if (separator == std::string::npos)
        {
            throw std::invalid_argument(
                "config line " + std::to_string(line_number) +
                " must use key=value format");
        }

        const std::string key = Trim(line.substr(0, separator));
        const std::string value = Trim(line.substr(separator + 1));
        if (key.empty() || value.empty())
        {
            throw std::invalid_argument(
                "config line " + std::to_string(line_number) +
                " has an empty key or value");
        }

        if (key == "node_id")
        {
            options.node_id = ParseInteger(value, "node_id");
            node_id_set = true;
        }
        else if (key == "peers")
        {
            options.peers = ParsePeers(value);
            peers_set = true;
        }
        else if (key == "client_port")
        {
            options.client_port = ParsePort(value, "client_port");
            client_port_set = true;
        }
        else if (key == "data_dir")
        {
            options.data_directory = value;
            data_directory_set = true;
        }
        else
        {
            throw std::invalid_argument(
                "unknown config key at line " + std::to_string(line_number) +
                ": " + key);
        }
    }
}

ServerOptions ParseOptions(int argc, char* argv[])
{
    ServerOptions options;
    options.peers = kDefaultPeers;

    bool node_id_set = false;
    bool peers_set = false;
    bool client_port_set = false;
    bool data_directory_set = false;

    // Load the config first so explicit command-line options can override it.
    std::string config_path;
    for (int scan_index = 1; scan_index < argc; ++scan_index)
    {
        if (std::string(argv[scan_index]) == "--config")
        {
            config_path = RequireValue(scan_index, argc, argv, "--config");
        }
    }
    if (!config_path.empty())
    {
        LoadConfig(
            config_path,
            options,
            node_id_set,
            peers_set,
            client_port_set,
            data_directory_set);
    }

    int index = 1;

    if (index < argc && argv[index][0] != '-')
    {
        options.node_id = ParseInteger(argv[index], "node_id");
        node_id_set = true;
        ++index;

        if (index < argc && argv[index][0] != '-')
        {
            options.client_port = ParsePort(argv[index], "client_port");
            client_port_set = true;
            ++index;
        }
    }

    while (index < argc)
    {
        const std::string option = argv[index];

        if (option == "--help")
        {
            PrintUsage();
            std::exit(0);
        }
        if (option == "--config")
        {
            RequireValue(index, argc, argv, option);
        }
        else if (option == "--node-id")
        {
            options.node_id = ParseInteger(
                RequireValue(index, argc, argv, option), "node_id");
            node_id_set = true;
        }
        else if (option == "--peers")
        {
            options.peers = ParsePeers(
                RequireValue(index, argc, argv, option));
            peers_set = true;
        }
        else if (option == "--client-port")
        {
            options.client_port = ParsePort(
                RequireValue(index, argc, argv, option), "client_port");
            client_port_set = true;
        }
        else if (option == "--data-dir")
        {
            options.data_directory = RequireValue(index, argc, argv, option);
            if (options.data_directory.empty())
            {
                throw std::invalid_argument("data directory must not be empty");
            }
            data_directory_set = true;
        }
        else
        {
            throw std::invalid_argument("unknown option: " + option);
        }
        ++index;
    }

    if (!node_id_set || options.node_id < 1 || options.node_id > 3)
    {
        throw std::invalid_argument("node_id must be 1, 2, or 3");
    }

    if (!client_port_set)
    {
        options.client_port = 18000 + options.node_id;
    }

    if (peers_set && !data_directory_set)
    {
        throw std::invalid_argument(
            "--data-dir is required with --peers to avoid reusing persisted cluster configuration");
    }

    if (!data_directory_set)
    {
        options.data_directory = "./data/node" + std::to_string(options.node_id);
    }

    if (!peers_set)
    {
        options.peers = kDefaultPeers;
    }

    return options;
}

const nukv::RaftPeer& FindLocalPeer(
    const std::vector<nukv::RaftPeer>& peers,
    int node_id)
{
    for (const auto& peer : peers)
    {
        if (peer.id == node_id)
        {
            return peer;
        }
    }
    throw std::invalid_argument("peers do not contain the selected node_id");
}
}

int main(int argc, char* argv[])
{
    try
    {
        const ServerOptions options = ParseOptions(argc, argv);
        const nukv::RaftPeer& local_peer =
            FindLocalPeer(options.peers, options.node_id);
        const std::size_t local_port_separator = local_peer.endpoint.rfind(':');
        const int raft_port = ParsePort(
            local_peer.endpoint.substr(local_port_separator + 1),
            "local Raft port");

        std::filesystem::create_directories(options.data_directory);

        nukv::RaftNode node(
            options.node_id,
            local_peer.endpoint,
            raft_port,
            options.data_directory + "/state_db",
            options.data_directory + "/raft_meta",
            options.peers);

        node.Start();

        std::promise<EventLoop*> network_loop_promise;
        std::future<EventLoop*> network_loop_future = network_loop_promise.get_future();
        std::thread network_thread(
            [&node, client_port = options.client_port,
             promise = std::move(network_loop_promise)]() mutable
            {
                try
                {
                    EventLoop loop;
                    InetAddress client_address(
                        static_cast<std::uint16_t>(client_port),
                        "0.0.0.0");
                    nukv::ClientServer client_server(&loop, client_address, node);
                    client_server.Start();
                    promise.set_value(&loop);
                    loop.loop();
                }
                catch (...)
                {
                    promise.set_exception(std::current_exception());
                }
            });

        EventLoop* network_loop = nullptr;
        try
        {
            network_loop = network_loop_future.get();
        }
        catch (...)
        {
            if (network_thread.joinable())
            {
                network_thread.join();
            }
            throw;
        }

        std::cout << "NuKV node" << options.node_id
                  << " started at " << local_peer.endpoint << std::endl;
        std::cout << "Client API listening at 0.0.0.0:"
                  << options.client_port << std::endl;
        std::cout << "Commands: status, put <key> <value>, get <key>, del <key>, exit" << std::endl;

        std::string line;

        while (true)
        {
            std::cout << "nukv-node" << options.node_id << "> " << std::flush;

            if (!std::getline(std::cin, line))
            {
                break;
            }

            std::istringstream input(line);
            std::string operation;
            input >> operation;

            if (operation.empty())
            {
                continue;
            }

            if (operation == "exit")
            {
                break;
            }

            if (operation == "status")
            {
                if (!node.IsReady())
                {
                    std::cout << "STARTING" << std::endl;
                }
                else
                {
                    std::cout << (node.IsLeader() ? "LEADER" : "FOLLOWER") << std::endl;
                }
                continue;
            }

            if (!node.IsReady())
            {
                std::cout << "NOT_READY" << std::endl;
                continue;
            }

            if (!node.IsLeader())
            {
                std::cout << "NOT_LEADER" << std::endl;
                continue;
            }

            if (operation == "put")
            {
                std::string key;
                std::string value;

                if (!(input >> key >> value))
                {
                    std::cout << "Usage: put <key> <value>" << std::endl;
                    continue;
                }

                if (key.rfind("__raft/", 0) == 0)
                {
                    std::cout << "Keys beginning with __raft/ are reserved" << std::endl;
                    continue;
                }

                nukv::proto::Command command;
                command.set_type(nukv::proto::COMMAND_TYPE_PUT);
                command.set_key(key);
                command.set_value(value);

                std::cout << (node.Submit(command) ? "OK" : "PUT failed") << std::endl;
            }
            else if (operation == "get")
            {
                std::string key;

                if (!(input >> key))
                {
                    std::cout << "Usage: get <key>" << std::endl;
                    continue;
                }

                if (key.rfind("__raft/", 0) == 0)
                {
                    std::cout << "Keys beginning with __raft/ are reserved" << std::endl;
                    continue;
                }

                nukv::proto::Command command;
                command.set_type(nukv::proto::COMMAND_TYPE_GET);
                command.set_key(key);

                if (!node.Submit(command))
                {
                    std::cout << "GET failed" << std::endl;
                    continue;
                }

                const auto value = node.GetLocal(key);
                std::cout << (value.has_value() ? value.value() : "NOT_FOUND") << std::endl;
            }
            else if (operation == "del")
            {
                std::string key;

                if (!(input >> key))
                {
                    std::cout << "Usage: del <key>" << std::endl;
                    continue;
                }

                if (key.rfind("__raft/", 0) == 0)
                {
                    std::cout << "Keys beginning with __raft/ are reserved" << std::endl;
                    continue;
                }

                nukv::proto::Command command;
                command.set_type(nukv::proto::COMMAND_TYPE_DELETE);
                command.set_key(key);

                std::cout << (node.Submit(command) ? "OK" : "DELETE failed") << std::endl;
            }
            else
            {
                std::cout << "Unknown command" << std::endl;
            }
        }

        network_loop->quit();
        if (network_thread.joinable())
        {
            network_thread.join();
        }

        node.Stop();
        std::cout << "NuKV node" << options.node_id << " stopped." << std::endl;
    }
    catch (const std::exception& error)
    {
        std::cerr << "Failed to run NuKV node: " << error.what() << std::endl;
        return 1;
    }

    return 0;
}
