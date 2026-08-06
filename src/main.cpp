#include "nukv/raft_node.hpp"
#include "command.pb.h"

#include <chrono>
#include <exception>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <iostream>

int main(int argc, char* argv[])
{
    try
    {
        if (argc != 2)
        {
            std::cerr << "Usage: nukv_server <node_id>" << std::endl;
            return 1;
        }

        std::size_t parsed_length = 0;
        const std::string node_id_argument = argv[1];
        const int node_id = std::stoi(node_id_argument, &parsed_length);

        if (parsed_length != node_id_argument.size() || node_id < 1 || node_id > 3)
        {
            std::cerr << "node_id must be 1, 2, or 3" << std::endl;
            return 1;
        }

        const std::vector<nukv::RaftPeer> peers = {
            {1, "127.0.0.1:19001"},
            {2, "127.0.0.1:19002"},
            {3, "127.0.0.1:19003"}
        };

        const int port = 19000 + node_id;
        const std::string endpoint = "127.0.0.1:" + std::to_string(port);
        const std::string data_directory = "./data/node" + std::to_string(node_id);

        std::filesystem::create_directories(data_directory);

        nukv::RaftNode node(
            node_id,
            endpoint,
            port,
            data_directory + "/state_db",
            data_directory + "/raft_meta",
            peers);

        node.Start();

        std::cout << "NuKV node" << node_id << " started at " << endpoint << std::endl;
        std::cout << "Commands: status, put <key> <value>, get <key>, del <key>, exit" << std::endl;

        std::string line;

        while (true)
        {
            std::cout << "nukv-node" << node_id << "> " << std::flush;

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
                std::cout << (node.IsLeader() ? "LEADER" : "FOLLOWER") << std::endl;
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

        node.Stop();
        std::cout << "NuKV node" << node_id << " stopped." << std::endl;
    }
    catch (const std::exception& error)
    {
        std::cerr << "Failed to run NuKV node: " << error.what() << std::endl;
        return 1;
    }

    return 0;
}