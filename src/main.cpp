#include "nukv/raft_node.hpp"
#include "command.pb.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

int main()
{
    try
    {
        std::filesystem::create_directories("./data");

        nukv::RaftNode node(
            1,
            "127.0.0.1:19001",
            19001,
            "./data/node1"
        );

        node.Start();

        for (int i = 0; i < 30 && !node.IsLeader(); ++i)
        {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(100)
            );
        }

        if (node.IsLeader())
        {
            std::cout
                << "This node is the Leader."
                << std::endl;
        }
        else
        {
            std::cout
                << "Leader election timed out."
                << std::endl;
        }

        std::cout
            << "NuKV node started successfully."
            << std::endl;

        std::cout
            << "Commands: put <key> <value>, exit"
            << std::endl;

        std::string line;

        while (true)
        {
            std::cout << "nukv> " << std::flush;

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

            if (operation == "put")
            {
                std::string key;
                std::string value;

                if (!(input >> key >> value))
                {
                    std::cout
                        << "Usage: put <key> <value>"
                        << std::endl;

                    continue;
                }

                nukv::proto::Command command;
                command.set_type(
                    nukv::proto::COMMAND_TYPE_PUT
                );
                command.set_key(key);
                command.set_value(value);

                if (node.Submit(command))
                {
                    std::cout << "OK" << std::endl;
                }
                else
                {
                    std::cout
                        << "PUT failed: this node may not be Leader"
                        << std::endl;
                }
            }
            else
            {
                std::cout
                    << "Unknown command"
                    << std::endl;
            }
        }

        node.Stop();

        std::cout
            << "NuKV node stopped."
            << std::endl;
    }
    catch (const std::exception& error)
    {
        std::cerr
            << "Failed to run NuKV node: "
            << error.what()
            << std::endl;

        return 1;
    }

    return 0;
}