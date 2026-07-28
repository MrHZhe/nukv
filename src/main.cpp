#include "nukv/raft_node.hpp"

#include <filesystem>
#include <iostream>
#include <thread>
#include <chrono>

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
            std::cout << "This node is the Leader." << std::endl;
        }
        else
        {
            std::cout << "Leader election timed out." << std::endl;
        }

        std::cout
            << "NuKV node started successfully.\n"
            << "Press Enter to stop..."
            << std::endl;

        std::cin.get();

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