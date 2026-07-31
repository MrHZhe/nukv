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

int main()
{
    try
    {
        std::filesystem::create_directories("./data/node1");
        std::filesystem::create_directories("./data/node2");
        std::filesystem::create_directories("./data/node3");

        const std::vector<nukv::RaftPeer> peers = {
            {1, "127.0.0.1:19001"},
            {2, "127.0.0.1:19002"},
            {3, "127.0.0.1:19003"}
        };

        nukv::RaftNode node1(
            1,
            "127.0.0.1:19001",
            19001,
            "./data/node1/state_db",
            "./data/node1/raft_meta",
            peers
        );

        nukv::RaftNode node2(
            2,
            "127.0.0.1:19002",
            19002,
            "./data/node2/state_db",
            "./data/node2/raft_meta",
            peers
        );

        nukv::RaftNode node3(
            3,
            "127.0.0.1:19003",
            19003,
            "./data/node3/state_db",
            "./data/node3/raft_meta",
            peers
        );

        std::exception_ptr node1_error;
        std::exception_ptr node2_error;
        std::exception_ptr node3_error;

        std::thread thread1(
            [&node1, &node1_error]()
            {
                try
                {
                    node1.Start();
                }
                catch (...)
                {
                    node1_error =
                        std::current_exception();
                }
            }
        );

        std::thread thread2(
            [&node2, &node2_error]()
            {
                try
                {
                    node2.Start();
                }
                catch (...)
                {
                    node2_error =
                        std::current_exception();
                }
            }
        );

        std::thread thread3(
            [&node3, &node3_error]()
            {
                try
                {
                    node3.Start();
                }
                catch (...)
                {
                    node3_error =
                        std::current_exception();
                }
            }
        );

        thread1.join();
        thread2.join();
        thread3.join();

        if (node1_error)
        {
            std::rethrow_exception(node1_error);
        }

        if (node2_error)
        {
            std::rethrow_exception(node2_error);
        }

        if (node3_error)
        {
            std::rethrow_exception(node3_error);
        }

        auto find_leader =
            [&node1, &node2, &node3]()
                -> nukv::RaftNode*
            {
                if (node1.IsLeader())
                {
                    return &node1;
                }

                if (node2.IsLeader())
                {
                    return &node2;
                }

                if (node3.IsLeader())
                {
                    return &node3;
                }

                return nullptr;
            };

        auto get_node_id =
            [&node1, &node2, &node3](
                const nukv::RaftNode* node)
            {
                if (node == &node1)
                {
                    return 1;
                }

                if (node == &node2)
                {
                    return 2;
                }

                if (node == &node3)
                {
                    return 3;
                }

                return 0;
            };

        nukv::RaftNode* leader = nullptr;

        for (int i = 0; i < 50; ++i)
        {
            leader = find_leader();

            if (leader)
            {
                break;
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(100)
            );
        }

        if (!leader)
        {
            std::cout
                << "Leader election timed out."
                << std::endl;

            node1.Stop();
            node2.Stop();
            node3.Stop();

            return 1;
        }

        std::cout
            << "Three NuKV nodes started successfully."
            << std::endl;

        std::cout
            << "Leader elected: node"
            << get_node_id(leader)
            << std::endl;

        std::cout
            << "Commands: "
            << "status, "
            << "stop <node_id>, "
            << "put <key> <value>, "
            << "get <key>, "
            << "del <key>, "
            << "check <key>, "
            << "exit"
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

            if (operation == "status")
            {
                leader = find_leader();

                if (leader)
                {
                    std::cout
                        << "Leader: node"
                        << get_node_id(leader)
                        << std::endl;
                }
                else
                {
                    std::cout
                        << "NO_LEADER"
                        << std::endl;
                }

                continue;
            }

            if (operation == "stop")
            {
                int node_id = 0;

                if (!(input >> node_id)
                    || node_id < 1
                    || node_id > 3)
                {
                    std::cout
                        << "Usage: stop <1|2|3>"
                        << std::endl;

                    continue;
                }

                if (node_id == 1)
                {
                    node1.Stop();
                }
                else if (node_id == 2)
                {
                    node2.Stop();
                }
                else
                {
                    node3.Stop();
                }

                std::cout
                    << "node"
                    << node_id
                    << " stopped"
                    << std::endl;

                continue;
            }

            if (operation == "check")
            {
                std::string key;

                if (!(input >> key))
                {
                    std::cout
                        << "Usage: check <key>"
                        << std::endl;

                    continue;
                }

                const auto print_value =
                    [&key](
                        int node_id,
                        const nukv::RaftNode& node)
                    {
                        const auto value =
                            node.GetLocal(key);

                        std::cout
                            << "node"
                            << node_id
                            << ": ";

                        if (value.has_value())
                        {
                            std::cout
                                << value.value();
                        }
                        else
                        {
                            std::cout
                                << "NOT_FOUND";
                        }

                        std::cout << std::endl;
                    };

                print_value(1, node1);
                print_value(2, node2);
                print_value(3, node3);

                continue;
            }

            leader = find_leader();

            if (!leader)
            {
                std::cout
                    << "NO_LEADER"
                    << std::endl;

                continue;
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

                if (leader->Submit(command))
                {
                    std::cout
                        << "OK"
                        << std::endl;
                }
                else
                {
                    std::cout
                        << "PUT failed"
                        << std::endl;
                }
            }
            else if (operation == "get")
            {
                std::string key;

                if (!(input >> key))
                {
                    std::cout
                        << "Usage: get <key>"
                        << std::endl;

                    continue;
                }

                const auto value =
                    leader->GetLocal(key);

                if (value.has_value())
                {
                    std::cout
                        << value.value()
                        << std::endl;
                }
                else
                {
                    std::cout
                        << "NOT_FOUND"
                        << std::endl;
                }
            }
            else if (operation == "del")
            {
                std::string key;

                if (!(input >> key))
                {
                    std::cout
                        << "Usage: del <key>"
                        << std::endl;

                    continue;
                }

                nukv::proto::Command command;

                command.set_type(
                    nukv::proto::
                        COMMAND_TYPE_DELETE
                );

                command.set_key(key);

                if (leader->Submit(command))
                {
                    std::cout
                        << "OK"
                        << std::endl;
                }
                else
                {
                    std::cout
                        << "DELETE failed"
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

        node1.Stop();
        node2.Stop();
        node3.Stop();

        std::cout
            << "NuKV cluster stopped."
            << std::endl;
    }
    catch (const std::exception& error)
    {
        std::cerr
            << "Failed to run NuKV cluster: "
            << error.what()
            << std::endl;

        return 1;
    }

    return 0;
}