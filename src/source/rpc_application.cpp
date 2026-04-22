#include "rpc_application.h"

#include <muduo/base/Logging.h>
#include <glog/logging.h>
#include <iostream>
#include <unistd.h>

RpcApplication &RpcApplication::GetInstance()
{
    static RpcApplication app;
    return app;
}

void RpcApplication::Init(int argc, char **argv)
{
    if (argc < 2) {
        LOG(ERROR) << "Usage: " << argv[0] << " -i <config_file>";
        exit(EXIT_FAILURE);
    }

    // 解析命令行参数，获取配置文件路径
    int opt;
    std::string config_file;
    while ((opt = getopt(argc, argv, "i:")) != -1) {
        switch (opt) {
            case 'i':
                config_file = optarg;
                break;
            case '?': // 未知参数
            case ':': // 缺少参数
            default:
                LOG(ERROR) << "Invalid option: " << optopt;
                exit(EXIT_FAILURE);
        }
    }

    if (config_file.empty()) {
        LOG(ERROR) << "Config file is not specified. Use -i <config_file>";
        exit(EXIT_FAILURE);
    }

    if (!GetInstance().config.LoadConfigFile(config_file)) {
        LOG(ERROR) << "Failed to load config file: " << config_file;
        exit(EXIT_FAILURE);
    }

    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    FLAGS_alsologtostderr = true;
    muduo::Logger::setLogLevel(muduo::Logger::WARN);

    // 启动 ZooKeeper 客户端，阻塞等待连接成功
    GetInstance().zk_client.Start();
}

RpcConfig &RpcApplication::GetConfig()
{
    return config;
}

ZkClient &RpcApplication::GetZkClient()
{
    return zk_client;
}
