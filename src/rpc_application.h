#ifndef RPC_APPLICATION_H
#define RPC_APPLICATION_H

#include "rpc_config.h"
#include "zk_client.h"

// RpcApplication类，负责整个RPC框架的初始化和配置管理
class RpcApplication {
public:
    static RpcApplication &GetInstance();
    static void Init(int argc, char **argv); // 初始化框架，加载配置文件并启动 ZooKeeper 客户端
    RpcConfig &GetConfig();
    ZkClient &GetZkClient();

private:
    RpcApplication() = default;
    ~RpcApplication() = default;
    RpcApplication(const RpcApplication &) = delete;
    RpcApplication &operator=(const RpcApplication &) = delete;

    RpcConfig config;
    ZkClient zk_client;
};

#endif
