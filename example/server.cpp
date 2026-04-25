#include "rpc_application.h"
#include "rpc_provider.h"
#include "user_service.h"
#include "order_service.h"

#include <glog/logging.h>
#include <iostream>
#include <signal.h>
#include <thread>

// 全局指针，用于在信号处理函数中访问 Provider
RpcProvider *g_provider = nullptr;

// 信号处理函数
void SigTermHandler(int sig)
{
    LOG(INFO) << "===========================================";
    LOG(INFO) << "Received signal " << sig << ", starting Graceful Shutdown...";

    // 开启一个分离的后台线程来执行停机序列，避免阻塞操作系统的信号处理
    std::thread([]() {
        // 第一步：主动断开 ZK，摘除流量
        // 此时新的请求会被客户端路由到其他存活的 Server
        RpcApplication::GetInstance().GetZkClient().Close();

        LOG(INFO) << "ZK session closed. Waiting 3 seconds for clients to flush cache and in-flight requests to complete...";

        // 第二步：保持 muduo 运行，缓冲 3 秒
        // 这 3 秒内，muduo 会继续把队列里还没处理完的请求处理掉，并返回给客户端
        std::this_thread::sleep_for(std::chrono::seconds(3));

        // 第三步：平滑停止网络服务，进程退出
        if (g_provider) {
            g_provider->GetEventLoop()->quit();
        }

        LOG(INFO) << "Graceful Shutdown completed. Goodbye!";
        exit(0); // 兜底安全退出
    }).detach();
}

int main(int argc, char **argv)
{
    // 1. 注册信号捕获
    // SIGINT 对应 Ctrl+C，SIGTERM 对应 kill -15
    signal(SIGINT, SigTermHandler);
    signal(SIGTERM, SigTermHandler);

    // 初始化RPC应用程序
    RpcApplication::GetInstance().Init(argc, argv);

    RpcProvider provider;
    g_provider = &provider; // 赋值给全局指针

    provider.NotifyService(new UserService()); // 登录注册服务
    provider.NotifyService(new OrderService()); // 订单服务

    provider.Run();
    return 0;
}
