#include "rpc_application.h"
#include "rpc_provider.h"
#include "user_service.h"

#include <glog/logging.h>
#include <signal.h>
#include <thread>

// 全局指针，用于在信号处理函数中访问 Provider
RpcProvider *g_provider = nullptr;

int main(int argc, char **argv)
{
    // 1. 信号隔离：屏蔽当前线程（也就是主线程）的 SIGINT 和 SIGTERM
    // 这样后续所有由主线程派生出来的业务线程、Muduo 网络线程，都会继承这个掩码，免疫这些信号。
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &set, nullptr);

    // 2. 启动优雅停机专属守护线程
    std::thread([&set]() {
        int sig;
        // sigwait 会在这里死等。直到外界发来 SIGINT 或 SIGTERM，它才会往下走。
        // 因为它是同步往下走的，所以这里执行的所有代码都是绝对安全的普通线程上下文。
        sigwait(&set, &sig);

        LOG(INFO) << "===========================================";
        LOG(INFO) << "Received signal " << sig << ", starting graceful shutdown...";

        // 第一步：主动断开 ZK，摘除流量
        RpcApplication::GetInstance().GetZkClient().Close();
        LOG(INFO) << "ZK session closed. Waiting 3 seconds for in-flight requests...";

        // 第二步：保持 muduo 运行，缓冲 3 秒
        std::this_thread::sleep_for(std::chrono::seconds(3));

        // 第三步：平滑停止网络服务
        if (g_provider) {
            g_provider->GetEventLoop()->quit();
        }

        LOG(INFO) << "Graceful shutdown thread finished task.";
    }).detach();

    // 3. 正常启动业务逻辑
    RpcApplication::GetInstance().Init(argc, argv);

    RpcProvider provider;
    g_provider = &provider;

    provider.NotifyService(new UserService());  // 登录注册服务

    provider.Run();
    LOG(INFO) << "Server completely shutdown. Goodbye!";
    return 0;
}
