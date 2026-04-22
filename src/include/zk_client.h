#ifndef ZK_CLIENT_H
#define ZK_CLIENT_H

#include <zookeeper/zookeeper.h>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

// 定义观察者回调：当监听的节点发生变化时，通知上层（如 MyRpcChannel）
using ZkNotifyHandler = std::function<void(int type, const std::string &path)>;

// 封装的 ZooKeeper 客户端类
class ZkClient {
public:
    ZkClient();
    ~ZkClient();

    // 启动连接 ZkServer，增加超时检测
    void Start(int timeout_ms = 5000);

    // 用于主动断开连接，快速删除临时节点
    void Close();

    // 在 ZkServer 上根据指定的 path 创建 znode 节点
    // state = 0 默认是持久性节点, state = ZOO_EPHEMERAL 是临时性节点
    void Create(const char *path, const char *data, int datalen, int state = 0);

    // 获取指定路径下的所有子节点，支持开启 Watcher
    std::vector<std::string> GetChildren(const char *path, bool watch = false);

    // 设置上层业务的回调接口
    void SetNotifyHandler(ZkNotifyHandler handler);

    // 内部 Watcher 调用接口
    void InvokeNotifyHandler(int type, const char *path);

    // 供全局 watcher 回调使用的通知接口
    void NotifyConnected();

private:
    zhandle_t *zk_handle;
    std::mutex mtx;
    std::condition_variable cv;
    bool connected;                 // 标志位，防止条件变量虚假唤醒
    ZkNotifyHandler notify_handler; // 业务层的回调函数
};

#endif
