#ifndef ZK_CLIENT_H
#define ZK_CLIENT_H

#include <zookeeper/zookeeper.h>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <shared_mutex>
#include <vector>

// 定义观察者回调：当监听的节点发生变化时，通知上层 MyRpcChannel
using ZkNotifyHandler = std::function<void(int type, const std::string &path)>;
// 用于恢复状态的回调
using SessionRecoveryHandler = std::function<void()>;

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
    bool GetChildren(const char *path, std::vector<std::string> &children, bool watch = false);

    // 设置上层业务的回调接口
    void SubscribeWatcher(const std::string &path, ZkNotifyHandler handler);

    // 内部 Watcher 调用接口
    void InvokeNotifyHandler(int type, const char *path);

    // 供全局 watcher 回调使用的通知接口
    void NotifyConnected();

    // 设置恢复回调，通常由 RpcProvider 调用
    void SetRecoveryHandler(SessionRecoveryHandler handler);

    // 重新初始化的方法
    void Reconnect();

private:
    zhandle_t *zk_handle;
    std::mutex mtx;
    std::condition_variable cv;
    bool connected;                 // 标志位，防止条件变量虚假唤醒

    // Key: ZK的节点路径 (例如 "/UserService/Login")
    // Value: 对应的回调函数
    std::unordered_map<std::string, ZkNotifyHandler> notify_handlers;
    std::shared_mutex handlers_mtx; // 防止多线程同时注册/触发时产生数据竞争

    SessionRecoveryHandler recovery_handler;
    // 保存初始化的参数，以便重连使用
    std::string conn_str;
};

#endif
