#ifndef CONNECTION_POOL_H
#define CONNECTION_POOL_H

#include <chrono>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>

class ConnectionPool {
public:
    // 获取全局单例
    static ConnectionPool &GetInstance()
    {
        static ConnectionPool pool;
        return pool;
    }

    // 从连接池获取一个可用的连接
    // 返回 > 0：成功拿到 fd。
    // 返回 -1：网络建连失败（服务器挂了）。
    // 返回 -2：达到最大连接数上限（服务器正忙）。
    int GetConnection(const std::string &ip, uint16_t port);

    // 将健康的连接放回连接池
    void ReleaseConnection(const std::string &ip, uint16_t port, int fd);

    // 销毁坏掉的连接
    void CloseConnection(const std::string &ip, uint16_t port, int fd);

private:
    ConnectionPool();
    ~ConnectionPool();

    // 内部创建新连接的逻辑
    int CreateNewConnection(const std::string &ip, uint16_t port);

    bool Ping(int fd);

    // 将队列中存储的 fd 改为带有时间戳的结构体
    // 连接在池子里排队时，它是 ConnectionItem（需要记录时间以供心跳检测）；连接被业务取走使用时，它退化为普通的 int fd。
    struct ConnectionItem {
        int fd;
        std::chrono::steady_clock::time_point last_active_time;

        ConnectionItem(int f, std::chrono::steady_clock::time_point t) : fd(f), last_active_time(t) {}
    };

    // 空闲连接池：Key 为 "ip:port"，Value 为可用的 fd 队列
    std::unordered_map<std::string, std::queue<ConnectionItem>> pool;
    std::mutex mutex;
    int max_idle_per_host;
    int max_active_per_host;
    // 记录每个 ip:port 当前一共创建了多少个连接（包含正在使用的）
    std::unordered_map<std::string, int> active_counts;
};

#endif // CONNECTION_POOL_H
