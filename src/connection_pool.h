#ifndef CONNECTION_POOL_H
#define CONNECTION_POOL_H

#include <arpa/inet.h>
#include <chrono>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
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
    int GetConnection(const std::string &ip, uint16_t port);

    // 将健康的连接放回连接池
    void ReleaseConnection(const std::string &ip, uint16_t port, int fd);

    // 销毁坏掉的连接
    void CloseConnection(int fd);

private:
    ConnectionPool() = default;
    ~ConnectionPool();

    // 内部创建新连接的逻辑
    int CreateNewConnection(const std::string &ip, uint16_t port);

    bool Ping(int fd);

    ssize_t recv_exact(int fd, char *buffer, size_t length);

    // 将队列中存储的 fd 改为带有时间戳的结构体
    // 连接在池子里排队时，它是 ConnectionItem（需要记录时间以供心跳检测）；连接被业务取走使用时，它退化为普通的 int fd。
    struct ConnectionItem {
        int fd;
        std::chrono::steady_clock::time_point last_active_time;
    };

    // 核心数据结构：Key 为 "ip:port"，Value 为可用的 fd 队列
    std::unordered_map<std::string, std::queue<ConnectionItem>> pool_;
    std::mutex mutex_;
};

#endif // CONNECTION_POOL_H
