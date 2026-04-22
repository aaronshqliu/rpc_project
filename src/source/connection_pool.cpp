#include "connection_pool.h"
#include "rpc_header.pb.h"
#include <netinet/tcp.h>
#include <sys/time.h>

ConnectionPool::~ConnectionPool()
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto &pair : pool_) {
        while (!pair.second.empty()) {
            // 取出结构体
            ConnectionItem item = pair.second.front();
            pair.second.pop();
            // 从结构体中提取 fd 并关闭
            if (item.fd != -1) {
                close(item.fd);
            }
        }
    }
}

ssize_t ConnectionPool::recv_exact(int fd, char *buffer, size_t length)
{
    size_t total_received = 0;
    while (total_received < length) {
        ssize_t bytes = recv(fd, buffer + total_received, length - total_received, 0);
        if (bytes > 0) {
            total_received += bytes;
        } else if (bytes == 0) {
            // 对端关闭了连接
            return total_received;
        } else {
            // 被系统信号中断，继续尝试读取
            if (errno == EINTR) {
                continue;
            }
            // 真正发生网络错误
            return -1;
        }
    }
    return total_received;
}

bool ConnectionPool::Ping(int fd)
{
    myrpc::RpcHeader pingHeader;
    pingHeader.set_msg_type(myrpc::PING);

    std::string header_str;
    pingHeader.SerializeToString(&header_str);

    uint32_t header_size = header_str.size();
    uint32_t net_header_size = htonl(header_size);

    std::string send_buf;
    send_buf.append((const char *)&net_header_size, 4);
    send_buf.append(header_str);

    // 发送 Ping
    if (send(fd, send_buf.c_str(), send_buf.size(), 0) == -1) {
        return false;
    }

    // 接收 Pong
    uint32_t recv_size = 0;
    if (recv_exact(fd, (char *)&recv_size, 4) != 4) {
        return false;
    }

    recv_size = ntohl(recv_size);
    std::string recv_buf;
    recv_buf.resize(recv_size);
    if (recv_exact(fd, &recv_buf[0], recv_size) != recv_size) {
        return false;
    }

    myrpc::RpcHeader pongHeader;
    if (pongHeader.ParseFromArray(&recv_buf[0], recv_size)) {
        return pongHeader.msg_type() == myrpc::PONG;
    }
    return false;
}

int ConnectionPool::GetConnection(const std::string &ip, uint16_t port)
{
    std::string key = ip + ":" + std::to_string(port);
    int fd = -1;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        while (pool_.find(key) != pool_.end() && !pool_[key].empty()) {
            auto item = pool_[key].front();
            pool_[key].pop();

            // 借出检查 (Ping on Checkout)
            auto now = std::chrono::steady_clock::now();
            auto idle_duration = std::chrono::duration_cast<std::chrono::seconds>(now - item.last_active_time).count();

            if (idle_duration > 10) { // 空闲超过 10 秒，需要发送 Ping 探测
                if (Ping(item.fd)) {
                    // 心跳存活，更新活跃时间并返回
                    return item.fd;
                } else {
                    // 心跳失败，说明服务端已断开或网络异常，直接销毁该死连接，继续检查下一个
                    close(item.fd);
                    continue;
                }
            } else {
                // 10 秒内刚用过，大概率是健康的，直接返回
                return item.fd;
            }
        }
    }

    // 缓存为空或全是死连接，新建连接
    return CreateNewConnection(ip, port);
}

void ConnectionPool::ReleaseConnection(const std::string &ip, uint16_t port, int fd)
{
    if (fd == -1) {
        return;
    }
    std::string key = ip + ":" + std::to_string(port);

    // 包装为结构体
    ConnectionItem item;
    item.fd = fd;
    // 打上最新活跃时间，这样 GetConnection 取出时，计算空闲时间才是准确的
    item.last_active_time = std::chrono::steady_clock::now();

    std::lock_guard<std::mutex> lock(mutex_);
    pool_[key].push(item); // 放回池中
}

void ConnectionPool::CloseConnection(int fd)
{
    if (fd != -1) {
        close(fd);
    }
}

int ConnectionPool::CreateNewConnection(const std::string &ip, uint16_t port)
{
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd == -1) {
        return -1;
    }

    // 1. 设置收发超时
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
    setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, (const char *)&tv, sizeof(tv));

    // 2. 禁用 TCP 的 Nagle 算法
    // Nagle 算法会把小包凑成大包再发，这会导致 RPC 调用（通常是几十字节的小包）产生 40ms 的延迟！
    // 工业级 RPC 框架（gRPC, Dubbo）都会默认开启 TCP_NODELAY。
    int opt_val = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt_val, sizeof(opt_val));

    // 3. 建立连接
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip.c_str());

    if (connect(client_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        close(client_fd);
        return -1;
    }

    // 直接返回 fd 给业务方使用
    return client_fd;
}
