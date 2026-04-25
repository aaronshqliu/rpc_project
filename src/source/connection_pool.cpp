#include "connection_pool.h"
#include "rpc_header.pb.h"
#include "net_utils.h"

#include <arpa/inet.h>
#include <netinet/tcp.h>

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
    if (net_utils::recv_exact(fd, (char *)&recv_size, 4) != 4) {
        return false;
    }

    recv_size = ntohl(recv_size);
    std::string recv_buf;
    recv_buf.resize(recv_size);
    if (net_utils::recv_exact(fd, &recv_buf[0], recv_size) != recv_size) {
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

    while (true) {
        int fd = -1;
        std::chrono::steady_clock::time_point last_active;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (pool_.find(key) != pool_.end() && !pool_[key].empty()) {
                auto item = pool_[key].front();
                pool_[key].pop();

                fd = item.fd;
                last_active = item.last_active_time;
            }
        }

        // 如果池子里面没拿到，直接跳出循环去新建
        if (fd == -1) {
            return CreateNewConnection(ip, port);
        }

        // 拿到了，在【无锁】状态下进行空闲判断和 Ping 测试
        auto now = std::chrono::steady_clock::now();
        auto idle_duration = std::chrono::duration_cast<std::chrono::seconds>(now - last_active).count();

        if (idle_duration > 10) {
            if (Ping(fd)) {
                // 心跳存活，返回可用 fd
                return fd;
            } else {
                // 心跳失败，关闭死连接。
                // 此时还在 while(true) 循环内，会自动进入下一次循环，去池子里拿下一个
                close(fd);
                continue;
            }
        } else {
            // 10 秒内刚用过，大概率是健康的，直接返回
            return fd;
        }
    }
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
    pool_[key].emplace(item); // 放回池中
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
