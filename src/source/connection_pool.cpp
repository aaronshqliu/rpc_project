#include "connection_pool.h"
#include "net_utils.h"
#include "rpc_application.h"
#include "rpc_header.pb.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <netinet/tcp.h>

ConnectionPool::~ConnectionPool()
{
    std::lock_guard<std::mutex> lock(mutex);

    // 清理所有待在池子里的空闲连接
    for (auto &pair : pool) {
        while (!pair.second.empty()) {
            ConnectionItem item = pair.second.front();
            pair.second.pop();
            if (item.fd != -1) {
                close(item.fd);
            }
        }
    }
}

bool ConnectionPool::Ping(int fd)
{
    // 1. 备份原有的超时设置
    struct timeval old_tv;
    socklen_t len = sizeof(old_tv);
    if (getsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char*)&old_tv, &len) < 0) {
        return false;
    }

    // 定义一个守卫，在作用域结束时还原现场
    struct TimeoutGuard {
        int fd_;
        struct timeval old_tv_;
        ~TimeoutGuard() {
            setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, (const char*)&old_tv_, sizeof(old_tv_));
        }
    } guard{fd, old_tv};

    /**
     * 2. 设置短超时 (500ms) 执行探测
     * 如果没有设置这 500ms 会怎样？
     * 假设此时服务端的机器电源被拔了，或者中间的防火墙把连接切断了（并且没发 RST 包）。
     * 当执行到 recv(fd, buffer) 时，因为 TCP 连接表面上还在，但永远不会有数据回来了。
     * Linux 底层的 recv 是一个阻塞调用，默认情况下，它会一直死等，可能会等上十几分钟甚至几个小时（直到系统底层的 TCP Keepalive 超时）。
     * 这就意味着，你这个业务线程永远卡死在这一行代码上了。如果有 600 个并发请求，瞬间你的 600 个线程就全卡死在 Ping 上了，你的系统直接瘫痪。
     * 
     * 设置了 500ms 会怎样？（快速失败 Fail-fast）
     * 设置了 SO_RCVTIMEO = 500000 微秒（即 500 毫秒）后，recv 最多只会等 500 毫秒。
     * 如果 500 毫秒内服务端回了 Pong，说明连接极其健康，完美。
     * 如果 500 毫秒没收到，recv 会立刻返回 -1，并把 errno 设置为 EAGAIN 或 EWOULDBLOCK。
     * 此时 Ping 函数就能迅速返回 false，通知连接池这个 fd 已经死了，赶紧 Close 掉去建新连接。
     */
    struct timeval tv_temp;
    tv_temp.tv_sec = 0;
    tv_temp.tv_usec = 500000;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv_temp, sizeof(tv_temp)) < 0) {
        return false;
    }

    myrpc::RpcHeader pingHeader;
    pingHeader.set_msg_type(myrpc::PING);

    std::string send_buf;
    uint32_t header_size = pingHeader.ByteSizeLong();
    uint32_t net_header_size = htonl(header_size);
    send_buf.append((const char *)&net_header_size, 4);
    pingHeader.AppendToString(&send_buf);

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
            std::lock_guard<std::mutex> lock(mutex);
            if (pool.find(key) != pool.end() && !pool[key].empty()) {
                auto item = pool[key].front();
                pool[key].pop();

                fd = item.fd;
                last_active = item.last_active_time;
            }
        }

        // 如果池子里面没拿到，直接跳出循环去新建
        if (fd == -1) {
            return CreateNewConnection(ip, port);
        }

        // 拿到了，在【无锁】状态下进行空闲判断和 Ping 测试。
        auto now = std::chrono::steady_clock::now();
        auto idle_duration = std::chrono::duration_cast<std::chrono::seconds>(now - last_active).count();

        if (idle_duration > 10) { // 惰性心跳
            if (Ping(fd)) {
                // 心跳存活，返回可用 fd
                return fd;
            } else {
                // 心跳失败，关闭死连接。
                // 此时还在 while(true) 循环内，会自动进入下一次循环，去池子里拿下一个
                CloseConnection(ip, port, fd);
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
    std::lock_guard<std::mutex> lock(mutex);
    pool[key].emplace(fd, std::chrono::steady_clock::now());
}

void ConnectionPool::CloseConnection(const std::string &ip, uint16_t port, int fd)
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
