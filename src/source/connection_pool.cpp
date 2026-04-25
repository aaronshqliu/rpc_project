#include "connection_pool.h"
#include "net_utils.h"
#include "rpc_application.h"
#include "rpc_header.pb.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <netinet/tcp.h>

ConnectionPool::ConnectionPool()
{
    max_idle_per_host = stoi(RpcApplication::GetInstance().GetConfig().GetString("max_idle_per_host"));
    max_active_per_host = stoi(RpcApplication::GetInstance().GetConfig().GetString("max_active_per_host"));

    LOG(INFO) << "ConnectionPool initialized. Max Idle: " << max_idle_per_host
              << ", Max Active: " << max_active_per_host;
}

ConnectionPool::~ConnectionPool()
{
    std::lock_guard<std::mutex> lock(mutex);

    // 1. 连接泄露检测：抓出没有归还的连接
    for (const auto &[host, total_active] : active_counts) {
        int idle_count = 0;
        if (pool.find(host) != pool.end()) {
            idle_count = pool[host].size();
        }

        int leaked_count = total_active - idle_count;
        if (leaked_count > 0) {
            LOG(ERROR) << "FATAL: True Connection LEAK detected! Host: " << host 
                       << " leaked " << leaked_count << " connections that were never returned!";
        }
    }

    // 2. 清理所有待在池子里的空闲连接
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

    // 2. 设置短超时 (500ms) 执行探测
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
            } else {
                // 池子里没有，准备新建前检查活跃连接总数
                if (active_counts[key] >= max_active_per_host) {
                    LOG(ERROR) << "Connection limit reached (" << max_active_per_host << ") for host: " << key;
                    return -2; // 返回 -2 表示超载
                }
                // 没满，先“占领”一个活跃名额，防止并发时被别的线程抢占超售
                active_counts[key]++;
            }
        }

        // 如果池子里面没拿到，直接跳出循环去新建
        if (fd == -1) {
            fd = CreateNewConnection(ip, port);
            if (fd == -1) {
                // 如果建连失败了（比如网络不通），必须把刚才占的名额退回来
                std::lock_guard<std::mutex> lock(mutex);
                active_counts[key]--;
            }
            return fd; // 无论成功失败，直接返回
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
    if (pool[key].size() >= max_idle_per_host) { // 检查是否达到最大空闲上限
        // 超过空闲上限，不要放回队列，直接物理关闭
        close(fd);
        // 回收活跃计数配额
        active_counts[key]--;
    } else {
        // 没满，放回池子，更新最后活跃时间
        pool[key].emplace(fd, std::chrono::steady_clock::now());
    }
}

void ConnectionPool::CloseConnection(const std::string &ip, uint16_t port, int fd)
{
    std::string key = ip + ":" + std::to_string(port);
    close(fd); // 物理关闭 socket

    // 同步扣减活跃名额
    std::lock_guard<std::mutex> lock(mutex);
    if (active_counts[key] > 0) {
        active_counts[key]--;
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
