#include "rpc_channel.h"
#include "connection_pool.h"
#include "rpc_application.h"
#include "rpc_header.pb.h"
#include "zk_client.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <sys/socket.h>
#include <unistd.h>

MyRpcChannel::MyRpcChannel()
{
    ZkClient &zk = RpcApplication::GetInstance().GetZkClient();

    zk.SetNotifyHandler([this](int type, const std::string &path) {
        // ZOO_CHILD_EVENT: 监听到服务提供者上线/下线 (子节点发生增减)
        // ZOO_DELETED_EVENT: 监听到整个方法节点都被删除了 (极端情况)
        if (type == ZOO_CHILD_EVENT || type == ZOO_DELETED_EVENT) {
            std::unique_lock<std::shared_mutex> lock(this->cache_mutex);

            if (this->host_cache.erase(path)) {
                LOG(INFO) << "Watcher triggered: Children changed, Cache invalidated for path: " << path.c_str();
            }
        }
    });
}

void MyRpcChannel::CallMethod(const ::google::protobuf::MethodDescriptor *method,
    ::google::protobuf::RpcController *controller, const ::google::protobuf::Message *request,
    ::google::protobuf::Message *response, ::google::protobuf::Closure *done)
{
    std::string service_name = method->service()->name();
    std::string method_name = method->name();
    std::string zk_path = "/" + service_name + "/" + method_name;

    // 1. 序列化请求参数
    std::string args_str;
    if (!request->SerializeToString(&args_str)) {
        controller->SetFailed("Serialize request error!");
        return;
    }

    // 2. 构造并序列化 RPC Header
    myrpc::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(args_str.size());
    rpcHeader.set_msg_type(myrpc::NORMAL_RPC);

    std::string header_str;
    if (!rpcHeader.SerializeToString(&header_str)) {
        controller->SetFailed("Serialize RPC header error!");
        return;
    }

    // 3. 计算各部分长度并组装待发送报文
    uint32_t header_size = header_str.size();
    uint32_t net_header_size = htonl(header_size);

    std::string send_buf;
    send_buf.append((const char *)&net_header_size, 4); // 写入Header长度
    send_buf.append(header_str);                        // 接着写入Header内容
    send_buf.append(args_str);                          // 最后写入Args内容

    // 4. 带自动剔除和负载均衡的重试机制
    int client_fd = -1;
    int max_retries = 3;
    bool rpc_success = false;

    for (int i = 0; i < max_retries; ++i) {
        // 4.1 每次循环都重新获取地址 (触发轮询负载均衡算法)
        ServiceHost host = QueryZkForHost(service_name, method_name);
        if (host.ip.empty()) {
            controller->SetFailed("Query service from Zookeeper failed! Service: " + service_name);
            continue;
        }

        std::string current_host_str = host.ip + ":" + std::to_string(host.port);

        // 4.2 从连接池获取连接
        client_fd = ConnectionPool::GetInstance().GetConnection(host.ip, host.port);
        if (client_fd == -1) {
            // 连接失败的处理逻辑：剔除坏节点
            {
                std::unique_lock<std::shared_mutex> write_lock(cache_mutex);
                auto it = host_cache.find(zk_path);
                if (it != host_cache.end()) {
                    auto &hosts = it->second;
                    hosts.erase(std::remove(hosts.begin(), hosts.end(), current_host_str), hosts.end());
                    if (hosts.empty())
                        host_cache.erase(it);
                }
            }
            continue; // 重试
        }

        // 5. 连接成功，跳出重试循环，进入数据收发阶段
        // 注意：数据发送失败不自动重试，防止非幂等操作重复执行
        if (send(client_fd, send_buf.c_str(), send_buf.size(), 0) == -1) {
            // 发送失败：说明连接已损坏，必须销毁，不能放回连接池！
            ConnectionPool::GetInstance().CloseConnection(client_fd);
            controller->SetFailed("Send RPC request failed!");
            return;
        }

        // 服务端发回的格式是：[4字节 Length] + [Response Data]
        uint32_t recv_size = 0;
        // 先精准读取前 4 个字节，获取响应总长度
        if (recv_exact(client_fd, (char *)&recv_size, 4) != 4) {
            ConnectionPool::GetInstance().CloseConnection(client_fd);
            controller->SetFailed("Recv response header timeout or failed!");
            return;
        }

        recv_size = ntohl(recv_size); // 转回主机字节序
        std::string recv_buf;
        recv_buf.resize(recv_size);

        if (recv_exact(client_fd, &recv_buf[0], recv_size) == recv_size) {
            if (!response->ParseFromArray(&recv_buf[0], recv_size)) {
                controller->SetFailed("Parse response error!");
                ConnectionPool::GetInstance().CloseConnection(client_fd); // 数据包错乱，安全起见销毁连接
            } else {
                rpc_success = true;
                // RPC 调用彻底成功，将健康的连接放回池中复用！
                ConnectionPool::GetInstance().ReleaseConnection(host.ip, host.port, client_fd);
            }
        } else {
            ConnectionPool::GetInstance().CloseConnection(client_fd);
            controller->SetFailed("Recv response data timeout or incomplete!");
        }

        break; // 建连成功并执行完收发，跳出重试循环

        if (!rpc_success && !controller->Failed()) {
            controller->SetFailed("RPC Call failed: Exhausted retries for " + service_name);
        }
    }

    // 检查重试是否耗尽
    if (!rpc_success && !controller->Failed()) {
        controller->SetFailed("RPC Call failed: Exhausted retries for " + service_name);
    }
}

ssize_t MyRpcChannel::recv_exact(int fd, char *buffer, size_t length)
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

MyRpcChannel::ServiceHost MyRpcChannel::QueryZkForHost(const std::string &service_name, const std::string &method_name)
{
    std::string path = "/" + service_name + "/" + method_name;
    std::vector<std::string> available_hosts;

    // 1. 尝试从缓存获取 (加读锁，允许多线程并发读)
    {
        std::shared_lock<std::shared_mutex> read_lock(cache_mutex);
        auto it = host_cache.find(path);
        if (it != host_cache.end() && !it->second.empty()) {
            available_hosts = it->second;
        }
    }

    // 2. 缓存未命中，去 ZK 查询
    if (available_hosts.empty()) {
        std::unique_lock<std::shared_mutex> write_lock(cache_mutex);

        // 防止多个线程同时发现缓存为空，阻塞在锁外，拿到锁后重复查询ZK
        auto it = host_cache.find(path);
        if (it != host_cache.end() && !it->second.empty()) {
            available_hosts = it->second;
        } else {
            // 真正去请求 ZooKeeper，并开启 Watcher
            ZkClient &zk = RpcApplication::GetInstance().GetZkClient();
            available_hosts = zk.GetChildren(path.c_str(), true);

            if (!available_hosts.empty()) {
                host_cache[path] = available_hosts; // 更新缓存
                LOG(INFO) << "Cache updated for path: " << path << ", found " << available_hosts.size()
                          << " instances.";
            }
        }
    }

    // 依然没有可用节点，说明服务全部宕机或未启动
    if (available_hosts.empty()) {
        LOG(ERROR) << "No available instances for service: " << service_name << " method: " << method_name;
        return {"", 0};
    }

    // 3. 客户端侧负载均衡 (Round-Robin 轮询)
    // 使用原子自增，保证多线程并发下的公平轮询
    uint32_t current_idx = request_count.fetch_add(1);
    std::string target_host_str = available_hosts[current_idx % available_hosts.size()];

    // 4. 解析并返回最终选定的 IP 和 Port
    size_t colon_idx = target_host_str.find(":");
    if (colon_idx == std::string::npos) {
        LOG(ERROR) << "Invalid host data format from ZK: " << target_host_str;
        return {"", 0};
    }

    ServiceHost host;
    host.ip = target_host_str.substr(0, colon_idx);
    host.port = static_cast<uint16_t>(std::stoi(target_host_str.substr(colon_idx + 1)));

    // LOG(INFO) << "RPC call load-balanced to -> " << host.ip << ":" << host.port;

    return host;
}

void MyRpcChannel::RemoveInvalidHost(const std::string &path, const std::string &invalid_host_str)
{
    std::unique_lock<std::shared_mutex> write_lock(cache_mutex);
    auto it = host_cache.find(path);
    if (it != host_cache.end()) {
        auto &hosts = it->second;
        hosts.erase(std::remove(hosts.begin(), hosts.end(), invalid_host_str), hosts.end());
        LOG(INFO) << "Actively removed invalid host: " << invalid_host_str << " from cache.";
    }
}
