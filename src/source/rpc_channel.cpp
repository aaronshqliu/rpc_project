#include "rpc_channel.h"
#include "connection_pool.h"
#include "net_utils.h"
#include "rpc_application.h"
#include "rpc_header.pb.h"
#include "zk_client.h"

#include <arpa/inet.h>
#include <glog/logging.h>

void MyRpcChannel::CallMethod(const ::google::protobuf::MethodDescriptor *method,
    ::google::protobuf::RpcController *controller, const ::google::protobuf::Message *request,
    ::google::protobuf::Message *response, ::google::protobuf::Closure *done)
{
    std::string service_name = method->service()->name();
    std::string method_name = method->name();

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

        // 4.2 从连接池获取连接
        client_fd = ConnectionPool::GetInstance().GetConnection(host.ip, host.port);
        if (client_fd == -1) {
            // 建连失败：服务器网络不通或宕机，必须剔除坏节点。
            LOG(ERROR) << "Connect failed, removing invalid host: " << host.ip << ":" << host.port;
            std::string zk_path = "/" + service_name + "/" + method_name;
            RemoveInvalidHost(zk_path, host);
            continue; // 重试下一台
        } else if (client_fd == -2) {
            // 连接池已满：服务器健康，只是太忙了，不要剔除它。
            LOG(WARNING) << "Host is too busy (connection pool full): " << host.ip << ":" << host.port;
            continue; // 直接重试，轮询去尝试下一台机器
        }

        // 5. 连接成功，跳出重试循环，进入数据收发阶段
        // 注意：数据发送失败不自动重试，防止非幂等操作重复执行
        if (send(client_fd, send_buf.c_str(), send_buf.size(), 0) == -1) {
            // 发送失败：说明连接已损坏，必须销毁，不能放回连接池。
            ConnectionPool::GetInstance().CloseConnection(host.ip, host.port, client_fd);
            controller->SetFailed("Send RPC request failed!");
            return;
        }

        // 服务端发回的格式是：[4字节 Length] + [Response Data]
        uint32_t recv_size = 0;
        // 先精准读取前 4 个字节，获取响应总长度
        if (net_utils::recv_exact(client_fd, (char *)&recv_size, 4) != 4) {
            ConnectionPool::GetInstance().CloseConnection(host.ip, host.port, client_fd);
            controller->SetFailed("Recv response header timeout or failed!");
            return;
        }

        recv_size = ntohl(recv_size);       // 转回主机字节序
        if (recv_size > 64 * 1024 * 1024) { // 限制 RPC 响应最大为 64MB
            ConnectionPool::GetInstance().CloseConnection(host.ip, host.port, client_fd);
            controller->SetFailed("Parse response header error: Response size too large!");
            return;
        }

        std::string recv_buf;
        recv_buf.resize(recv_size);
        if (net_utils::recv_exact(client_fd, &recv_buf[0], recv_size) == recv_size) {
            if (!response->ParseFromArray(&recv_buf[0], recv_size)) {
                controller->SetFailed("Parse response error!");
                ConnectionPool::GetInstance().CloseConnection(host.ip, host.port, client_fd); // 数据包错乱，安全起见销毁连接
            } else {
                rpc_success = true;
                // RPC 调用彻底成功，将健康的连接放回池中复用！
                ConnectionPool::GetInstance().ReleaseConnection(host.ip, host.port, client_fd);
            }
        } else {
            ConnectionPool::GetInstance().CloseConnection(host.ip, host.port, client_fd);
            controller->SetFailed("Recv response data timeout or incomplete!");
            return;
        }

        break; // 建连成功并执行完收发，跳出重试循环
    }

    // 检查重试是否耗尽
    if (!rpc_success && !controller->Failed()) {
        controller->SetFailed("RPC Call failed: Exhausted retries for " + service_name);
    }
}

MyRpcChannel::ServiceHost MyRpcChannel::QueryZkForHost(const std::string &service_name, const std::string &method_name)
{
    std::string path = "/" + service_name + "/" + method_name;
    // 1. 尝试从缓存获取 (加读锁，允许多线程并发读)
    {
        std::shared_lock<std::shared_mutex> read_lock(cache_mutex);
        auto it = host_cache.find(path);
        if (it != host_cache.end() && it->second && !it->second->hosts.empty()) {
            return GetHostByRoundRobin(it->second);
        }
    }

    // 2. 缓存未命中，去 ZK 查询
    std::shared_ptr<ServiceNodeList> node_list = nullptr;
    {
        std::unique_lock<std::shared_mutex> write_lock(cache_mutex);
        // 防止多个线程同时发现缓存为空，阻塞在锁外，拿到锁后重复查询ZK
        auto it = host_cache.find(path);
        if (it != host_cache.end() && it->second && !it->second->hosts.empty()) {
            node_list = it->second;
        } else {
            // 真正去请求 ZooKeeper，并开启 Watcher
            ZkClient &zk = RpcApplication::GetInstance().GetZkClient();
            std::vector<std::string> available_hosts;

            if (!zk.GetChildren(path.c_str(), available_hosts, true)) {
                // 情况 A：ZK 挂了。因为此时缓存也为空，我们实在找不到节点了
                LOG(ERROR) << "ZK is DOWN and no local cache for: " << path;
                return {"", 0};
            }

            if (available_hosts.empty()) {
                // 情况 B：ZK 正常，但该服务确实一个 Provider 都没有
                LOG(ERROR) << "Service exist in ZK but has NO instances: " << path;
                return {"", 0};
            }

            // 情况 C：查询成功且有节点，解析并更新缓存
            std::vector<ServiceHost> parsed_hosts = ParseHostStrings(available_hosts);
            if (!parsed_hosts.empty()) {
                node_list = std::make_shared<ServiceNodeList>();
                node_list->hosts = std::move(parsed_hosts);
                host_cache[path] = node_list;

                LOG(INFO) << "--------------------------------------------------";
                LOG(INFO) << "[Service Discovery] Path: " << path;
                LOG(INFO) << "[Service Discovery] Status: ONLINE";
                LOG(INFO) << "[Service Discovery] Instances Found: " << node_list->hosts.size();
                LOG(INFO) << "--------------------------------------------------";

                // 注册监听
                zk.SubscribeWatcher(path, [this](int type, const std::string &changed_path) {
                    this->BackgroundRefreshCache(changed_path);
                });
            }
        }
    }

    // 3. 客户端侧负载均衡 (轮询)
    if (node_list && !node_list->hosts.empty()) {
        return GetHostByRoundRobin(node_list);
    }

    return {"", 0};
}

void MyRpcChannel::PreFetchService(const std::string &service_name, const std::string &method_name)
{
    QueryZkForHost(service_name, method_name);
}

MyRpcChannel::ServiceHost MyRpcChannel::GetHostByRoundRobin(std::shared_ptr<ServiceNodeList> list)
{
    // std::memory_order_relaxed: 只关心原子自增，不关心内存屏障同步，这样性能最高
    uint32_t idx = list->next_idx.fetch_add(1, std::memory_order_relaxed);
    return list->hosts[idx % list->hosts.size()];
}

void MyRpcChannel::RemoveInvalidHost(const std::string &path, const ServiceHost &invalid_host)
{
    // 获取独占写锁，保护 host_cache 这个 Map 结构
    std::unique_lock<std::shared_mutex> write_lock(cache_mutex);
    auto it = host_cache.find(path);
    // 确保节点存在且指针不为空
    if (it != host_cache.end() && it->second) {
        auto old_list = it->second;

        // 【写时复制核心】：1. 创建一个全新的结构体
        auto new_list = std::make_shared<ServiceNodeList>();

        // 2. 将健康的节点全部拷贝过去，跳过那个失效的节点
        for (const auto &h : old_list->hosts) {
            if (h != invalid_host) {
                new_list->hosts.emplace_back(h);
            }
        }

        // 3. 检查踢掉这个节点后，列表是不是空了
        if (new_list->hosts.empty()) {
            host_cache.erase(it);
            LOG(INFO) << "All instances are down. Removed service path: " << path << " from cache.";
        } else {
            // 4. 继承旧的轮询计数器（保证其他机器的轮询节奏不断）
            new_list->next_idx.store(old_list->next_idx.load(std::memory_order_relaxed), std::memory_order_relaxed);

            // 5. 原子替换智能指针！旧指针会在没线程用它时自动销毁
            it->second = new_list;
            LOG(INFO) << "Actively removed invalid host: " << invalid_host.ToString() << " from cache.";
        }
    }
}

void MyRpcChannel::BackgroundRefreshCache(const std::string &path)
{
    ZkClient &zk = RpcApplication::GetInstance().GetZkClient();
    std::vector<std::string> new_host_strs;

    // 1. 重新向 ZK 拉取最新的节点列表（注意 watch 参数依然为 true，为了监听下一次变化）
    if (!zk.GetChildren(path.c_str(), new_host_strs, true)) {
        // 如果 ZK 查询报错（网络问题等），不清空缓存，而是保留旧缓存继续使用，这叫“容错降级”。
        LOG(WARNING) << "ZK update failed, keeping STALE cache for path: " << path;
        return;
    }

    // 2. 解析新的节点数据
    std::vector<ServiceHost> parsed_hosts = ParseHostStrings(new_host_strs);

    // 3. 如果全宕机了，只能清空，根本不需要分配新内存，直接进锁清理即可
    if (parsed_hosts.empty()) {
        std::unique_lock<std::shared_mutex> write_lock(cache_mutex);
        if (auto it = host_cache.find(path); it != host_cache.end()) {
            host_cache.erase(it);
        }
        LOG(WARNING) << "All nodes down for path: " << path << ". Cache cleared.";
        return;
    }

    // 4. 写时复制，创建新列表，平滑替换旧列表
    auto new_list = std::make_shared<ServiceNodeList>();
    new_list->hosts = std::move(parsed_hosts);
    {
        std::unique_lock<std::shared_mutex> write_lock(cache_mutex);
        // 继承旧的轮询计数器，保证负载均衡不断档
        if (auto it = host_cache.find(path); it != host_cache.end() && it->second) {
            new_list->next_idx.store(it->second->next_idx.load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
        host_cache[path] = new_list;
    }
    LOG(INFO) << "Background refresh success for path: " << path << ", new node count: " << new_list->hosts.size();
}

std::vector<MyRpcChannel::ServiceHost> MyRpcChannel::ParseHostStrings(const std::vector<std::string> &host_strs)
{
    std::vector<ServiceHost> parsed_hosts;
    for (const auto &host_str : host_strs) {
        size_t idx = host_str.find(":");
        if (idx != std::string::npos) {
            ServiceHost host;
            host.ip = host_str.substr(0, idx);
            host.port = static_cast<uint16_t>(std::stoi(host_str.substr(idx + 1)));
            parsed_hosts.emplace_back(host);
        } else {
            LOG(ERROR) << "Invalid host data format from ZK: " << host_str;
        }
    }
    return parsed_hosts;
}
