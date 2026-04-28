#include "rpc_channel.h"
#include "connection_pool.h"
#include "net_utils.h"
#include "rpc_application.h"
#include "rpc_header.pb.h"
#include "zk_client.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <thread>

void MyRpcChannel::CallMethod(const ::google::protobuf::MethodDescriptor *method,
                              ::google::protobuf::RpcController *controller,
                              const ::google::protobuf::Message *request,
                              ::google::protobuf::Message *response,
                              ::google::protobuf::Closure *done)
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
    myrpc::RpcRequestHeader request_header;
    request_header.set_service_name(service_name);
    request_header.set_method_name(method_name);
    request_header.set_args_size(args_str.size());
    request_header.set_msg_type(myrpc::NORMAL_RPC);

    std::string header_str;
    if (!request_header.SerializeToString(&header_str)) {
        controller->SetFailed("Serialize RPC header error!");
        return;
    }

    // 3. 计算各部分长度并组装待发送报文
    // 协议：| 4字节(魔数) | 4字节(总长度) | 4字节(Header长度) | 变长(Header) | 变长(Args) |
    uint32_t magic_num = 0x12345678;
    uint32_t header_size = header_str.size();
    uint32_t args_size = args_str.size();

    // 总长度 = Header长度字段(4字节) + Header内容长度 + Args内容长度
    uint32_t total_size = 4 + header_size + args_size;

    // 转换为网络字节序 (大端)
    uint32_t net_magic_num = htonl(magic_num);
    uint32_t net_total_size = htonl(total_size);
    uint32_t net_header_size = htonl(header_size);

    std::string send_buf;
    send_buf.append((const char *)&net_magic_num, 4);   // 1. 写入魔数
    send_buf.append((const char *)&net_total_size, 4);  // 2. 写入总长度
    send_buf.append((const char *)&net_header_size, 4); // 3. 写入Header长度
    send_buf.append(header_str);                        // 4. 接着写入Header内容
    send_buf.append(args_str);                          // 5. 最后写入Args内容

    // 4. 带自动剔除和负载均衡的重试机制
    int client_fd = -1;
    int max_retries = 3;
    bool rpc_success = false;

    for (int i = 1; i <= max_retries; ++i) {
        // 4.1 每次循环都重新获取地址 (触发轮询负载均衡算法)
        ServiceHost host = QueryZkForHost(service_name, method_name);
        if (host.ip.empty()) {
            LOG(WARNING) << "Retry " << i << ": Query ZK failed for " << service_name;
            if (i == max_retries) {  // 如果是最后一次循环了，才设置最终的失败状态
                controller->SetFailed("RPC Failed: Cannot find service providers in ZK after retries.");
                return; // 彻底退出 RPC 调用
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(i * 10));  // 还没到最后一次，睡一会儿再查，给 ZK 恢复的时间
            continue;
        }

        // 4.2 从连接池获取连接
        client_fd = ConnectionPool::GetInstance().GetConnection(host.ip, host.port);
        if (client_fd == -1) {
            LOG(ERROR) << "GetConnection failed for " << host.ip << ":" << host.port 
                       << " | errno: " << errno << " (" << strerror(errno) << ")";

            if (errno == EMFILE || errno == ENFILE) {
                // 客户端自身资源耗尽，不能错杀服务端，直接终止本次 RPC 重试。
                LOG(FATAL) << "Client FD exhausted! Aborting current RPC request.";
                return; 
            }

            if (errno == ECONNREFUSED || errno == ETIMEDOUT) {
                // 这是明确的服务端宕机或网络不通的信号，执行剔除。
                LOG(WARNING) << "Server dead or unreachable. Removing invalid host: " << host.ip << ":" << host.port;
                std::string zk_path = "/" + service_name + "/" + method_name;
                RemoveInvalidHost(zk_path, host);
            }

            continue; // 重试下一台
        } else if (client_fd == -2) {  // 连接池已满：服务器健康，只是太忙了，不要剔除它。
            LOG(WARNING) << "Host is too busy (connection pool full): " << host.ip << ":" << host.port;
            std::this_thread::sleep_for(std::chrono::milliseconds(i * 10)); // 睡一小会儿，给服务端喘息的时间，也给别的线程归还连接的时间
            continue; // 重试，轮询去尝试下一台机器
        }

        // 5. 连接成功，跳出重试循环，进入数据收发阶段
        // 注意：数据发送失败不自动重试，防止非幂等操作重复执行
        if (send(client_fd, send_buf.c_str(), send_buf.size(), 0) == -1) {  // 发送失败：说明连接已损坏，必须销毁，不能放回连接池。
            ConnectionPool::GetInstance().CloseConnection(host.ip, host.port, client_fd);
            controller->SetFailed("Send RPC request failed!");
            return;
        }

        // 6. 接收服务端发回的响应：[4字节 总长度] + [4字节 Header长度] + [RpcResponseHeader] + [Response Data]
        // 注意：服务端响应为了保持高效，通常不需要加魔数，保持极简即可。
        uint32_t recv_total_size = 0;
        // 先精准读取前 4 个字节，获取后面的总长度
        if (net_utils::recv_exact(client_fd, (char *)&recv_total_size, 4) != 4) {  
            ConnectionPool::GetInstance().CloseConnection(host.ip, host.port, client_fd);
            controller->SetFailed("Recv response total_size timeout or failed!");
            return;
        }

        recv_total_size = ntohl(recv_total_size);       // 转回主机字节序
        if (recv_total_size > 64 * 1024 * 1024) {       // 限制 RPC 响应最大为 64MB
            ConnectionPool::GetInstance().CloseConnection(host.ip, host.port, client_fd);
            controller->SetFailed("Response size too large!");
            return;
        }

        // 把剩下的整包数据全读出来
        std::string recv_buf;
        recv_buf.resize(recv_total_size);
        if (net_utils::recv_exact(client_fd, &recv_buf[0], recv_total_size) == recv_total_size) {

            // 步骤 A：提取 4 字节的 Header 长度
            uint32_t header_size = ntohl(*(uint32_t*)(&recv_buf[0]));

            // 步骤 B：反序列化 RpcResponseHeader (偏移量为 4，长度为 header_size)
            myrpc::RpcResponseHeader resp_header;
            if (!resp_header.ParseFromArray(&recv_buf[4], header_size)) {
                controller->SetFailed("Parse RpcResponseHeader error!");
                ConnectionPool::GetInstance().CloseConnection(host.ip, host.port, client_fd);
                return;
            }

            // 步骤 C：检查框架级错误码
            if (resp_header.errcode() != 0) {
                // 如果 errcode != 0，说明业务逻辑根本没执行（比如服务没找到）
                controller->SetFailed("RPC Framework Error: " + resp_header.errmsg());
                // 注意：这里连接是健康的，只是逻辑报错，千万别销毁连接，放回连接池！
                ConnectionPool::GetInstance().ReleaseConnection(host.ip, host.port, client_fd);
                return; // 直接退出，不需要再解析 Response Data 了
            }

            // 步骤 D：如果框架层成功，再去反序列化真正的业务 Response
            // 偏移量为 4 + header_size，剩余长度为 total_size - 4 - header_size
            uint32_t data_size = recv_total_size - 4 - header_size;
            if (!response->ParseFromArray(&recv_buf[4 + header_size], data_size)) {   
                controller->SetFailed("Parse business response data error!");
                // 业务数据错乱，安全起见销毁连接
                ConnectionPool::GetInstance().CloseConnection(host.ip, host.port, client_fd);
            } else {  
                // RPC 调用彻底成功，将健康的连接放回池中复用！
                rpc_success = true;
                ConnectionPool::GetInstance().ReleaseConnection(host.ip, host.port, client_fd);
            }
        } else {
            ConnectionPool::GetInstance().CloseConnection(host.ip, host.port, client_fd);
            controller->SetFailed("Recv response data timeout or incomplete!");
            return;
        }

        break; // 建连成功并执行完收发，跳出重试循环
    }

    // --- 退出重试循环后的兜底逻辑 ---
    if (!rpc_success) {
        //【分支 A：本地夭折】请求没有成功交发给网络层
        // 拦截隐性耗尽
        if (!controller->Failed()) {  
            controller->SetFailed("RPC Call failed: Exhausted all " +
                                  std::to_string(max_retries) + " retries for " + service_name);
        }
        LOG(ERROR) << "RPC Call aborted. Final error: " << controller->ErrorText();

        // 必须立刻调用 done，通知外层业务 RPC 失败了，别等了。
        if (done != nullptr) {
            done->Run();
        }

        // 拦截执行，绝对不要往下走了
        return; 
    }

    // 【分支 B：成功发送】
    // 如果代码能走到这里，说明 rpc_success == true，意味着 response 已经被成功解析赋值了！
    if (done != nullptr) {
        done->Run();
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
        } else {  // 真正去请求 ZooKeeper，并开启 Watcher
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

        // 1. 创建一个全新的结构体
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
            it->second = new_list;
        } else {
            host_cache[path] = new_list;
        }
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
