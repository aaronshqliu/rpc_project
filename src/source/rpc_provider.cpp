#include "rpc_provider.h"
#include "rpc_application.h"
#include "rpc_header.pb.h"
#include "zk_client.h"

#include <glog/logging.h>
#include <muduo/net/InetAddress.h>

ConnectionEntry::ConnectionEntry(const muduo::net::TcpConnectionPtr &conn) : weak_conn(conn) {}

ConnectionEntry::~ConnectionEntry()
{
    muduo::net::TcpConnectionPtr conn = weak_conn.lock();
    if (conn) {
        LOG(WARNING) << "Connection idle timeout! Kicking out: " << conn->peerAddress().toIpPort();
        conn->forceClose();
    }
}

// 自定义 Closure 类，接受一个 Lambda 表达式
class RpcClosure : public google::protobuf::Closure {
public:
    explicit RpcClosure(std::function<void()> cb) : cb(cb) {}

    void Run() override
    {
        cb();        // 执行传入的 Lambda 逻辑
        delete this; // 按照 Protobuf 的规矩，Closure 执行完毕后必须自我销毁
    }

private:
    std::function<void()> cb;
};

muduo::net::EventLoop *RpcProvider::GetEventLoop()
{
    return &event_loop;
}

void RpcProvider::NotifyService(google::protobuf::Service *service)
{
    ServiceInfo info;
    info.service = service;

    // 获取服务的描述信息
    const google::protobuf::ServiceDescriptor *service_desc = service->GetDescriptor();

    for (int i = 0; i < service_desc->method_count(); ++i) {
        // 获取具体方法的描述
        const google::protobuf::MethodDescriptor *method_desc = service_desc->method(i);
        info.method_map[method_desc->name()] = method_desc;
    }

    service_map[service_desc->name()] = info;
}

void RpcProvider::Run()
{
    // 读取配置（IP + port）
    m_ip = RpcApplication::GetInstance().GetConfig().GetString("rpc_server_ip");
    m_port = atoi(RpcApplication::GetInstance().GetConfig().GetString("rpc_server_port").c_str());
    idle_timeout_seconds = atoi(RpcApplication::GetInstance().GetConfig().GetString("rpc_idle_timeout_seconds").c_str());
    max_connections = atoi(RpcApplication::GetInstance().GetConfig().GetString("rpc_max_connections").c_str());

    muduo::net::InetAddress address(m_ip, m_port);
    tcp_server = std::make_unique<muduo::net::TcpServer>(&event_loop, address, "RpcProvider");

    // 绑定连接回调和消息回调，分离网络连接业务和消息处理业务
    tcp_server->setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    tcp_server->setMessageCallback(
        std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    tcp_server->setThreadNum(4);
    tcp_server->start();

    // 启动时间轮滴答定时器
    time_wheel.resize(idle_timeout_seconds);
    event_loop.runEvery(1.0, std::bind(&RpcProvider::OnTimerTick, this));

    // 设置重连回调并注册
    ZkClient &zk = RpcApplication::GetInstance().GetZkClient();
    zk.SetRecoveryHandler(std::bind(&RpcProvider::RegisterServiceToZk, this));
    zk.Start();
    RegisterServiceToZk();

    LOG(INFO) << "RpcProvider started on " << m_ip << ":" << m_port;

    event_loop.loop();
}

void RpcProvider::RegisterServiceToZk()
{
    ZkClient &zk = RpcApplication::GetInstance().GetZkClient();
    std::string port_str = std::to_string(m_port);
    std::string host_data = m_ip + ":" + port_str;

    for (auto &[service_name, service_info] : service_map) {
        for (auto &[method_name, method_desc] : service_info.method_map) {
            std::string method_path = "/" + service_name + "/" + method_name;
            std::string instance_path = method_path + "/" + host_data;

            zk.Create(instance_path.c_str(), host_data.c_str(), host_data.size(), ZOO_EPHEMERAL);
            LOG(INFO) << "Register RPC service: " << instance_path << " success!";
        }
    }
}

void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr &conn)
{
    if (conn->connected()) {
        // 使用原子变量进行预检
        int count = ++current_connections;
        if (count > max_connections) {  // 超过最大承载，减回计数并强制关闭
            --current_connections;
            LOG(ERROR) << "Max connections reached (" << max_connections 
                       << "), kicking " << conn->peerAddress().toIpPort();
            conn->forceClose();  // forceClose 会直接发送 RST 包，不经过四次挥手，释放资源最快
            return;
        }

        // 绑定时间轮
        EntryPtr entry = std::make_shared<ConnectionEntry>(conn);  // 创建一个追踪器。此时 entry 引用计数为 1
        time_wheel.back().insert(entry);  // 放入时间轮的末尾（最新的桶）。此时引用计数变为 2

        conn->setContext(std::weak_ptr<ConnectionEntry>(entry));  // 将追踪器的弱引用存入 context，方便 OnMessage 刷新
        LOG(WARNING) << "Client connected: " << conn->peerAddress().toIpPort() 
                   << " [Total: " << count << "]";
    } else {
        // 无论是客户端主动断开，还是服务端由于超时/超限强制关闭，都会走到这里
        --current_connections;

        LOG(WARNING) << "Client disconnected: " << conn->peerAddress().toIpPort() 
                   << " [Total: " << current_connections.load() << "]";
    }
}

void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn,
                            muduo::net::Buffer *buffer,
                            muduo::Timestamp receive_time)
{
    // 循环处理缓冲区，解决粘包问题。约定了协议 [4 字节的 Header 长度][RpcHeader（序列化的二进制）][Method Args（序列化的二进制）]
    while (buffer->readableBytes() >= 4) {
        // 解决粘包：至少要能读出 4 字节的 Header Size
        int32_t header_size = buffer->peekInt32();

        // 如果缓冲区的数据不够完整的一个包，则等待
        if (buffer->readableBytes() < 4 + header_size) {
            break;
        }

        // 数据包完整，开始解包
        std::string rpc_header_str(buffer->peek() + 4, header_size);
        myrpc::RpcHeader rpc_header;
        if (!rpc_header.ParseFromString(rpc_header_str)) {
            LOG(ERROR) << "Failed to parse RpcHeader";
            conn->shutdown();
            return;
        }

        // 拦截心跳包
        if (rpc_header.msg_type() == myrpc::PING) {
            // 构造 Pong 响应头
            myrpc::RpcHeader pongHeader;
            pongHeader.set_msg_type(myrpc::PONG);

            std::string pong_str;
            pongHeader.SerializeToString(&pong_str);

            uint32_t net_header_size = htonl(pong_str.size());

            std::string send_buf;
            send_buf.append((const char *)&net_header_size, 4);
            send_buf.append(pong_str);
            conn->send(send_buf);

            buffer->retrieve(4 + header_size);
            continue;
        }

        // 如果是 NORMAL_RPC，则继续走正常的业务分发
        // 解析业务请求参数 (Args)
        uint32_t args_size = rpc_header.args_size();
        if (buffer->readableBytes() < 4 + header_size + args_size) {
            break; // 数据体还不完整
        }

        // 读出参数
        std::string args_str(buffer->peek() + 4 + header_size, args_size);
        buffer->retrieve(4 + header_size + args_size); // 移动缓冲区指针

        auto service_map_it = service_map.find(rpc_header.service_name());
        if (service_map_it == service_map.end()) {
            LOG(ERROR) << rpc_header.service_name() << " is not exist!";
            conn->shutdown();
            return;
        }

        auto method_map_it = service_map_it->second.method_map.find(rpc_header.method_name());
        if (method_map_it == service_map_it->second.method_map.end()) {
            LOG(ERROR) << rpc_header.method_name() << " is not exist!";
            conn->shutdown();
            return;
        }

        auto service = service_map_it->second.service;
        auto method = method_map_it->second;

        // 生成请求对象
        auto request = service->GetRequestPrototype(method).New();
        request->ParseFromString(args_str);

        // 生成响应对象
        auto response = service->GetResponsePrototype(method).New();
        // 6. 将请求分发给对应的服务对象进行处理
        RpcClosure *done = new RpcClosure([this, conn, request, response]() {
            SendResponse(conn, request, response);
        });

        // 调用业务逻辑：Register、Login、MakeOrder等，这里可以放到线程池中完成
        service->CallMethod(method, nullptr, request, response, done);
    }
}

// 将 RPC 响应对象序列化并发送回客户端
void RpcProvider::SendResponse(const muduo::net::TcpConnectionPtr &conn,
                               google::protobuf::Message *request,
                               google::protobuf::Message *response)
{
    std::string response_str;

    // 将 response 对象序列化为字符串
    if (response->SerializeToString(&response_str)) {
        // 构造响应：[4 bytes Length] + [Response Data]
        muduo::net::Buffer send_buffer;
        send_buffer.appendInt32(response_str.size()); // 先发送响应长度
        send_buffer.append(response_str);             // 再发送响应内容
        conn->send(&send_buffer);                     // 序列化成功，通过 muduo 网络库发送给客户端
    } else {
        LOG(ERROR) << "Serialize response error!";
    }

    delete request;
    delete response;
}

void RpcProvider::OnTimerTick()
{
    time_wheel.emplace_back(Bucket());  // 原地构造一个全新的空桶，放入队尾
    time_wheel.pop_front();  // 弹出最老的桶，利用智能指针引用计数自动清理死连接

    static int tick_count = 0;
    if (++tick_count % 10 == 0) { // 每 10 秒打印一次
        LOG(INFO) << "=== [Server Metrics] Active Connections: " 
                  << current_connections.load() << " / " << max_connections
                  << " ===";
    }
}

RpcProvider::~RpcProvider()
{
    for (auto &i : service_map) {
        if (i.second.service != nullptr) {
            delete i.second.service;
            i.second.service = nullptr;
        }
    }
}
