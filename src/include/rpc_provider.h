#ifndef RPC_PROVIDER_H
#define RPC_PROVIDER_H

#include <deque>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/service.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <unordered_map>
#include <unordered_set>

// 连接包装器
struct ConnectionEntry {
    explicit ConnectionEntry(const muduo::net::TcpConnectionPtr &conn);
    ~ConnectionEntry();

    std::weak_ptr<muduo::net::TcpConnection> weak_conn;
};

using EntryPtr = std::shared_ptr<ConnectionEntry>;
using Bucket = std::unordered_set<EntryPtr>;

// RpcProvider类，负责发布服务和启动RPC服务器
class RpcProvider {
public:
    RpcProvider() = default;
    ~RpcProvider();
    void NotifyService(google::protobuf::Service *service);
    void Run();
    muduo::net::EventLoop* GetEventLoop();

private:
    void OnConnection(const muduo::net::TcpConnectionPtr &conn);

    void OnMessage(const muduo::net::TcpConnectionPtr &conn,
                   muduo::net::Buffer *buffer,
                   muduo::Timestamp receive_time);

    void SendResponse(const muduo::net::TcpConnectionPtr &conn,
                      google::protobuf::Message *request,
                      google::protobuf::Message *response); // 服务分发后的回调

    // 将 service_map 中的所有服务重新发布到 ZK
    void RegisterServiceToZk();

    // 时间轮滴答函数
    void OnTimerTick();

    muduo::net::EventLoop event_loop;
    std::unique_ptr<muduo::net::TcpServer> tcp_server;

    // ServiceInfo结构体用来存储某一个具体服务的所有信息。在 Protobuf 的概念里，一个 Service（服务）往往包含多个 Method（方法）。
    struct ServiceInfo {
        // 这个指针就是指向 new 出来的那个具体的业务对象。当框架需要执行业务逻辑时，最终就是通过这个指针去调用的。
        google::protobuf::Service *service; 
        // Key 是方法名（比如 "Login"），Value 是指向该方法描述符 (MethodDescriptor) 的指针。
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> method_map;
    };

    // 整个 RPC Provider 的总路由表。Key 是服务名（比如 "UserServiceRpc"），Value 是对应的服务档案，也就是上面的 ServiceInfo。
    std::unordered_map<std::string, ServiceInfo> service_map;

    std::string m_ip;   // 缓存服务器IP
    uint16_t m_port;    // 缓存服务器端口

    std::deque<Bucket> time_wheel;  // 时间轮：用 deque 实现滑动窗口
    int idle_timeout_seconds;       // 超时时间配置，比如 60 秒

    std::atomic<int> current_connections {0}; // 当前真实存活的连接数
    int max_connections;             // 最大允许连接数
};

#endif // RPC_PROVIDER_H
