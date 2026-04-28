#ifndef RPC_PROVIDER_H
#define RPC_PROVIDER_H

#include <google/protobuf/descriptor.h>
#include <google/protobuf/service.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <unordered_map>

// RpcProvider类，负责发布服务和启动RPC服务器
class RpcProvider {
public:
    RpcProvider() = default;
    ~RpcProvider() = default;
    void NotifyService(google::protobuf::Service *service);
    void Run();
    muduo::net::EventLoop* GetEventLoop();

private:
    void OnConnection(const muduo::net::TcpConnectionPtr &conn);

    void OnMessage(const muduo::net::TcpConnectionPtr &conn,
                   muduo::net::Buffer *buffer,
                   muduo::Timestamp receive_time);

    // 将 service_map 中的所有服务重新发布到 ZK
    void RegisterServiceToZk();

    // ServiceInfo结构体用来存储某一个具体服务的所有信息。在 Protobuf 的概念里，一个 Service（服务）往往包含多个 Method（方法）。
    struct ServiceInfo {
        // 这个指针就是指向 new 出来的那个具体的业务对象。当框架需要执行业务逻辑时，最终就是通过这个指针去调用的。
        std::unique_ptr<google::protobuf::Service> service; 
        // Key 是方法名（比如 "Login"），Value 是指向该方法描述符 (MethodDescriptor) 的指针。
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> method_map;
    };

    // 整个 RPC Provider 的总路由表。Key 是服务名（比如 "UserServiceRpc"），Value 是对应的服务档案，也就是上面的 ServiceInfo。
    std::unordered_map<std::string, ServiceInfo> service_map;

    muduo::net::EventLoop event_loop;
    std::unique_ptr<muduo::net::TcpServer> tcp_server;

    std::string ip;   // 缓存服务器IP
    uint16_t port;    // 缓存服务器端口
};

#endif // RPC_PROVIDER_H
