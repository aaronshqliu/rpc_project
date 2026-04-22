#ifndef RPC_PROVIDER_H
#define RPC_PROVIDER_H

#include <google/protobuf/descriptor.h>
#include <google/protobuf/service.h>
#include <muduo/net/EventLoop.h>
#include <unordered_map>

// RpcProvider类，负责发布服务和启动RPC服务器
class RpcProvider {
public:
    RpcProvider() = default;
    ~RpcProvider();
    void NotifyService(google::protobuf::Service *service);
    void Run();

private:
    void OnConnection(const muduo::net::TcpConnectionPtr &conn);
    void OnMessage(const muduo::net::TcpConnectionPtr &conn,
                   muduo::net::Buffer *buffer,
                   muduo::Timestamp receive_time);
    void SendResponse(const muduo::net::TcpConnectionPtr &conn,
                      google::protobuf::Message *request,
                      google::protobuf::Message *response); // 服务分发后的回调

    struct ServiceInfo {
        google::protobuf::Service *service;                                                     // 服务对象
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> method_map; // 方法描述符映射
    };
    muduo::net::EventLoop event_loop;
    std::unordered_map<std::string, ServiceInfo> service_map; // 存储注册成功的服务及其详细信息
};

#endif // RPC_PROVIDER_H
