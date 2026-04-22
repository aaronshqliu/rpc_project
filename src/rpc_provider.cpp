#include "rpc_provider.h"
#include "rpc_application.h"
#include "rpc_header.pb.h"
#include "zk_client.h"

#include <glog/logging.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpServer.h>

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
    std::string ip = RpcApplication::GetInstance().GetConfig().GetString("rpc_server_ip");
    uint16_t port = atoi(RpcApplication::GetInstance().GetConfig().GetString("rpc_server_port").c_str());

    muduo::net::InetAddress address(ip, port);
    muduo::net::TcpServer server(&event_loop, address, "RpcProvider");

    server.setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    server.setMessageCallback(
        std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    server.setThreadNum(4);

    // 将注册的服务发布到 ZooKeeper 上，供客户端发现
    ZkClient &zk = RpcApplication::GetInstance().GetZkClient();
    for (auto &sp : service_map) {
        for (auto &mp : sp.second.method_map) {
            // /Service_Name/Method_Name (临时节点)
            std::string method_path = "/" + sp.first + "/" + mp.first;
            // 节点存储的数据是当前 provider 的 ip:port
            std::string method_path_data = ip + ":" + std::to_string(port);
            std::string instance_path = method_path + "/" + method_path_data;
            // ZOO_EPHEMERAL 临时节点，断开连接自动删除
            zk.Create(instance_path.c_str(), nullptr, 0, ZOO_EPHEMERAL);
            LOG(INFO) << "Register RPC service: " << method_path.c_str() << " success!";
        }
    }
    server.start();
    event_loop.loop();
}

void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr &conn)
{
    if (!conn->connected()) {
        conn->shutdown();
    }
}

void RpcProvider::OnMessage(
    const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buffer, muduo::Timestamp receive_time)
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

        // 调用业务逻辑
        service->CallMethod(method, nullptr, request, response, done);
    }
}

// 将 RPC 响应对象序列化并发送回客户端
void RpcProvider::SendResponse(
    const muduo::net::TcpConnectionPtr &conn, google::protobuf::Message *request, google::protobuf::Message *response)
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

RpcProvider::~RpcProvider()
{
    for (auto &i : service_map) {
        if (i.second.service != nullptr) {
            delete i.second.service;
            i.second.service = nullptr;
        }
    }
}
