#include "rpc_provider.h"
#include "rpc_application.h"
#include "rpc_header.pb.h"
#include "zk_client.h"

#include <glog/logging.h>
#include <muduo/net/InetAddress.h>

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

    // 获取服务的描述信息
    const google::protobuf::ServiceDescriptor *service_desc = service->GetDescriptor();

    for (int i = 0; i < service_desc->method_count(); ++i) {
        // 获取具体方法的描述
        const google::protobuf::MethodDescriptor *method_desc = service_desc->method(i);
        info.method_map[method_desc->name()] = method_desc;
    }

    info.service = std::unique_ptr<google::protobuf::Service>(service);
    service_map.try_emplace(service_desc->name(), std::move(info));
}

void RpcProvider::Run()
{
    // 读取配置（IP + port）
    ip = RpcApplication::GetInstance().GetConfig().GetString("rpc_server_ip");
    port = atoi(RpcApplication::GetInstance().GetConfig().GetString("rpc_server_port").c_str());

    muduo::net::InetAddress address(ip, port);
    tcp_server = std::make_unique<muduo::net::TcpServer>(&event_loop, address, "RpcProvider");

    // 绑定连接回调和消息回调，分离网络连接业务和消息处理业务
    tcp_server->setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    tcp_server->setMessageCallback(
        std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    tcp_server->setThreadNum(4);
    tcp_server->start();

    // 设置重连回调并注册
    ZkClient &zk = RpcApplication::GetInstance().GetZkClient();
    zk.SetRecoveryHandler(std::bind(&RpcProvider::RegisterServiceToZk, this));
    zk.Start();
    RegisterServiceToZk();

    LOG(INFO) << "RpcProvider started on " << ip << ":" << port;

    event_loop.loop();
}

void RpcProvider::RegisterServiceToZk()
{
    ZkClient &zk = RpcApplication::GetInstance().GetZkClient();
    std::string port_str = std::to_string(port);
    std::string host_data = ip + ":" + port_str;

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
    if (!conn->connected()) {
        conn->shutdown();
    }
}

void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn,
                            muduo::net::Buffer *buffer,
                            muduo::Timestamp receive_time)
{
    // 约定协议 [4字节(魔数)][4字节(总长度)][4字节(Header长度)][变长字节 RpcRequestHeader(序列化的二进制)][变长字节 Method Args(序列化的二进制)]
    // 这里的“总长度”定义为后面三个部分的大小之和：4 + header_size + args_size

    // 循环处理缓冲区，解决粘包问题：至少要能读出 8字节（4字节魔数 + 4字节总长度）
    while (buffer->readableBytes() >= 8) {
        // 1. 读取魔数进行安全校验
        uint32_t magic_num = ntohl(*(uint32_t*)(buffer->peek()));
        if (magic_num != 0x12345678) { // 约定的魔数是 0x12345678
            LOG(ERROR) << "Invalid Magic Number! Suspected malicious request.";
            conn->shutdown();
            return;
        }

        // 2. 读取总长度
        uint32_t total_size = ntohl(*(uint32_t*)(buffer->peek() + 4));

        // 3. 解决半包：如果当前缓冲区的数据不够一个完整的包 (8 字节头 + total_size)
        if (buffer->readableBytes() < 8 + total_size) {
            break; // 数据体还不完整，跳出循环，等待底层的 epoll 接着触发可读事件。
        }

        //////////////////////////////////////////////
        // 走到这里，说明 buffer 里绝对包含了一个完整的协议包
        //////////////////////////////////////////////

        // 4. 读取 4 字节的 Header 长度
        uint32_t header_size = ntohl(*(uint32_t*)(buffer->peek() + 8));

        // 5. 提取 RpcRequestHeader 二进制流并反序列化 (偏移量为 4+4+4 = 12)
        std::string rpc_header_str(buffer->peek() + 12, header_size);
        myrpc::RpcRequestHeader request_header;
        if (!request_header.ParseFromString(rpc_header_str)) {
            LOG(ERROR) << "Failed to parse RpcRequestHeader";
            conn->shutdown();
            return;
        }

        // 6. 提取 Args 二进制流 (偏移量为 12 + header_size)
        uint32_t args_size = request_header.args_size();
        
        if (4 + header_size + args_size != total_size) {  // 校验协议包的数据大小是否被篡改或越界
            LOG(ERROR) << "Protocol size mismatch! Packet corrupted.";
            conn->shutdown();
            return;
        }

        std::string args_str(buffer->peek() + 12 + header_size, args_size);

        // 7. 数据已经全部取出，从 muduo 缓冲区中将这整个包“抹掉”
        buffer->retrieve(8 + total_size);

        ////////////////////
        // 以下是业务逻辑分发层
        ////////////////////

        // 拦截心跳包
        if (request_header.msg_type() == myrpc::PING) {
            // 1. 构造 Pong 响应头
            myrpc::RpcResponseHeader pong_header;
            pong_header.set_errcode(0);
            pong_header.set_errmsg("");
            pong_header.set_msg_type(myrpc::PONG);

            std::string pong_str;
            pong_header.SerializeToString(&pong_str);

            // 2. 计算长度 (心跳包没有业务 Data，所以 total_size 也就是 4 + header_size)
            uint32_t header_size = pong_str.size();
            uint32_t total_size = 4 + header_size; 

            uint32_t net_total_size = htonl(total_size);
            uint32_t net_header_size = htonl(header_size);

            // 3. 组装 PONG 报文
            // 响应协议：[4字节总长度] + [4字节Header长度] + [RpcResponseHeader]
            std::string send_buf;
            send_buf.append((const char *)&net_total_size, 4);
            send_buf.append((const char *)&net_header_size, 4);
            send_buf.append(pong_str);

            conn->send(send_buf); // 发送 PONG
            continue; // 处理完毕，解析缓冲区里的下一个包
        }

        // 如果是 NORMAL_RPC，则继续走正常的业务分发
        // 定义一个专属的 Lambda 函数，用于发送框架级错误响应
        auto send_error_response = [&conn](int errcode, const std::string& errmsg) {
            myrpc::RpcResponseHeader error_resp;
            error_resp.set_errcode(errcode);
            error_resp.set_errmsg(errmsg);
            error_resp.set_msg_type(myrpc::NORMAL_RPC); // 这是一个业务请求的响应，只是失败了

            std::string header_str;
            error_resp.SerializeToString(&header_str);

            uint32_t header_size = header_str.size();
            // 发生框架级错误时，我们不需要携带业务的 Response Data，所以总长度就是 4 + header_size
            uint32_t total_size = 4 + header_size; 

            uint32_t net_total_size = htonl(total_size);
            uint32_t net_header_size = htonl(header_size);

            muduo::net::Buffer send_buffer;
            send_buffer.append((const char*)&net_total_size, 4);
            send_buffer.append((const char*)&net_header_size, 4);
            send_buffer.append(header_str);

            conn->send(&send_buffer); // 把 404 错误包打回给客户端
        };

        auto service_map_it = service_map.find(request_header.service_name());
        if (service_map_it == service_map.end()) {
            std::string err_msg = "Service [" + request_header.service_name() + "] does not exist!";
            LOG(ERROR) << err_msg;
            // 触发回包：告诉客户端 404 找不到服务
            send_error_response(404, err_msg);
            continue; // 这个包处理完了（以失败告终），继续看缓冲区里有没有下一个包
        }

        auto method_map_it = service_map_it->second.method_map.find(request_header.method_name());
        if (method_map_it == service_map_it->second.method_map.end()) {
            std::string err_msg = "Method [" + request_header.method_name() + "] does not exist!";
            LOG(ERROR) << err_msg;
            // 触发回包：告诉客户端 404 找不到方法
            send_error_response(404, err_msg);
            continue;
        }

        auto service = service_map_it->second.service.get();
        auto method = method_map_it->second;

        // 生成请求对象并反序列化
        auto request = service->GetRequestPrototype(method).New();
        request->ParseFromString(args_str);

        // 生成响应对象
        auto response = service->GetResponsePrototype(method).New();

        // 将响应回调分发给业务逻辑
        RpcClosure *done = new RpcClosure([conn, request, response]() {
            if (!conn->connected()) {
                LOG(WARNING) << "Client disconnected before sending response. Aborting serialize.";
                delete request;
                delete response;
                return;
            }

            std::string response_str;
            if (response->SerializeToString(&response_str)) {

                // 1. 构造响应头
                myrpc::RpcResponseHeader resp_header;
                resp_header.set_errcode(0); // 0 表示框架层面调用成功
                resp_header.set_errmsg("");

                std::string header_str;
                resp_header.SerializeToString(&header_str);

                // 2. 计算长度
                uint32_t header_size = header_str.size();
                uint32_t total_size = 4 + header_size + response_str.size();

                // 3. 转换字节序
                uint32_t net_total_size = htonl(total_size);
                uint32_t net_header_size = htonl(header_size);

                // 4. 组装响应包：[总长度] + [Header长度] + [Header数据] + [业务数据]
                muduo::net::Buffer send_buffer;
                send_buffer.append((const char*)&net_total_size, 4);
                send_buffer.append((const char*)&net_header_size, 4);
                send_buffer.append(header_str);
                send_buffer.append(response_str);

                conn->send(&send_buffer);
            } else {
                LOG(ERROR) << "Serialize response error!";
            }

            delete request;
            delete response;
        });

        // 调用业务逻辑：Register、Login、MakeOrder等，这里可以放到线程池中完成
        service->CallMethod(method, nullptr, request, response, done);
    }
}
