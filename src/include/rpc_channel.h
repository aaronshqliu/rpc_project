#ifndef RPC_CHANNEL_H
#define RPC_CHANNEL_H

#include <atomic>
#include <google/protobuf/service.h>
#include <shared_mutex>
#include <unordered_map>

class MyRpcChannel : public google::protobuf::RpcChannel {
public:
    MyRpcChannel();
    virtual ~MyRpcChannel() = default;

    void CallMethod(const ::google::protobuf::MethodDescriptor *method, ::google::protobuf::RpcController *controller,
        const ::google::protobuf::Message *request, ::google::protobuf::Message *response,
        ::google::protobuf::Closure *done) override;

private:
    struct ServiceHost {
        std::string ip;
        uint16_t port;
    };

    // 将主机列表和独立的轮询计数器绑定在一起
    struct ServiceNodeList {
        std::vector<std::string> hosts;
        std::atomic<uint32_t> next_idx{0}; // 专属这个服务节点的计数器，从 0 开始
    };
    std::unordered_map<std::string, std::shared_ptr<ServiceNodeList>> host_cache; // 缓存的是该方法下的【所有可用节点列表】
    std::shared_mutex cache_mutex;

    ssize_t recv_exact(int fd, char *buffer, size_t length);
    ServiceHost QueryZkForHost(const std::string &service_name, const std::string &method_name);
    void RemoveInvalidHost(const std::string &path, const std::string &invalid_host_str);
};

#endif
