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
    std::unordered_map<std::string, std::vector<std::string>> host_cache; // 缓存的是该方法下的【所有可用节点列表】
    std::shared_mutex cache_mutex;
    std::atomic<uint32_t> request_count {0};

    ssize_t recv_exact(int fd, char *buffer, size_t length);
    ServiceHost QueryZkForHost(const std::string &service_name, const std::string &method_name);
    void RemoveInvalidHost(const std::string &path, const std::string &invalid_host_str);
};

#endif
