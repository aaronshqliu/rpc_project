#ifndef RPC_CHANNEL_H
#define RPC_CHANNEL_H

#include <atomic>
#include <google/protobuf/service.h>
#include <shared_mutex>
#include <string>
#include <unordered_map>

class MyRpcChannel : public google::protobuf::RpcChannel {
public:
    MyRpcChannel() {}
    virtual ~MyRpcChannel() {}

    void CallMethod(const ::google::protobuf::MethodDescriptor *method, ::google::protobuf::RpcController *controller,
        const ::google::protobuf::Message *request, ::google::protobuf::Message *response,
        ::google::protobuf::Closure *done) override;
    
    void PreFetchService(const std::string &service_name, const std::string &method_name);

private:
    struct ServiceHost {
        std::string ip;
        uint16_t port;

        bool operator==(const ServiceHost &other) const
        {
            return ip == other.ip && port == other.port;
        }

        bool operator!=(const ServiceHost &other) const
        {
            return !(*this == other);
        }

        std::string ToString() const
        {
            return ip + ":" + std::to_string(port);
        }
    };

    // 将主机列表和独立的轮询计数器绑定在一起
    struct ServiceNodeList {
        std::vector<ServiceHost> hosts;
        std::atomic<uint32_t> next_idx {0}; // 专属这个服务节点的计数器，从 0 开始
    };

    std::unordered_map<std::string, std::shared_ptr<ServiceNodeList>>
        host_cache; // 缓存的是该方法下的【所有可用节点列表】
    std::shared_mutex cache_mutex;

    ServiceHost QueryZkForHost(const std::string &service_name, const std::string &method_name);
    void RemoveInvalidHost(const std::string &path, const ServiceHost &invalid_host);
    void BackgroundRefreshCache(const std::string &path);
    ServiceHost GetHostByRoundRobin(std::shared_ptr<ServiceNodeList> list);
    std::vector<ServiceHost> ParseHostStrings(const std::vector<std::string> &host_strs);
};

#endif
