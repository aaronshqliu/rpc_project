#ifndef RPC_CONTROLLER_H
#define RPC_CONTROLLER_H

#include <google/protobuf/service.h>

class MyRpcController : public google::protobuf::RpcController {
public:
    MyRpcController();

    // 重置 Controller 状态，以便复用
    void Reset() override;

    // 检查在一次 RPC 调用过程中是否发生了错误
    bool Failed() const override;

    // 获取具体的错误信息
    std::string ErrorText() const override;

    // RpcChannel 在网络发送失败时，调用此方法设置错误状态和信息
    void SetFailed(const std::string &reason) override;

    // 以下三个方法用于支持异步 RPC 调用的取消功能, 暂不实现
    void StartCancel() override;
    bool IsCanceled() const override;
    void NotifyOnCancel(google::protobuf::Closure *callback) override;

private:
    bool failed;         // 标记 RPC 调用是否失败
    std::string errText; // 存储失败的具体原因
};

#endif
