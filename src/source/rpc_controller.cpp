#include "rpc_controller.h"

MyRpcController::MyRpcController() : failed(false), errText("") {}

void MyRpcController::Reset()
{
    failed = false;
    errText = "";
}

bool MyRpcController::Failed() const
{
    return failed;
}

std::string MyRpcController::ErrorText() const
{
    return errText;
}

void MyRpcController::SetFailed(const std::string &reason)
{
    failed = true;
    errText = reason;
}

// 暂不实现的异步控制接口
void MyRpcController::StartCancel() {}
bool MyRpcController::IsCanceled() const { return false; }
void MyRpcController::NotifyOnCancel(google::protobuf::Closure* callback) {}
