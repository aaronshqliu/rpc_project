#include "rpc_controller.h"

MyRpcController::MyRpcController() : failed(false), err_text("") {}

void MyRpcController::Reset()
{
    failed = false;
    err_text = "";
}

bool MyRpcController::Failed() const
{
    return failed;
}

std::string MyRpcController::ErrorText() const
{
    return err_text;
}

void MyRpcController::SetFailed(const std::string &reason)
{
    failed = true;
    err_text = reason;
}

// 暂不实现的异步控制接口
void MyRpcController::StartCancel() {}
bool MyRpcController::IsCanceled() const { return false; }
void MyRpcController::NotifyOnCancel(google::protobuf::Closure* callback) {}
