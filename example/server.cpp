#include "rpc_application.h"
#include "rpc_provider.h"
#include "user.pb.h"

#include <glog/logging.h>
#include <iostream>

class UserService : public user::UserServiceRpc {
public:
    void Login(::google::protobuf::RpcController *controller,
               const ::user::LoginRequest *request,
               ::user::LoginResponse *response,
               ::google::protobuf::Closure *done) override
    {
        std::string name = request->username();
        std::string password = request->password();

        int result = Login(name, password);
        user::ResultCode *code = response->mutable_result();
        code->set_code(result);
        code->set_message(result == 0 ? "Login successful" : "Login failed");

        done->Run();
    }

    void Register(::google::protobuf::RpcController *controller,
                  const ::user::RegisterRequest *request,
                  ::user::RegisterResponse *response,
                  ::google::protobuf::Closure *done) override
    {
        std::string name = request->username();
        std::string password = request->password();

        int result = Register(name, password);
        user::ResultCode *code = response->mutable_result();
        code->set_code(result);
        code->set_message(result == 0 ? "Register successful" : "Register failed");

        done->Run();
    }

private:
    int Login(const std::string &name, const std::string &password)
    {
        // LOG(INFO) << "Login method called with name: " << name << " and password: " << password;
        return 0; // 返回0表示登录成功
    }

    int Register(const std::string &name, const std::string &password)
    {
        // LOG(INFO) << "Register method called with name: " << name << " and password: " << password;
        return 0; // 返回0表示注册成功
    }
};

int main(int argc, char **argv)
{
    // 初始化RPC应用程序
    RpcApplication::Init(argc, argv);
    RpcProvider provider;
    provider.NotifyService(new UserService());
    provider.Run();
    return 0;
}
