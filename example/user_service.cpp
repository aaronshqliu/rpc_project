#include "user_service.h"

void UserService::Login(::google::protobuf::RpcController *controller,
                        const ::user::LoginRequest *request,
                        ::user::LoginResponse *response,
                        ::google::protobuf::Closure *done)
{
    std::string name = request->username();
    std::string password = request->password();

    int result = DoLogin(name, password);
    user::ResultCode *code = response->mutable_result();
    code->set_code(result);
    code->set_message(result == 0 ? "Login successful." : "Login Failed.");
    done->Run();
}

void UserService::Register(::google::protobuf::RpcController *controller,
                           const ::user::RegisterRequest *request,
                           ::user::RegisterResponse *response,
                           ::google::protobuf::Closure *done)
{
    std::string name = request->username();
    std::string password = request->password();

    int result = DoRegister(name, password);
    user::ResultCode *code = response->mutable_result();
    code->set_code(result);
    code->set_message(result == 0 ? "Register successful." : "Register Failed.");
    done->Run();
}

int UserService::DoLogin(const std::string &name, const std::string &password)
{
    // LOG(INFO) << "Login method called with name: " << name << " and password: " << password;
    return 0; // 返回0表示登录成功
}

int UserService::DoRegister(const std::string &name, const std::string &password)
{
    // LOG(INFO) << "Register method called with name: " << name << " and password: " << password;
    return 0; // 返回0表示注册成功
}
