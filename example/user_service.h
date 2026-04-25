#ifndef USER_SERVICE_H
#define USER_SERVICE_H

#include "user.pb.h"

class UserService : public user::UserServiceRpc {
public:
    void Login(::google::protobuf::RpcController *controller,
               const ::user::LoginRequest *request,
               ::user::LoginResponse *response,
               ::google::protobuf::Closure *done) override;

    void Register(::google::protobuf::RpcController *controller,
                  const ::user::RegisterRequest *request,
                  ::user::RegisterResponse *response,
                  ::google::protobuf::Closure *done) override;
    
private:
    int DoLogin(const std::string &name, const std::string &password);
    int DoRegister(const std::string &name, const std::string &password);
    
};

#endif // USER_SERVICE_H
