#ifndef ORDER_SERVICE_H
#define ORDER_SERVICE_H

#include "order.pb.h"

class OrderService : public order::OrderServiceRpc {
public:
    void MakeOrder(::google::protobuf::RpcController *controller,
                   const ::order::MakeOrderRequest *request,
                   ::order::MakeOrderResponse *response,
                   ::google::protobuf::Closure *done) override;

private:
    int DoMakeOrder(int32_t user_id, int32_t product_id, std::string &out_order_id);
};

#endif // ORDER_SERVICE_H
