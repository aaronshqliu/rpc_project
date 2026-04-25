#include "order_service.h"

void OrderService::MakeOrder(::google::protobuf::RpcController* controller,
                             const ::order::MakeOrderRequest* request,
                             ::order::MakeOrderResponse* response,
                             ::google::protobuf::Closure* done) 
{
    int32_t user_id = request->user_id();
    int32_t product_id = request->product_id();
    std::string generated_order_id;

    int ret_code = DoMakeOrder(user_id, product_id, generated_order_id);

    // 根据业务函数的返回值，组装 Response
    if (ret_code == 0) {
        response->set_success(true);
        response->set_order_id(generated_order_id);
        response->set_errmsg("");
    } else {
        response->set_success(false);
        response->set_order_id("");
        
        // 错误码转义（通常这里会有一个公共的 ErrorCodeToMsg 函数，这里简化演示）
        switch (ret_code) {
            case 1001:
                response->set_errmsg("Inventory shortage (库存不足)");
                break;
            case 1002:
                response->set_errmsg("User account frozen (用户账户被冻结)");
                break;
            default:
                response->set_errmsg("Unknown system error, code: " + std::to_string(ret_code));
                break;
        }
    }

    done->Run();
}

int OrderService::DoMakeOrder(int32_t user_id, int32_t product_id, std::string& out_order_id) 
{
    if (product_id == 999) {
        return 1001;
    }

    if (user_id == -1) {
        return 1002;
    }

    out_order_id = "ORD-" + std::to_string(user_id) + "-" + std::to_string(product_id);

    return 0;
}
