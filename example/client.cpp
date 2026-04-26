#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "rpc_application.h"
#include "rpc_channel.h"
#include "rpc_controller.h"

#include "user.pb.h"
#include "order.pb.h"

// 发送请求的函数，模拟RPC客户端发送请求并处理响应
void SendRequest(int thread_id, std::shared_ptr<MyRpcChannel> shared_channel, std::atomic<int> &success_counts,
    std::atomic<int> &failure_counts, int requests_per_thread)
{
    // 所有线程共享同一个 channel 实例，共用 ZK 缓存和轮询计数器
    user::UserServiceRpc_Stub user_stub(shared_channel.get());
    order::OrderServiceRpc_Stub order_stub(shared_channel.get());

    MyRpcController controller;

    user::RegisterRequest reg_request;
    user::RegisterResponse reg_response;
    user::LoginRequest login_request;
    user::LoginResponse login_response;

    order::MakeOrderRequest order_request;
    order::MakeOrderResponse order_response;

    for (int i = 0; i < requests_per_thread; ++i) {
        // 1. 测试 Register
        controller.Reset();
        reg_request.Clear();
        reg_response.Clear();

        int current_user_id = thread_id * 10000 + i;
        std::string unique_username = "user_" + std::to_string(thread_id) + "_" + std::to_string(i);

        reg_request.set_id(current_user_id);
        reg_request.set_username(unique_username);
        reg_request.set_password("12345");

        // 第4个参数 google::protobuf::Closure *done：
        // 1. 传 nullptr：明确告诉底层的 CallMethod：客户端线程是一个同步调用。
        // 我会在这里阻塞，直到你通过网络把 reg_response 填满，我才会执行下一行代码。你搞定之后不需要叫醒我，因为我自己一直在等。
        // 2. 传一个真实的 Closure 对象：明确告诉底层：客户端线程是一个异步调用。
        // 我把请求交给你了，我先去干别的活儿。你什么时候收到服务端的包了，记得调用一下 done->Run() 来通知我。
        user_stub.Register(&controller, &reg_request, &reg_response, nullptr);

        if (controller.Failed()) {
            // LOG(ERROR) << "Thread " << thread_id << " - RPC failed: " << controller.ErrorText();
            failure_counts++;
        } else {
            if (reg_response.result().code() == 0) {
                success_counts++;
            } else {
                failure_counts++;
            }
        }

        // 2. 测试 Login
        controller.Reset();
        login_request.Clear();
        login_response.Clear();

        login_request.set_username(unique_username);
        login_request.set_password("12345");

        user_stub.Login(&controller, &login_request, &login_response, nullptr);

        if (controller.Failed()) {
            failure_counts++;
        } else {
            if (login_response.result().code() == 0) {
                success_counts++;
            } else {
                failure_counts++;
            }
        }

        // 3. 测试 MakeOrder
        controller.Reset();
        order_request.Clear(); 
        order_response.Clear();

        order_request.set_user_id(current_user_id);
        order_request.set_product_id(1);

        order_stub.MakeOrder(&controller, &order_request, &order_response, nullptr);

        if (controller.Failed()) {
            failure_counts++;
        } else {
            if (order_response.success() == true) { 
                success_counts++; 
            } else { 
                failure_counts++; 
            }
        }

    }
}

int main(int argc, char **argv)
{
    RpcApplication::GetInstance().Init(argc, argv);

    const int thread_counts = 500;
    const int requests_per_thread = 1000;
    std::vector<std::thread> threads;
    std::atomic<int> success_counts(0);
    std::atomic<int> failure_counts(0);

    auto start_time = std::chrono::high_resolution_clock::now();
    auto shared_channel = std::make_shared<MyRpcChannel>();

    LOG(INFO) << "Initializing Service Discovery...";
    shared_channel->PreFetchService("UserServiceRpc", "Login");
    shared_channel->PreFetchService("OrderServiceRpc", "MakeOrder");

    for (int i = 0; i < thread_counts; ++i) {
        threads.emplace_back(
            SendRequest, i, shared_channel, std::ref(success_counts), std::ref(failure_counts), requests_per_thread);
    }

    for (auto &thread : threads) {
        thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end_time - start_time;
    double duration_seconds = diff.count();

    // 因为每次循环有注册、登录、下单 3 个请求，所以这里乘 3
    long long total_expected_requests = thread_counts * requests_per_thread * 3;
    long long actual_completed_requests = success_counts.load() + failure_counts.load();

    LOG(INFO) << "================ RPC 压测报告 ================";
    LOG(INFO) << "预期总请求数 (Expected) : " << total_expected_requests;
    LOG(INFO) << "实际完成数 (Completed)  : " << actual_completed_requests;
    LOG(INFO) << "成功请求数 (Success)    : " << success_counts.load();
    LOG(INFO) << "失败请求数 (Failed)     : " << failure_counts.load();
    LOG(INFO) << "总计耗时 (Time taken)   : " << duration_seconds << " seconds";

    if (duration_seconds > 0.000001) {
        double total_qps = actual_completed_requests / duration_seconds;
        double success_qps = success_counts.load() / duration_seconds;

        LOG(INFO) << "总体吞吐量 (Total QPS)  : " << static_cast<long long>(total_qps) << " req/s";
        LOG(INFO) << "成功吞吐量 (Success QPS): " << static_cast<long long>(success_qps) << " req/s";
    }
    LOG(INFO) << "=============================================";

    return 0;
}
