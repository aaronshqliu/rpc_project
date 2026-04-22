#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "rpc_application.h"
#include "rpc_channel.h"
#include "rpc_controller.h"
#include "user.pb.h"

// 发送请求的函数，模拟RPC客户端发送请求并处理响应
void SendRequest(int thread_id, MyRpcChannel *shared_channel, std::atomic<int> &success_counts,
    std::atomic<int> &failure_counts, int requests_per_thread)
{
    // 所有线程共享同一个 channel 实例，共用 ZK 缓存和轮询计数器
    user::UserServiceRpc_Stub stub(shared_channel);

    MyRpcController controller;
    user::RegisterRequest reg_request;
    user::RegisterResponse reg_response;
    user::LoginRequest login_request;
    user::LoginResponse login_response;

    for (int i = 0; i < requests_per_thread; ++i) {
        controller.Reset();

        // 利用循环变量 i 保证每次注册的用户名唯一，防止业务层报重复错误
        std::string unique_username = "user_" + std::to_string(thread_id) + "_" + std::to_string(i);

        reg_request.set_id(thread_id * 10000 + i);
        reg_request.set_username(unique_username);
        reg_request.set_password("12345");

        stub.Register(&controller, &reg_request, &reg_response, nullptr);

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

        controller.Reset();
        login_request.set_username(unique_username);
        login_request.set_password("12345");

        stub.Login(&controller, &login_request, &login_response, nullptr);

        if (controller.Failed()) {
            failure_counts++;
        } else {
            if (login_response.result().code() == 0) {
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

    const int thread_counts = 50;
    const int requests_per_thread = 200;
    std::vector<std::thread> threads;
    std::atomic<int> success_counts(0);
    std::atomic<int> failure_counts(0);

    MyRpcChannel shared_channel;

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < thread_counts; ++i) {
        threads.emplace_back(
            SendRequest, i, &shared_channel, std::ref(success_counts), std::ref(failure_counts), requests_per_thread);
    }

    for (auto &thread : threads) {
        thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end_time - start_time;
    double duration_seconds = diff.count();

    long long total_expected_requests = thread_counts * requests_per_thread * 2;
    long long actual_completed_requests = success_counts.load() + failure_counts.load();

    LOG(INFO) << "================ RPC 压测报告 ================";
    LOG(INFO) << "预期总请求数 (Expected) : " << total_expected_requests;
    LOG(INFO) << "实际完成数 (Completed)  : " << actual_completed_requests;
    LOG(INFO) << "成功请求数 (Success)   : " << success_counts.load();
    LOG(INFO) << "失败请求数 (Failed)    : " << failure_counts.load();
    LOG(INFO) << "总计耗时 (Time taken)  : " << duration_seconds << " seconds";

    if (duration_seconds > 0.000001) {
        double total_qps = actual_completed_requests / duration_seconds;
        double success_qps = success_counts.load() / duration_seconds;

        LOG(INFO) << "总体吞吐量 (Total QPS)  : " << static_cast<long long>(total_qps) << " req/s";
        LOG(INFO) << "成功吞吐量 (Success QPS): " << static_cast<long long>(success_qps) << " req/s";
    }
    LOG(INFO) << "=============================================";

    return 0;
}
