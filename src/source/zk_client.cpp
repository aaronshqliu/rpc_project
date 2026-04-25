#include "zk_client.h"
#include "rpc_application.h"

#include <glog/logging.h>
#include <thread>

void global_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcher_context)
{
    ZkClient *client = static_cast<ZkClient *>(watcher_context);
    // 1. 处理连接状态
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            client->NotifyConnected(); // 调用 ZkClient 的 NotifyConnected 方法，通知连接成功
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            LOG(ERROR) << "ZK Session Expired! Attempting to reconnect...";
            // 异步重连，防止阻塞 ZK 线程
            std::thread([client]() {
                client->Reconnect();
            }).detach();
        }
    }
    // 2. 处理子节点变动
    else {
        // 处理节点变动事件 (ZOO_CHANGED_EVENT, ZOO_DELETED_EVENT, etc.)
        client->InvokeNotifyHandler(type, path);
    }
}

ZkClient::ZkClient() : zk_handle(nullptr), connected(false) {}

ZkClient::~ZkClient()
{
    if (zk_handle != nullptr) {
        zookeeper_close(zk_handle); // 关闭句柄，释放底层的网络连接资源
    }
}

void ZkClient::NotifyConnected()
{
    std::lock_guard<std::mutex> lock(mtx);
    connected = true;
    cv.notify_all(); // 唤醒阻塞在 condition_variable 上的线程
}

void ZkClient::SubscribeWatcher(const std::string &path, ZkNotifyHandler handler)
{
    std::unique_lock<std::shared_mutex> lock(handlers_mtx);
    notify_handlers[path] = std::move(handler);
}

void ZkClient::InvokeNotifyHandler(int type, const char *path)
{
    if (path == nullptr) {
        return;
    }

    std::string path_str(path);
    ZkNotifyHandler handler_to_call = nullptr;

    // 加读锁，查找对应的 handler
    {
        std::shared_lock<std::shared_mutex> lock(handlers_mtx);
        auto it = notify_handlers.find(path_str);
        if (it != notify_handlers.end()) {
            handler_to_call = it->second;
        }
    }

    // 找到后执行回调 (在锁外执行，防止死锁或回调执行太慢阻塞其他线程)
    if (handler_to_call) {
        handler_to_call(type, path_str);
    }
}

void ZkClient::SetRecoveryHandler(SessionRecoveryHandler handler)
{
    recovery_handler = handler;
}

void ZkClient::Reconnect() {
    // 1. 关闭老句柄
    if (zk_handle != nullptr) {
        zookeeper_close(zk_handle);
        zk_handle = nullptr;
    }
    
    // 2. 重置连接标志
    connected = false;

    // 3. 重新调用初始化
    LOG(INFO) << "Re-initializing zookeeper...";
    zk_handle = zookeeper_init(conn_str.c_str(), global_watcher, 10000, nullptr, this, 0);
    
    // 4. 等待新的连接成功
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this] { return connected; });

    // 5. 通知上层 Provider 重新注册服务
    if (recovery_handler) {
        LOG(INFO) << "ZK reconnected, triggering service re-registration...";
        std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 3000));
        recovery_handler();
    }
}

void ZkClient::Start(int timeout_ms)
{
    if (zk_handle != nullptr) {
        return;
    }
    std::string host = RpcApplication::GetInstance().GetConfig().GetString("zookeeper_ip");
    std::string port = RpcApplication::GetInstance().GetConfig().GetString("zookeeper_port");
    std::string conn_str = host + ":" + port;

    /*
     * zookeeper_init 函数是非阻塞的，它会创建三个内部线程（API调用线程、网络I/O线程、事件回调线程）
     * 函数本身返回成功，只代表句柄内存分配成功，不代表网络连接成功！
     * 第3个参数是 session 超时时间
     * 第5个参数是 watcher_context (上下文)。这里直接把 this 指针传进去，这样在 global_watcher 中就能获取到当前的
     * ZkClient 实例。
     */
    zk_handle = zookeeper_init(conn_str.c_str(), global_watcher, 10000, nullptr, this, 0);
    if (zk_handle == nullptr) {
        LOG(ERROR) << "zookeeper_init error!"; // 内存分配失败
        return;
    }

    std::unique_lock<std::mutex> lock(mtx);

    // 增加超时等待逻辑
    if (!cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] {
            return connected;
        })) {
        LOG(ERROR) << "zookeeper_init timeout! Check your ZK server status.";
    }

    LOG(INFO) << "zookeeper_init success!";
}

void ZkClient::Create(const char *path, const char *data, int datalen, int state)
{
    std::string path_str(path);
    size_t pos = 1; // 跳过第一个 '/'

    // 1. 递归检查并创建父节点（持久节点）
    while ((pos = path_str.find('/', pos)) != std::string::npos) {
        std::string parent_path = path_str.substr(0, pos);
        int res = zoo_exists(zk_handle, parent_path.c_str(), 0, nullptr);
        if (res == ZNONODE) {
            char buf[128];
            // 父节点强制设为持久节点 (state = 0)
            int rc = zoo_create(zk_handle, parent_path.c_str(), nullptr, -1, &ZOO_OPEN_ACL_UNSAFE, 0, buf, sizeof(buf));
            if (rc != ZOK && rc != ZNODEEXISTS) {
                LOG(ERROR) << "Create parent node failed: " << parent_path;
                return;
            }
        }
        pos++;
    }

    // 2. 创建最终的目标节点
    char path_buffer[256];
    int rc = zoo_create(zk_handle, path_str.c_str(), data, datalen, &ZOO_OPEN_ACL_UNSAFE, state, path_buffer, sizeof(path_buffer));
    if (rc == ZOK) {
        LOG(WARNING) << "Create znode success: " << path_str;
    } else if (rc == ZNODEEXISTS) {
        LOG(ERROR) << "Znode already exists! Another instance is running at: " << path_str;
        // 如果这是临时节点 (state & ZOO_EPHEMERAL)，说明冲突严重，建议直接结束进程
        if (state & ZOO_EPHEMERAL) {
            LOG(FATAL) << "Service registration conflict. Please check your port configuration!";
            exit(EXIT_FAILURE); 
        }
    } else {
        LOG(FATAL) << "Create znode failed: " << path_str << ", code: " << rc;
    }
}

bool ZkClient::GetChildren(const char *path, std::vector<std::string> &children, bool watch)
{
    children.clear();
    struct String_vector strings;

    // 调用 ZK C API：第三个参数 watch 设为 1 表示在该路径上挂载子节点监听
    // 当有 Provider 上线（新增子节点）或下线（删除子节点）时，会触发 ZOO_CHILD_EVENT
    int rc = zoo_get_children(zk_handle, path, watch ? 1 : 0, &strings);
    if (rc != ZOK) {
        if (rc == ZNONODE) {
            return true; // 返回成功，但 children 为空
        }
        // 如果是其他错误（网络、超时、Session失效），返回失败
        LOG(ERROR) << "ZK GetChildren failed... path: " << path << ", code: " << rc;
        return false;
    }

    // 成功获取，填充数据
    for (int i = 0; i < strings.count; ++i) {
        children.emplace_back(strings.data[i]);
    }
    deallocate_String_vector(&strings);
    return true;
}

void ZkClient::Close()
{
    if (zk_handle != nullptr) {
        zookeeper_close(zk_handle); // 这会立刻断开 Session，临时节点瞬间消失
        zk_handle = nullptr;
        LOG(INFO) << "ZkClient manually closed, ephemeral nodes deleted.";
    }
}
