#include "zk_client.h"
#include "rpc_application.h"

#include <glog/logging.h>

void global_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcher_context)
{
    ZkClient *client = static_cast<ZkClient *>(watcher_context);
    // 1. 处理连接状态
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            client->NotifyConnected(); // 调用 ZkClient 的 NotifyConnected 方法，通知连接成功
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

void ZkClient::SetNotifyHandler(ZkNotifyHandler handler)
{
    notify_handler = std::move(handler);
}

void ZkClient::InvokeNotifyHandler(int type, const char *path)
{
    if (notify_handler && path != nullptr) {
        notify_handler(type, std::string(path));
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
    int res = zoo_exists(zk_handle, path_str.c_str(), 0, nullptr);
    if (res == ZOK) {
        // 如果节点已经存在，可能是上次宕机残留的幽灵节点，先强制删除
        LOG(WARNING) << "Znode already exists, possibly leftover from previous crash. Deleting: " << path_str;
        zoo_delete(zk_handle, path_str.c_str(), -1); // -1 表示忽略版本号强制删除
        res = ZNONODE;                               // 重置状态，让它继续往下走去创建
    }
    if (res == ZNONODE) {
        char path_buffer[256];
        int rc = zoo_create(
            zk_handle, path_str.c_str(), data, datalen, &ZOO_OPEN_ACL_UNSAFE, state, path_buffer, sizeof(path_buffer));
        if (rc != ZOK) {
            LOG(FATAL) << "Create znode failed: " << path_str << ", code: " << rc;
        } else {
            LOG(INFO) << "Create znode success: " << path_str;
        }
    }
}

std::vector<std::string> ZkClient::GetChildren(const char *path, bool watch)
{
    std::vector<std::string> children;
    struct String_vector strings;

    // 调用 ZK C API：第三个参数 watch 设为 1 表示在该路径上挂载子节点监听
    // 当有 Provider 上线（新增子节点）或下线（删除子节点）时，会触发 ZOO_CHILD_EVENT
    int flag = zoo_get_children(zk_handle, path, watch ? 1 : 0, &strings);
    if (flag != ZOK) {
        LOG(ERROR) << "Get children error... path: " << path << ", code: " << flag;
        return children; // 如果失败（例如该服务还没有任何提供者注册），返回空 vector
    }

    // 遍历提取子节点数据
    for (int i = 0; i < strings.count; ++i) {
        children.emplace_back(strings.data[i]);
    }

    // 必须释放底层的字符串数组内存，否则会造成内存泄漏
    deallocate_String_vector(&strings);

    return children;
}

void ZkClient::Close()
{
    if (zk_handle != nullptr) {
        zookeeper_close(zk_handle); // 这会立刻断开 Session，临时节点瞬间消失
        zk_handle = nullptr;
        LOG(INFO) << "ZkClient manually closed, ephemeral nodes deleted.";
    }
}
