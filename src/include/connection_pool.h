#ifndef CONNECTION_POOL_H
#define CONNECTION_POOL_H

#include <chrono>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>

class ConnectionPool {
public:
    // 获取全局单例
    static ConnectionPool &GetInstance()
    {
        static ConnectionPool pool;
        return pool;
    }

    // 从连接池获取一个可用的连接
    // 返回 > 0：成功拿到 fd。
    // 返回 -1：网络建连失败（服务器挂了）。
    // 返回 -2：达到最大连接数上限（服务器正忙）。
    int GetConnection(const std::string &ip, uint16_t port);

    // 将健康的连接放回连接池
    void ReleaseConnection(const std::string &ip, uint16_t port, int fd);

    // 销毁坏掉的连接
    void CloseConnection(const std::string &ip, uint16_t port, int fd);

private:
    ConnectionPool() = default;
    ~ConnectionPool();

    // 内部创建新连接的逻辑
    int CreateNewConnection(const std::string &ip, uint16_t port);

    bool Ping(int fd);

    // 将队列中存储的 fd 改为带有时间戳的结构体
    // 连接在池子里排队时，它是 ConnectionItem（需要记录时间以供心跳检测）；连接被业务取走使用时，它退化为普通的 int fd。
    struct ConnectionItem {
        int fd;
        std::chrono::steady_clock::time_point last_active_time;

        ConnectionItem(int f, std::chrono::steady_clock::time_point t) : fd(f), last_active_time(t) {}
    };

    /**
     * 队列的核心特性是 FIFO（先进先出），这对连接池有着天然的好处：
     * 1. 负载均衡与公平性（防止连接“饿死”）：
     * 当你把归还的连接放到队尾，下次取用时从队头拿。这意味着所有的连接都会被轮询使用。
     * 如果不轮询，某些连接可能长达几个小时不被使用，导致服务端或中间路由器（NAT网关）因为超时而单方面切断这些TCP 连接。
     * 2. 完美契合 Ping/Pong 策略：
     * 因为是先进先出，队头的连接永远是闲置时间最长的连接。拿出来时如果发现闲置时间 > 10s，就发一次 Ping。
     * 这样可以保证整个连接池里的连接都能被定期“激活”一遍，保持热度。

     * 为什么不用 vector（动态数组）？
     * vector 是连续内存结构，虽然它的 CPU 缓存命中率极高，但在连接池场景下存在致命缺陷：
     * 如果做 FIFO（先进先出）： 需要从 vector 的头部取连接（pop_front），在 vector 中删除首个元素的时间复杂度是 O(N)。
     * 因为后面的所有元素都需要向前移动一次内存，在并发竞争激烈的高并发 RPC 场景下，这会成为严重的性能瓶颈。
     * 如果做 LIFO（后进先出 - 栈）： 如果你每次从队尾取、还回队尾（push_back / pop_back），时间复杂度是 O(1)。
     * 但这会导致一个问题：极化。最顶上的几个连接会被频繁复用，而底部的连接永远处于闲置状态，进而被服务端断开。
     * 当你突发大流量需要用到下面那些连接时，你会发现它们全都是死连接，导致大量的 Ping 甚至重连，引发瞬间的延迟毛刺。

     * 为什么不用 list（双向链表）？
     * 实际上，在 STL 中，queue 本质上是一个容器适配器，它的底层默认实现正是 deque（双端队列）。你完全可以通过模板参数让它的底层变成 list。
     * 不用原生 list 的原因不在于性能，而在于“工程约束”：list 暴露了太多的 API。它允许在中间任意位置进行插入和删除，也允许双向遍历。
     * 对于连接池来说，业务逻辑根本不需要也不应该在中间插入或删除连接。
     * 使用 queue 相当于在 API 层面加了一把锁，强制约束所有开发者只能遵循“队尾进、队头出”的原则，避免了后人维护代码时写出破坏连接生命周期管理的 Bug。

     * 为什么不用 unordered_map（哈希表）？
     * 哈希表是为了解决 “根据特定 Key 快速查找 Value” 的问题。
     * 需求不匹配： 当业务调用 GetConnection 时，它并不关心拿到的是池子里的“第几个”连接，它只需要任意一个可用的空闲连接。
     * 无序性破坏了超时管理： 哈希表是无序的（或者按 Key 排序的），它无法天然地维持连接的“闲置时间先后顺序”。
     * 你无法轻易地挑出“闲置时间最长的那个”，这会让 > 10s 判定变得随机且不可控。
     * 内存与性能开销： 哈希表需要维护哈希桶、处理哈希冲突，其内存碎片化比队列严重得多，且 insert 和 erase 的常数级时间开销也大于简单的队列操作。

     * 为什么不用 priority_queue (优先级队列)？
     * 优先级队列最大的价值在于动态排序，但连接池的空闲连接归还和借出，本身就具有极强的时间规律性。
     * 当使用 queue 时，每次用完归还的连接都放到队尾，每次借出连接都从队头拿。这个动作天然保证了：队头的连接，永远是闲置时间最长的；队尾的连接，永远是刚刚才闲置的。
     * 队列： push 和 pop 操作的时间复杂度都是 O(1)。这意味着即使并发极高，锁被占用的时间也极短。
     * 优先级队列： 它的底层通常是二叉堆。每次插入或删除一个连接，都需要执行“上浮”或“下沉”操作来重新平衡这棵树，时间复杂度是 O(log N)。
     * 优先级队列通常用于处理非同质化的任务（比如带权重的网络包、不同优先级的定时器任务）。而在同一个 ConnectionPool 中，所有的 ConnectionItem 都是指向同一个服务端的。
     * 它们在功能上是完全等价的、高度同质化的。因此，给它们赋予“优先级”并进行复杂的调度，在业务逻辑上是多此一举。
     */
    std::unordered_map<std::string, std::queue<ConnectionItem>> pool; // 空闲连接池：Key 为 "ip:port"，Value 为可用的 fd 队列
    std::mutex mutex;
};

#endif // CONNECTION_POOL_H
