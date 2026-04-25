#include "net_utils.h"

#include <cerrno>
#include <sys/socket.h>
#include <unistd.h>

namespace net_utils {
ssize_t recv_exact(int fd, char *buffer, size_t length)
{
    size_t total_received = 0;
    while (total_received < length) {
        ssize_t bytes = recv(fd, buffer + total_received, length - total_received, 0);
        if (bytes > 0) {
            total_received += bytes;
        } else if (bytes == 0) {
            // 对端关闭了连接
            return total_received;
        } else {
            // 被系统信号中断，继续尝试读取
            if (errno == EINTR) {
                continue;
            }
            // 真正发生网络错误
            return -1;
        }
    }
    return total_received;
}
} // namespace net_utils
