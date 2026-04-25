#ifndef NET_UTILS_H
#define NET_UTILS_H

#include <sys/types.h>

namespace net_utils {
ssize_t recv_exact(int fd, char *buffer, size_t length);
}

#endif // NET_UTILS_H
