cmake_minimum_required(VERSION 3.10)
project(MyRpcFramework)

# 1. 设置 C++ 编译标准为 C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 2. 设置编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -pthread")

# 3. 设置输出目录
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)

# 4. 查找 Protobuf 依赖库
find_package(Protobuf REQUIRED)

# 5. 设置头文件搜索路径
include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/example
    ${PROTOBUF_INCLUDE_DIRS}
)

# ==========================================
# 模块一：编译 RPC 框架核心动态库 (libmyrpc.so)
# ==========================================
# 搜集 src 目录下所有的 .cpp 和 protobuf 生成的 .cc 文件
file(GLOB RPC_SRC "src/*.cpp" "src/*.cc")

# 生成动态链接库 myrpc
add_library(myrpc STATIC ${RPC_SRC})

# 链接框架底层的依赖：Protobuf、Muduo、ZooKeeper(多线程版)、pthread
target_link_libraries(myrpc
    ${PROTOBUF_LIBRARIES}
    muduo_net
    muduo_base
    zookeeper_mt 
    pthread
    glog
)


# ==========================================
# 模块二：编译业务代码 (Example)
# ==========================================
# 收集 Example 目录下的公共 protobuf 生成文件
set(EXAMPLE_PROTO_SRC "example/user.pb.cc")

# 1. 编译服务端
add_executable(server example/server.cpp ${EXAMPLE_PROTO_SRC})
# 链接自定义的 rpc 框架库
target_link_libraries(server myrpc)

# 2. 编译客户端
add_executable(client example/client.cpp ${EXAMPLE_PROTO_SRC})
# 链接自定义的 rpc 框架库
target_link_libraries(client myrpc)
