cmake_minimum_required(VERSION 3.10)
project(MyRpcFramework)

# =========================
# 1. C++ 标准
# =========================
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# =========================
# 2. 编译选项
# =========================
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -pthread")

# =========================
# 3. 构建产物统一放 build/
# =========================
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# =========================
# 4. Protobuf
# =========================
find_package(Protobuf REQUIRED)

# =========================
# 5. generated 目录
# =========================
set(GENERATED_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

# =========================
# 6. proto 文件列表
# =========================
set(PROTO_FILES
    ${CMAKE_SOURCE_DIR}/example/user.proto
    ${CMAKE_SOURCE_DIR}/src/rpc_header.proto
)

# =========================
# 7. 自动生成 protobuf
# =========================
foreach(proto ${PROTO_FILES})
    get_filename_component(proto_name ${proto} NAME_WE)
    get_filename_component(proto_dir ${proto} PATH)

    add_custom_command(
        OUTPUT
            ${GENERATED_DIR}/${proto_name}.pb.cc
            ${GENERATED_DIR}/${proto_name}.pb.h

        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS
            -I ${CMAKE_SOURCE_DIR}/example
            -I ${CMAKE_SOURCE_DIR}/src
            --cpp_out=${GENERATED_DIR}
            ${proto}

        DEPENDS ${proto}
        COMMENT "Generating protobuf: ${proto_name}"
    )

    list(APPEND PROTO_SRCS ${GENERATED_DIR}/${proto_name}.pb.cc)
endforeach()

# 统一 proto 生成目标
add_custom_target(generate_proto ALL DEPENDS ${PROTO_SRCS})

# =========================
# 8. include 路径
# =========================
include_directories(
    ${CMAKE_SOURCE_DIR}/src/include
    ${GENERATED_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
)

# =========================
# 9. RPC 框架库
# =========================
file(GLOB RPC_SRC
    ${CMAKE_SOURCE_DIR}/src/source/*.cpp
)

add_library(myrpc STATIC
    ${RPC_SRC}
    ${PROTO_SRCS}
)

add_dependencies(myrpc generate_proto)

target_link_libraries(myrpc
    ${PROTOBUF_LIBRARIES}
    muduo_net
    muduo_base
    zookeeper_mt
    pthread
    glog
)

# =========================
# 10. 把 conf 复制到 build 目录
# =========================
file(COPY ${CMAKE_SOURCE_DIR}/conf
     DESTINATION ${CMAKE_BINARY_DIR})

# =========================
# 11. 示例程序
# =========================
add_executable(server example/server.cpp)
add_executable(client example/client.cpp)

add_dependencies(server generate_proto)
add_dependencies(client generate_proto)

target_link_libraries(server myrpc)
target_link_libraries(client myrpc)
