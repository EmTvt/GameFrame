cmake_minimum_required(VERSION 3.10)

# 项目名
project(epoll_proj LANGUAGES CXX)

# C++ 标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 默认 Debug 构建（带调试符号），方便 VSCode 调试
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# 导出 compile_commands.json，便于 clangd / IntelliSense 使用
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 编译选项
add_compile_options(-Wall -Wextra)

# 网络库源文件集合：避免在每个可执行目标里重复列举。
# 仍故意不抽 static lib —— 目标数量还少，object 重编也快；等到第二个可执行 demo
# 想用部分文件再考虑 OBJECT library。
set(EPOLL_NET_SOURCES
    src/channel.cpp
    src/connection.cpp
    src/event_loop.cpp
    src/event_loop_thread.cpp
    src/event_loop_thread_pool.cpp
    src/server.cpp
    src/tcp_client.cpp
    src/timer_queue.cpp
)

# log_sender/ 源文件：业务进程的日志发送器（用 TcpClient + MPSCQueue 拼出来）。
# 故意不并入 EPOLL_NET_SOURCES：log_sender 是 net 之上的应用层组件，依赖方向是
# log_sender → net → util。给它独立的变量，让"谁能不带这个 .cpp 编"一眼可见
# （例如 test_event_loop / test_mpsc_queue 就不需要它，避免不必要的编译耦合）。
set(LOG_SENDER_SOURCES
    log_sender/log_sender.cpp
)

# 可执行目标
add_executable(epoll_proj
    main.cpp
    ${EPOLL_NET_SOURCES}
    ${LOG_SENDER_SOURCES}
)

# 头文件搜索路径（让 #include "src/xxx.h" 和 #include "xxx.h" 都能工作）
target_include_directories(epoll_proj PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# 链接 pthread（EventLoop 用了 std::thread / std::mutex；Linux 上需要 pthread）
find_package(Threads REQUIRED)
target_link_libraries(epoll_proj PRIVATE Threads::Threads)

# ---- 测试：EventLoop 跨线程能力 ----
add_executable(test_event_loop
    test_event_loop.cpp
    src/channel.cpp
    src/event_loop.cpp
    src/timer_queue.cpp
)
target_include_directories(test_event_loop PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(test_event_loop PRIVATE Threads::Threads)

# ---- 测试：MPSCQueue 多生产者单消费者队列 ----
# 纯头文件实现，只链 pthread；故意不依赖 src/ 任何 .cpp，保证测试隔离。
add_executable(test_mpsc_queue
    test_mpsc_queue.cpp
)
target_include_directories(test_mpsc_queue PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(test_mpsc_queue PRIVATE Threads::Threads)

# ---- log_server：独立的日志收集进程 ----
# 复用 EPOLL_NET_SOURCES（见上）。
add_executable(log_server
    log_server/main.cpp
    log_server/log_file.cpp
    ${EPOLL_NET_SOURCES}
)
target_include_directories(log_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(log_server PRIVATE Threads::Threads)

# ---- 测试：TcpClient 非阻塞 connect + 指数退避重连 ----
# 跑前先启动 log_server（默认端口 9099）。本程序连过去发若干 length-prefix 帧，
# 再演示一次断线重连。详细使用方式见 test_tcp_client.cpp 头注释。
add_executable(test_tcp_client
    test_tcp_client.cpp
    ${EPOLL_NET_SOURCES}
)
target_include_directories(test_tcp_client PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(test_tcp_client PRIVATE Threads::Threads)

add_executable(test_mpsc_queue_tsan test_mpsc_queue.cpp)
target_compile_options(test_mpsc_queue_tsan PRIVATE -fsanitize=thread -O1 -g)
target_link_options   (test_mpsc_queue_tsan PRIVATE -fsanitize=thread)

# ---- 测试：LogSender 多生产者并发 push + 真发到 log_server ----
# 跑前先启动 log_server（默认端口 9099）。详细用法见 test_log_sender.cpp 头注释。
add_executable(test_log_sender
    test_log_sender.cpp
    ${EPOLL_NET_SOURCES}
    ${LOG_SENDER_SOURCES}
)
target_include_directories(test_log_sender PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(test_log_sender PRIVATE Threads::Threads)

# ---- 测试：LOG_* 宏端到端（自包含，进程内 mini sink，无需外部 log_server）----
# 直接 ./build/test_logging 一键跑，返回码即结论。详见 test_logging.cpp 头注释。
add_executable(test_logging
    test_logging.cpp
    ${EPOLL_NET_SOURCES}
    ${LOG_SENDER_SOURCES}
)
target_include_directories(test_logging PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(test_logging PRIVATE Threads::Threads)

# ---- HTTP Server demo ----
add_executable(http_server
    http/http_demo_main.cpp
    http/http_server.cpp
    ${EPOLL_NET_SOURCES}
)
target_include_directories(http_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(http_server PRIVATE Threads::Threads)
