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

# 可执行目标
add_executable(epoll_proj
    main.cpp
    src/channel.cpp
    src/connection.cpp
    src/event_loop.cpp
    src/event_loop_thread.cpp
    src/event_loop_thread_pool.cpp
    src/server.cpp
    src/timer_queue.cpp
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
# 故意复用 src/ 的网络库源文件而不是抽成 static lib：
#   - 现阶段就一个 epoll_proj + 一个 log_server，抽 lib 是过度设计
#   - 等第 3 步加入 TcpClient 时如果发现源文件列表越来越长，再统一抽 epoll_net 静态库
add_executable(log_server
    log_server/main.cpp
    log_server/log_file.cpp
    src/channel.cpp
    src/connection.cpp
    src/event_loop.cpp
    src/event_loop_thread.cpp
    src/event_loop_thread_pool.cpp
    src/server.cpp
    src/timer_queue.cpp
)
target_include_directories(log_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(log_server PRIVATE Threads::Threads)

add_executable(test_mpsc_queue_tsan test_mpsc_queue.cpp)
target_compile_options(test_mpsc_queue_tsan PRIVATE -fsanitize=thread -O1 -g)
target_link_options   (test_mpsc_queue_tsan PRIVATE -fsanitize=thread)
