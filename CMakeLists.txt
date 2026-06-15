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
)
target_include_directories(test_event_loop PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(test_event_loop PRIVATE Threads::Threads)
