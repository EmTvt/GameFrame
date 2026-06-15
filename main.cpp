// 入口：演示如何使用 TcpServer + Connection + Buffer 抽象搭建 echo 服务
//
// 测试方式：
//   终端1：./build/epoll_proj
//   终端2/3/...：telnet 127.0.0.1 8888
//   输入任意文字回车，服务端原样回显，多个客户端互不干扰
//
// 注意业务回调签名变化（第二步起）：
//   void(const ConnectionPtr& conn, Buffer& input)
//   业务负责从 input 缓冲区里"按协议"取走完整消息；这里 echo 服务直接全部取走。
//   传 shared_ptr 是为了业务能安全地把 conn 存到 session map / 定时器 / 协程帧里。

#include <iostream>
#include <thread>

#include "src/buffer.h"
#include "src/connection.h"
#include "src/server.h"

int main() {
    constexpr uint16_t kPort = 8888;
    constexpr size_t   kIoThreads = 4;   // 0 = 单 Reactor；>0 = 主从 Reactor

    epoll_proj::TcpServer server(kPort, kIoThreads);

    server.set_message_callback(
        [](const epoll_proj::ConnectionPtr& conn, epoll_proj::Buffer& input) {
            // 简单 echo：把当前缓冲区里所有数据取走并写回
            std::string data = input.retrieve_all_as_string();

            // 打印当前回调跑在哪个线程，直观验证 IO 已经分散到 subLoop 上
            std::cout << "[app tid=" << std::this_thread::get_id() << "] fd="
                      << conn->fd() << " (" << conn->peer() << ") recv "
                      << data.size() << " bytes: ";
            std::cout.write(data.data(), static_cast<std::streamsize>(data.size()));
            std::cout.flush();

            conn->send(data);
        });

    server.run();
    return 0;
}
