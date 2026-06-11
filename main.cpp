// 入口：演示如何使用 TcpServer + Connection 抽象搭建一个 echo 服务
//
// 测试方式：
//   终端1：./build/epoll_proj
//   终端2/3/...：telnet 127.0.0.1 8888
//   输入任意文字回车，服务端原样回显，多个客户端互不干扰

#include <iostream>
#include <string_view>

#include "src/connection.h"
#include "src/server.h"

int main() {
    constexpr uint16_t kPort = 8888;

    epoll_proj::TcpServer server(kPort);

    // 注入业务回调：收到数据就原样回写（echo）
    server.set_message_callback(
        [](epoll_proj::Connection& conn, std::string_view data) {
            std::cout << "[app] fd=" << conn.fd()
                      << " (" << conn.peer() << ") recv " << data.size() << " bytes: ";
            std::cout.write(data.data(), static_cast<std::streamsize>(data.size()));
            std::cout.flush();

            conn.send(data);
        });

    server.run();
    return 0;
}
