// HTTP Demo: 轻量 HTTP/1.1 服务器
//
// 验证 HttpServer + set_context + shutdown/force_close_with_delay 的实际使用。
//
// 路由：
//   GET /         → "Hello, epoll_proj!"
//   GET /hello    → "Hello, World!"
//   其他          → 404
//
// 用法：
//   ./build/http_server [port]       # 默认 8080
//   curl http://127.0.0.1:8080/hello

#include <csignal>
#include <cstdlib>
#include <iostream>

#include "http/http_server.h"

using namespace epoll_proj;

int main(int argc, char* argv[]) {
    uint16_t port = 8080;
    if (argc >= 2) port = static_cast<uint16_t>(std::atoi(argv[1]));

    // 忽略 SIGPIPE（对端关了写会触发）
    ::signal(SIGPIPE, SIG_IGN);

    HttpServer server(port, 2);  // 2 个 IO 线程

    server.set_http_callback([](const HttpRequest& req, HttpResponse* resp) {
        if (req.path() == "/") {
            resp->set_status_code(HttpResponse::StatusCode::k200Ok);
            resp->set_status_message("OK");
            resp->set_body("Hello, epoll_proj!\n");
        } else if (req.path() == "/hello") {
            resp->set_status_code(HttpResponse::StatusCode::k200Ok);
            resp->set_status_message("OK");
            resp->set_body("Hello, World!\n");
        } else {
            resp->set_status_code(HttpResponse::StatusCode::k404NotFound);
            resp->set_status_message("Not Found");
            resp->set_body("404 Not Found\n");
        }
    });

    std::cout << "[http_server] listening on port " << port << std::endl;
    server.run();  // 阻塞，进入事件循环

    return 0;
}
