// HttpServer: 基于 TcpServer 的轻量 HTTP/1.1 服务器
//
// 职责：
//   - 内部包一个 TcpServer
//   - 每条连接挂 HttpContext（通过 set_context）
//   - message_cb 里喂 Buffer 给 HttpContext 解析，解析完一个请求就回调业务
//   - 支持 keep-alive（解析完一个请求后 reset 状态机等下一个）
//
// 使用方式：
//   HttpServer server(8080, 4);  // 4 个 IO 线程
//   server.set_http_callback([](const HttpRequest& req, HttpResponse* resp) {
//       if (req.path() == "/hello") { resp->set_body("world"); }
//       else { resp->set_status_code(HttpResponse::StatusCode::k404NotFound); ... }
//   });
//   server.run();  // 阻塞

#pragma once

#include <cstdint>
#include <functional>

#include "http_request.h"
#include "http_response.h"
#include "src/connection.h"
#include "src/server.h"
#include "util/buffer.h"

namespace epoll_proj {

class HttpServer {
public:
    using HttpCallback = std::function<void(const HttpRequest&, HttpResponse*)>;

    // num_threads: IO 线程数，0 表示单 Reactor
    HttpServer(uint16_t port, int num_threads = 0);

    void set_http_callback(HttpCallback cb) { http_cb_ = std::move(cb); }

    // 启动监听 + 进入事件循环（阻塞）
    void run();

private:
    void on_connection(const ConnectionPtr& conn);
    void on_message(const ConnectionPtr& conn, Buffer& buf);
    void on_request(const ConnectionPtr& conn, const HttpRequest& req);

    TcpServer server_;
    HttpCallback http_cb_;
};

}  // namespace epoll_proj
