#include "http/http_server.h"

#include <iostream>
#include <memory>

#include "http/http_context.h"

namespace epoll_proj {

HttpServer::HttpServer(uint16_t port, int num_threads)
    : server_(port, static_cast<size_t>(num_threads)) {
    server_.set_connection_callback(
        [this](const ConnectionPtr& conn) { on_connection(conn); });
    server_.set_message_callback(
        [this](const ConnectionPtr& conn, Buffer& buf) { on_message(conn, buf); });
}

void HttpServer::run() {
    server_.run();
}

void HttpServer::on_connection(const ConnectionPtr& conn) {
    if (conn->connected()) {
        // 每条新连接挂一个解析状态机
        conn->set_context(std::make_shared<HttpContext>());
    }
}

void HttpServer::on_message(const ConnectionPtr& conn, Buffer& buf) {
    auto ctx = conn->context<HttpContext>();
    if (!ctx) {
        conn->close();
        return;
    }

    if (!ctx->parse(buf)) {
        // 解析出错：发 400 然后关
        HttpResponse resp;
        resp.set_status_code(HttpResponse::StatusCode::k400BadRequest);
        resp.set_status_message("Bad Request");
        resp.set_close_connection(true);
        resp.set_body("400 Bad Request\n");
        resp.set_content_type("text/plain");
        conn->send(resp.serialize());
        conn->shutdown();
        return;
    }

    if (ctx->got_all()) {
        on_request(conn, ctx->request());
        // keep-alive：重置状态机等待下一个请求
        ctx->reset();
    }
    // 否则半包，等更多数据（下次 EPOLLIN 继续喂）
}

void HttpServer::on_request(const ConnectionPtr& conn, const HttpRequest& req) {
    std::cout << "[http] " << conn->peer() << " "
              << req.method_string() << " " << req.path();
    if (!req.query().empty()) std::cout << "?" << req.query();
    std::cout << " (keep-alive=" << (req.keep_alive() ? "yes" : "no") << ")"
              << std::endl;

    bool close = !req.keep_alive();

    HttpResponse resp;
    resp.set_close_connection(close);
    resp.set_content_type("text/plain");

    if (http_cb_) {
        http_cb_(req, &resp);
    } else {
        resp.set_status_code(HttpResponse::StatusCode::k404NotFound);
        resp.set_status_message("Not Found");
        resp.set_body("404 Not Found\n");
    }

    conn->send(resp.serialize());

    if (close) {
        conn->shutdown();
    }
}

}  // namespace epoll_proj
