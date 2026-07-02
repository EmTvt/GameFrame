// HttpResponse: 待发送的 HTTP 响应
//
// 构造好后调 append_to_buffer() 序列化成文本追加到 Buffer/string，
// 再由 Connection::send() 发出。

#pragma once

#include <string>
#include <unordered_map>

namespace epoll_proj {

class HttpResponse {
public:
    enum class StatusCode {
        k200Ok = 200,
        k301MovedPermanently = 301,
        k400BadRequest = 400,
        k404NotFound = 404,
        k500InternalError = 500,
    };

    void set_status_code(StatusCode code) { status_code_ = code; }
    void set_status_message(std::string_view msg) { status_message_ = msg; }
    void set_close_connection(bool on) { close_connection_ = on; }
    bool close_connection() const { return close_connection_; }

    void set_content_type(std::string_view type) { headers_["Content-Type"] = type; }

    void add_header(std::string_view key, std::string_view value) {
        headers_[std::string(key)] = std::string(value);
    }

    void set_body(std::string body) { body_ = std::move(body); }

    // 序列化为 HTTP 响应文本
    std::string serialize() const {
        std::string result;
        // 预估大小避免频繁 realloc
        result.reserve(256 + body_.size());

        // Status line
        result += "HTTP/1.1 ";
        result += std::to_string(static_cast<int>(status_code_));
        result += ' ';
        result += status_message_;
        result += "\r\n";

        // Headers
        if (close_connection_) {
            result += "Connection: close\r\n";
        } else {
            result += "Connection: keep-alive\r\n";
        }

        result += "Content-Length: ";
        result += std::to_string(body_.size());
        result += "\r\n";

        for (const auto& [key, value] : headers_) {
            result += key;
            result += ": ";
            result += value;
            result += "\r\n";
        }

        // 空行 + body
        result += "\r\n";
        result += body_;

        return result;
    }

private:
    StatusCode status_code_ = StatusCode::k200Ok;
    std::string status_message_ = "OK";
    bool close_connection_ = false;
    std::unordered_map<std::string, std::string> headers_;
    std::string body_;
};

}  // namespace epoll_proj
