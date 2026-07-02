// HttpContext: HTTP/1.1 请求解析状态机
//
// 每条连接通过 Connection::set_context<HttpContext>() 持有一份。
// 跨多次 EPOLLIN 续解析（半包友好）：喂 Buffer&，返回是否需要更多数据。
//
// 状态机：kExpectRequestLine → kExpectHeaders → kGotAll
// （第一版不解析 body，GET 为主）

#pragma once

#include "http_request.h"
#include "util/buffer.h"

#include <string_view>

namespace epoll_proj {

class HttpContext {
public:
    enum class ParseState {
        kExpectRequestLine,
        kExpectHeaders,
        kGotAll,
    };

    ParseState state() const { return state_; }
    bool got_all() const { return state_ == ParseState::kGotAll; }

    HttpRequest& request() { return request_; }
    const HttpRequest& request() const { return request_; }

    // 喂数据进来解析。返回 false 表示解析出错（格式非法），调用方应关闭连接。
    // 返回 true 时检查 got_all() 判断是否完整解析了一个请求。
    bool parse(Buffer& buf) {
        bool ok = true;
        while (ok) {
            if (state_ == ParseState::kExpectRequestLine) {
                // 找 \r\n
                const char* crlf = buf.find_crlf();
                if (!crlf) break;  // 半包，等更多数据
                ok = parse_request_line(
                    std::string_view(buf.peek(), static_cast<size_t>(crlf - buf.peek())));
                if (ok) {
                    buf.retrieve_until(crlf + 2);  // 跳过 \r\n
                    state_ = ParseState::kExpectHeaders;
                }
            } else if (state_ == ParseState::kExpectHeaders) {
                const char* crlf = buf.find_crlf();
                if (!crlf) break;  // 半包
                std::string_view line(buf.peek(), static_cast<size_t>(crlf - buf.peek()));
                if (line.empty()) {
                    // 空行：headers 结束
                    state_ = ParseState::kGotAll;
                    buf.retrieve_until(crlf + 2);
                    break;
                }
                ok = parse_header(line);
                if (ok) {
                    buf.retrieve_until(crlf + 2);
                }
            } else {
                // kGotAll
                break;
            }
        }
        return ok;
    }

    // 复位状态机，用于 keep-alive 下解析下一个请求
    void reset() {
        state_ = ParseState::kExpectRequestLine;
        request_.reset();
    }

private:
    // 解析 "GET /path?query HTTP/1.1"
    bool parse_request_line(std::string_view line) {
        // method
        auto sp1 = line.find(' ');
        if (sp1 == std::string_view::npos) return false;
        request_.set_method(line.substr(0, sp1));
        if (request_.method() == HttpRequest::Method::kInvalid) return false;

        // URI
        auto sp2 = line.find(' ', sp1 + 1);
        if (sp2 == std::string_view::npos) return false;
        auto uri = line.substr(sp1 + 1, sp2 - sp1 - 1);

        // 拆 path?query
        auto q = uri.find('?');
        if (q != std::string_view::npos) {
            request_.set_path(uri.substr(0, q));
            request_.set_query(uri.substr(q + 1));
        } else {
            request_.set_path(uri);
        }

        // version
        auto ver = line.substr(sp2 + 1);
        if (ver == "HTTP/1.1") {
            request_.set_version(HttpRequest::Version::kHttp11);
        } else if (ver == "HTTP/1.0") {
            request_.set_version(HttpRequest::Version::kHttp10);
        } else {
            return false;
        }
        return true;
    }

    // 解析 "Key: Value"
    bool parse_header(std::string_view line) {
        auto colon = line.find(':');
        if (colon == std::string_view::npos) return false;
        auto key = line.substr(0, colon);
        // 跳过冒号后的空格
        auto value_start = colon + 1;
        while (value_start < line.size() && line[value_start] == ' ') ++value_start;
        auto value = line.substr(value_start);
        request_.add_header(key, value);
        return true;
    }

    ParseState state_ = ParseState::kExpectRequestLine;
    HttpRequest request_;
};

}  // namespace epoll_proj
