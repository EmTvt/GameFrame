// HttpRequest: 解析完成的一个 HTTP 请求
//
// 只支持 GET（第一版），body/chunked 后续再加。
// 存储解析结果，供 HttpServer 的路由回调使用。

#pragma once

#include <string>
#include <unordered_map>

namespace epoll_proj {

class HttpRequest {
public:
    enum class Method { kInvalid, kGet, kPost, kHead, kPut, kDelete };
    enum class Version { kUnknown, kHttp10, kHttp11 };

    void set_method(std::string_view m) {
        if (m == "GET")         method_ = Method::kGet;
        else if (m == "POST")   method_ = Method::kPost;
        else if (m == "HEAD")   method_ = Method::kHead;
        else if (m == "PUT")    method_ = Method::kPut;
        else if (m == "DELETE") method_ = Method::kDelete;
        else                    method_ = Method::kInvalid;
    }

    Method method() const { return method_; }

    const char* method_string() const {
        switch (method_) {
            case Method::kGet:    return "GET";
            case Method::kPost:   return "POST";
            case Method::kHead:   return "HEAD";
            case Method::kPut:    return "PUT";
            case Method::kDelete: return "DELETE";
            default:              return "INVALID";
        }
    }

    void set_path(std::string_view p) { path_ = p; }
    const std::string& path() const { return path_; }

    void set_query(std::string_view q) { query_ = q; }
    const std::string& query() const { return query_; }

    void set_version(Version v) { version_ = v; }
    Version version() const { return version_; }

    void add_header(std::string_view key, std::string_view value) {
        // 存储时 key 转小写，方便查找
        std::string k(key);
        for (auto& c : k) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        headers_[std::move(k)] = std::string(value);
    }

    std::string get_header(const std::string& key) const {
        auto it = headers_.find(key);
        return it != headers_.end() ? it->second : "";
    }

    const std::unordered_map<std::string, std::string>& headers() const { return headers_; }

    bool keep_alive() const {
        auto conn = get_header("connection");
        if (version_ == Version::kHttp11) {
            // HTTP/1.1 默认 keep-alive，除非显式 close
            return conn != "close";
        }
        // HTTP/1.0 默认关，除非显式 keep-alive
        return conn == "keep-alive";
    }

    void reset() {
        method_ = Method::kInvalid;
        version_ = Version::kUnknown;
        path_.clear();
        query_.clear();
        headers_.clear();
    }

private:
    Method method_ = Method::kInvalid;
    Version version_ = Version::kUnknown;
    std::string path_;
    std::string query_;
    std::unordered_map<std::string, std::string> headers_;
};

}  // namespace epoll_proj
