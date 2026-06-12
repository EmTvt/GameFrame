// Buffer: 简单的字节缓冲区，读写共用
//
// 内存布局：
//   +-------------------+----------------+----------------+
//   |    prependable    |   readable     |    writable    |
//   +-------------------+----------------+----------------+
//   0          <=  reader_index_  <=  writer_index_  <=  size()
//
// - prependable：可在数据前面预留空间（暂不用，预留给后续协议头需求）
// - readable   ：已写入但未被消费的数据，业务从这里读
// - writable   ：可写入数据的空闲区
//
// 当 writable 不够时，会先尝试通过把 readable 数据前移腾空间；
// 还不够再 resize 扩容。

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace epoll_proj {

class Buffer {
public:
    static constexpr size_t kInitialSize = 1024;
    static constexpr size_t kPrependSize = 8;  // 预留头部空间

    Buffer()
        : data_(kPrependSize + kInitialSize),
          reader_index_(kPrependSize),
          writer_index_(kPrependSize) {}

    // 当前可读字节数
    size_t readable_bytes() const { return writer_index_ - reader_index_; }
    // 当前可写字节数
    size_t writable_bytes() const { return data_.size() - writer_index_; }
    // 头部可前置字节数
    size_t prependable_bytes() const { return reader_index_; }

    bool empty() const { return readable_bytes() == 0; }

    // 指向可读数据起始位置的指针
    const char* peek() const { return data_.data() + reader_index_; }

    // 把可读数据作为 string_view 暴露（不拷贝）
    std::string_view readable_view() const {
        return std::string_view(peek(), readable_bytes());
    }

    // 写入数据
    void append(const char* data, size_t len) {
        ensure_writable(len);
        std::memcpy(begin_write(), data, len);
        writer_index_ += len;
    }
    void append(std::string_view sv) { append(sv.data(), sv.size()); }

    // 业务消费 len 个字节
    void retrieve(size_t len) {
        if (len >= readable_bytes()) {
            retrieve_all();
            return;
        }
        reader_index_ += len;
    }

    // 把所有可读数据作为 string 取出（消费掉）
    std::string retrieve_all_as_string() {
        std::string s(peek(), readable_bytes());
        retrieve_all();
        return s;
    }

    // 清空（仅重置游标，不释放内存）
    void retrieve_all() {
        reader_index_ = kPrependSize;
        writer_index_ = kPrependSize;
    }

    // 给外部（如 read 系统调用）写入用的指针
    char* begin_write() { return data_.data() + writer_index_; }

    // 通知 Buffer：外部已经往 begin_write() 处写了 len 字节
    void has_written(size_t len) { writer_index_ += len; }

    // 确保至少有 len 字节可写空间
    void ensure_writable(size_t len) {
        if (writable_bytes() >= len) return;
        // 如果把已读数据"压缩"掉就够了，则前移；否则扩容
        if (writable_bytes() + prependable_bytes() >= len + kPrependSize) {
            size_t readable = readable_bytes();
            std::memmove(data_.data() + kPrependSize,
                         data_.data() + reader_index_,
                         readable);
            reader_index_ = kPrependSize;
            writer_index_ = reader_index_ + readable;
        } else {
            data_.resize(writer_index_ + len);
        }
    }

private:
    std::vector<char> data_;
    size_t reader_index_;
    size_t writer_index_;
};

}  // namespace epoll_proj
