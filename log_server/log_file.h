// LogFile：日志文件落盘的最小封装
//
// 设计原则（log_server 第一步）：
//   - 只关心"把一段 bytes 写到磁盘"，不关心协议、不关心网络
//   - 调用方负责传"一条完整日志的 payload"，本类不再做分帧
//   - 支持按文件大小滚动（rollover），文件名带时间戳避免覆盖
//   - 提供显式 flush()：log_server 的 EventLoop 可以挂个定时器周期 flush
//
// 线程模型：
//   - 假定调用方串行调用（log_server 单线程 EventLoop 天然满足）
//   - 因此内部不加锁；若以后引入多线程写，再考虑互斥
//
// 故意不做：
//   - 不做按时间滚动（先够用，后面想加再加）
//   - 不做压缩 / 异步落盘 / mmap：留给后续扩展点

#pragma once

#include <cstddef>
#include <cstdio>
#include <string>
#include <string_view>

namespace log_server {

class LogFile {
public:
    // basedir:    日志目录，必须已存在（log_server 启动时检查）
    // basename:   文件名前缀，例如 "epoll_proj"
    // roll_size:  单文件大小超过这个值就滚动新文件，单位字节
    LogFile(std::string basedir, std::string basename, std::size_t roll_size);
    ~LogFile();

    LogFile(const LogFile&) = delete;
    LogFile& operator=(const LogFile&) = delete;

    // 追加写一条日志 payload。调用方应保证 data 是完整一条（含/不含换行皆可，本类不加）。
    // 失败时仅打到 stderr，不抛异常 —— 日志系统自身不应让上层崩溃。
    void append(std::string_view data);

    // 把 FILE* 的用户态缓冲冲到内核（不调 fsync，避免吃 IOPS）
    void flush();

    std::size_t written_bytes() const { return written_bytes_; }

private:
    // 关闭当前文件，按当前时间戳开新文件
    void roll_file();

    // 构造形如 basedir/basename.YYYYmmdd-HHMMSS.pid.log 的路径
    std::string make_filename() const;

    std::string basedir_;
    std::string basename_;
    std::size_t roll_size_;

    std::FILE* fp_ = nullptr;
    std::size_t written_bytes_ = 0;   // 当前文件已写字节数，达到 roll_size_ 就滚动
};

}  // namespace log_server
