#include "log_file.h"

#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <utility>

namespace log_server {

LogFile::LogFile(std::string basedir, std::string basename, std::size_t roll_size)
    : basedir_(std::move(basedir)),
      basename_(std::move(basename)),
      roll_size_(roll_size) {
    roll_file();   // 启动即开一个新文件
}

LogFile::~LogFile() {
    if (fp_) {
        std::fflush(fp_);
        std::fclose(fp_);
    }
}

void LogFile::append(std::string_view data) {
    if (!fp_) return;

    // 写之前先判滚动 —— 用"写之前判"比"写之后判"更简单：
    // 单条日志可能不大，超出一点点也能容忍
    if (written_bytes_ >= roll_size_) {
        roll_file();
    }

    // 默认全缓冲（写文件时 libc 行为），这里用 fwrite_unlocked 跳过 FILE 内部锁，
    // 因为我们假定单线程调用。fwrite_unlocked 是 POSIX 扩展，不在 std:: 命名空间。
    std::size_t n = ::fwrite_unlocked(data.data(), 1, data.size(), fp_);
    if (n != data.size()) {
        std::fprintf(stderr, "[log_server] LogFile::append short write: %zu/%zu\n",
                     n, data.size());
    }
    written_bytes_ += n;
}

void LogFile::flush() {
    if (fp_) std::fflush(fp_);
}

void LogFile::roll_file() {
    if (fp_) {
        std::fflush(fp_);
        std::fclose(fp_);
        fp_ = nullptr;
    }

    const std::string path = make_filename();
    fp_ = std::fopen(path.c_str(), "ae");   // a: append; e: O_CLOEXEC
    if (!fp_) {
        std::fprintf(stderr, "[log_server] LogFile open failed: %s\n", path.c_str());
        return;
    }
    written_bytes_ = 0;
    std::fprintf(stderr, "[log_server] LogFile rolled to: %s\n", path.c_str());
}

std::string LogFile::make_filename() const {
    // 取本地时间，格式：basedir/basename.YYYYmmdd-HHMMSS.PID.log
    const auto now = std::chrono::system_clock::now();
    const std::time_t tt = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf{};
    ::localtime_r(&tt, &tm_buf);

    std::ostringstream oss;
    oss << basedir_ << '/' << basename_ << '.'
        << std::put_time(&tm_buf, "%Y%m%d-%H%M%S")
        << '.' << ::getpid() << ".log";
    return oss.str();
}

}  // namespace log_server
