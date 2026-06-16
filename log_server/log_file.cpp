#include "log_file.h"

#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>

namespace log_server {

LogFile::LogFile(std::string basedir, std::string basename, std::size_t roll_size)
    : basedir_(std::move(basedir)),
      basename_(std::move(basename)),
      roll_size_(roll_size) {
    // mkdir -p basedir_：不存在则递归建；已存在返回 false 不报错
    // 用 error_code 重载避免抛异常 —— 日志组件自身不该让上层挂掉
    std::error_code ec;
    std::filesystem::create_directories(basedir_, ec);
    if (ec) {
        std::fprintf(stderr,
                     "[log_server] LogFile: create_directories(%s) failed: %s\n",
                     basedir_.c_str(), ec.message().c_str());
        // 不 return：让后续 fopen 自然失败并打印更具体的报错
    }
    // 启动即打开当天的文件（按需选 seq）
    roll_if_needed();
}

LogFile::~LogFile() {
    if (fp_) {
        std::fflush(fp_);
        std::fclose(fp_);
    }
}

void LogFile::append(std::string_view data) {
    // 写之前判滚动：覆盖三种情况
    //   1) 启动后第一次写（roll_if_needed 已在构造里调过，这里通常 no-op）
    //   2) 当前文件 size 超过 roll_size → seq++
    //   3) 跨日 → seq 重置回 1，date 换新
    roll_if_needed();

    if (!fp_) {
        return;
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

void LogFile::append_line(std::string_view payload) {
    // 拼一行：[YYYY-MM-DD HH:MM:SS.mmm] <payload>\n
    // 取系统时钟，拆出秒级 time_t 喂给 localtime_r，再单独算出毫秒分量。
    // 用栈上 char[] + snprintf 避免每行都过 ostringstream 的开销（压测时这是热路径）。
    const auto now = std::chrono::system_clock::now();
    const auto secs = std::chrono::time_point_cast<std::chrono::seconds>(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - secs).count();

    const std::time_t tt = std::chrono::system_clock::to_time_t(secs);
    std::tm tm_buf{};
    ::localtime_r(&tt, &tm_buf);

    // "[2026-06-16 19:48:00.123] " = 27 字节，留 32 足够
    char prefix[32];
    const int plen = std::snprintf(prefix, sizeof(prefix),
                                   "[%04d-%02d-%02d %02d:%02d:%02d.%03d] ",
                                   tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                                   tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec,
                                   static_cast<int>(ms));
    if (plen > 0) {
        append(std::string_view(prefix, static_cast<std::size_t>(plen)));
    }
    append(payload);
    append("\n");
}

void LogFile::flush() {
    if (fp_) std::fflush(fp_);
}

void LogFile::roll_if_needed() {
    const std::string today = today_str();

    // 1) 跨日：直接换新文件，seq 从 1 起
    if (!current_date_.empty() && today != current_date_) {
        current_date_ = today;
        current_seq_ = 1;
        open_file();
        return;
    }

    // 2) 当前文件已超阈值：seq++
    if (fp_ && written_bytes_ >= roll_size_) {
        current_seq_++;
        open_file();
        return;
    }

    // 3) 首次进入（构造里调用）：决定 date + seq
    //    复用当天最大序号文件，若它已满则用 max+1；当天无文件则从 1 起
    if (!fp_) {
        current_date_ = today;
        const int max_seq = scan_today_max_seq(today);
        if (max_seq == 0) {
            current_seq_ = 1;
        } else {
            // 看最大序号文件的当前 size：未满则继续写，满了则 +1
            const std::string candidate = make_filename(today, max_seq);
            std::error_code ec;
            const auto sz = std::filesystem::file_size(candidate, ec);
            if (!ec && sz < roll_size_) {
                current_seq_ = max_seq;        // 续写
            } else {
                current_seq_ = max_seq + 1;    // 开新
            }
        }
        open_file();
    }
}

void LogFile::open_file() {
    if (fp_) {
        std::fflush(fp_);
        std::fclose(fp_);
        fp_ = nullptr;
    }

    const std::string path = make_filename(current_date_, current_seq_);
    fp_ = std::fopen(path.c_str(), "ae");   // a: append; e: O_CLOEXEC
    if (!fp_) {
        std::fprintf(stderr, "[log_server] LogFile open failed: %s\n", path.c_str());
        written_bytes_ = 0;
        return;
    }

    // 续写场景：把 written_bytes_ 初始化为文件当前 size，避免刚 open 就立刻又被判为已满
    // （或反过来：明明已满却又写好一会才滚动）
    std::error_code ec;
    const auto sz = std::filesystem::file_size(path, ec);
    written_bytes_ = ec ? 0 : static_cast<std::size_t>(sz);

    std::fprintf(stderr, "[log_server] LogFile rolled to: %s (start size=%zu)\n",
                 path.c_str(), written_bytes_);
}

std::string LogFile::make_filename(const std::string& date_str, int seq) const {
    // basedir/basename.YYYYMMDD.log.N
    std::ostringstream oss;
    oss << basedir_ << '/' << basename_ << '.' << date_str << ".log." << seq;
    return oss.str();
}

int LogFile::scan_today_max_seq(const std::string& date_str) const {
    // 前缀形如 "epoll_proj.20260616.log."
    const std::string prefix = basename_ + '.' + date_str + ".log.";

    int max_seq = 0;
    std::error_code ec;
    std::filesystem::directory_iterator it(basedir_, ec);
    if (ec) return 0;   // 目录读不到就当今天没文件，让外层从 1 起

    for (const auto& entry : it) {
        if (!entry.is_regular_file()) continue;
        const std::string name = entry.path().filename().string();
        if (name.size() <= prefix.size()) continue;
        if (name.compare(0, prefix.size(), prefix) != 0) continue;

        // 解析后缀部分作为整数；非数字 / 越界则跳过（容忍人为放进来的奇怪文件）
        const std::string tail = name.substr(prefix.size());
        if (tail.empty()) continue;
        char* end = nullptr;
        const long v = std::strtol(tail.c_str(), &end, 10);
        if (end == tail.c_str() || *end != '\0') continue;   // 不是纯数字
        if (v <= 0 || v > 1'000'000) continue;               // 防御异常值
        if (static_cast<int>(v) > max_seq) max_seq = static_cast<int>(v);
    }
    return max_seq;
}

std::string LogFile::today_str() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t tt = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf{};
    ::localtime_r(&tt, &tm_buf);

    std::ostringstream oss;
    oss << std::put_time(&tm_buf, "%Y%m%d");
    return oss.str();
}

}  // namespace log_server
