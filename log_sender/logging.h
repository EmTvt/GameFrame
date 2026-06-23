// logging.h: 业务侧日志接口 —— LOG_DEBUG / LOG_INFO / LOG_WARN / LOG_ERROR
//
// 这一层只做两件事，且都极轻：
//   1) 在**调用线程**把 (level, file, line, fmt, ...) 格式化成一条 string
//      —— 故意在调用线程做，避免跨线程传 va_list（va_list 不能安全跨线程/延迟使用）
//   2) 把这条 string 交给全局 LogSender::global()->push()
//      —— push 是纯内存操作（进 MPSCQueue），纳秒级，不阻塞业务
//
// 真正的 drain / encode / send / 落盘 都在 LogSender 线程和 log_server 进程里，
// 与这里无关。宏不碰网络、不碰连接、不碰队列内部。
//
// 为什么经过 LogSender 而不是直接 push 到 MPSCQueue：
//   - MPSCQueue 是 LogSender 的私有成员，外部够不到；
//   - 唤醒消费者（空→非空那次 queue_in_loop 戳醒 sender_loop）需要 loop 引用，
//     这个引用在 LogSender 手里。LogSender::push 把"入队 + 边沿唤醒"封在一起。
//
// 全局可达：LOG_* 通过 LogSender::global() 拿实例。实例由 main 显式构造并
//   set_global() 注册（见 log_sender.h 的设计注释）。未注册时宏静默跳过，不崩。
//
// 时间戳由谁打：log_server 落盘时 append_line 会加 [YYYY-MM-DD HH:MM:SS.mmm] 前缀，
//   所以这里**不再重复打时间戳**，只打级别 + 文件:行号 + 正文，避免两段时间。

#pragma once

#include <cstdarg>
#include <cstdio>
#include <string>

#include "log_sender/log_sender.h"

namespace epoll_proj {

enum class LogLevel : int {
    DEBUG = 0,
    INFO  = 1,
    WARN  = 2,
    ERROR = 3,
};

// 编译期最低级别开关：低于这个级别的 LOG_* 在预处理后会被编译器整段消除（零开销）。
// 想在 release 里关掉 DEBUG/INFO，编译时 -DLOG_COMPILE_LEVEL=2 即可（只留 WARN/ERROR）。
// 默认 0（DEBUG）：全部打开。
#ifndef LOG_COMPILE_LEVEL
#define LOG_COMPILE_LEVEL 0
#endif

namespace detail {

inline const char* level_str(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO";
        case LogLevel::WARN:  return "WARN";
        case LogLevel::ERROR: return "ERROR";
    }
    return "?????";
}

// 取 __FILE__ 的纯文件名部分（去掉目录前缀），让日志行短一点、可读。
inline const char* basename_of(const char* path) {
    const char* base = path;
    for (const char* p = path; *p; ++p) {
        if (*p == '/') base = p + 1;
    }
    return base;
}

}  // namespace detail

// 把一条日志格式化成最终 payload："LEVEL file:line | <body>"
//
// printf 风格：编译器靠 format attribute 帮我们在编译期校验 fmt 与变参是否匹配
//   （传错类型会出 -Wformat 警告，配合项目里的 -Wall -Wextra 直接暴露）。
// 两遍 vsnprintf：第一遍算长度，第二遍填内容，避免猜缓冲区大小。
__attribute__((format(printf, 4, 5)))
inline std::string format_log(LogLevel level, const char* file, int line,
                              const char* fmt, ...) {
    // 1) 先把可变参数部分格式化成 body
    va_list ap;
    va_start(ap, fmt);
    va_list ap2;
    va_copy(ap2, ap);
    const int n = std::vsnprintf(nullptr, 0, fmt, ap);
    va_end(ap);

    std::string body;
    if (n > 0) {
        body.resize(static_cast<std::size_t>(n));
        // 写 n 个字符 + 1 个结尾 '\0'（落在 body[n]，值与 string 自带终止符相同，合法）
        std::vsnprintf(body.data(), static_cast<std::size_t>(n) + 1, fmt, ap2);
    }
    va_end(ap2);

    // 2) 拼前缀。不打时间戳（log_server 落盘时统一加），只给级别 + 定位信息。
    std::string out;
    out.reserve(16 + body.size());
    out += detail::level_str(level);
    out += ' ';
    out += detail::basename_of(file);
    out += ':';
    out += std::to_string(line);
    out += " | ";
    out += body;
    return out;
}

}  // namespace epoll_proj

// ---------------- 宏 ----------------
//
// do-while(0) 惯用法：把多语句包成"必须以分号结尾的单条语句"，在 if/else 等
//   上下文里安全替换；同时给 body 一个独立块作用域。
//
// 级别门控用 `if ((level_num) < LOG_COMPILE_LEVEL) break;`：两个操作数都是编译期
//   常量，编译器会把比较折叠成常量；级别被关时整段 if-true 分支为死代码被消除，
//   达到"关闭的日志零开销"。break 在 do-while(0) 里就是"跳出本宏"，比 goto 干净。
//
// global() 为 nullptr（LogSender 未注册 / 已注销）时同样 break：静默跳过，不崩。
//
// __VA_OPT__(,)：C++20 标准写法。无可变参数时不产生多余逗号，
//   有则补一个逗号 —— 替代 GNU 的 ##__VA_ARGS__ 扩展，符合 -std=c++20。
#define EPOLL_LOG_IMPL(level_enum, level_num, fmt, ...)                  \
    do {                                                                \
        if ((level_num) < LOG_COMPILE_LEVEL) break;                     \
        ::epoll_proj::LogSender* _epoll_log_s =                         \
            ::epoll_proj::LogSender::global();                          \
        if (!_epoll_log_s) break;                                       \
        _epoll_log_s->push(::epoll_proj::format_log(                    \
            level_enum, __FILE__, __LINE__, fmt __VA_OPT__(,) __VA_ARGS__)); \
    } while (0)

#define LOG_DEBUG(fmt, ...) \
    EPOLL_LOG_IMPL(::epoll_proj::LogLevel::DEBUG, 0, fmt __VA_OPT__(,) __VA_ARGS__)
#define LOG_INFO(fmt, ...) \
    EPOLL_LOG_IMPL(::epoll_proj::LogLevel::INFO, 1, fmt __VA_OPT__(,) __VA_ARGS__)
#define LOG_WARN(fmt, ...) \
    EPOLL_LOG_IMPL(::epoll_proj::LogLevel::WARN, 2, fmt __VA_OPT__(,) __VA_ARGS__)
#define LOG_ERROR(fmt, ...) \
    EPOLL_LOG_IMPL(::epoll_proj::LogLevel::ERROR, 3, fmt __VA_OPT__(,) __VA_ARGS__)
