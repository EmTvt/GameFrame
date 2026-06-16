// LogFile：日志文件落盘的最小封装
//
// 设计原则（log_server 第一步）：
//   - 只关心"把一段 bytes 写到磁盘"，不关心协议、不关心网络
//   - 调用方负责传"一条完整日志的 payload"，本类不再做分帧
//   - 支持按文件大小 + 按日期滚动；同一天内用递增序号区分
//   - 提供显式 flush()：log_server 的 EventLoop 可以挂个定时器周期 flush
//
// 文件命名约定：
//   basedir/basename.YYYYMMDD.log.N
//   例：res/log/epoll_proj.20260616.log.1
//   - 同一天写满 roll_size 后切到 .2、.3 ……
//   - 跨日（系统日期变化）后第一次写时重置回 .1
//   - 进程重启会复用当天已存在的最大序号文件（若未满）或开新序号
//
// 线程模型：
//   - 假定调用方串行调用（log_server 单线程 EventLoop 天然满足）
//   - 因此内部不加锁；若以后引入多线程写，再考虑互斥
//
// 故意不做：
//   - 不做按"小时"等更细粒度的时间滚动：日粒度 + size 粒度足够覆盖常见运维需求
//   - 不做压缩 / 异步落盘 / mmap：留给后续扩展点
//   - 不做 logrotate 那种把 .1→.2、.2→.3 整体重命名：序号永远只增不减，rotate 简单且无竞争

#pragma once

#include <cstddef>
#include <cstdio>
#include <string>
#include <string_view>

namespace log_server {

class LogFile {
public:
    // basedir:    日志目录，不存在时构造时会自动递归创建（mkdir -p 语义）
    // basename:   文件名前缀，例如 "epoll_proj"
    // roll_size:  单文件大小超过这个值就滚动新文件，单位字节
    LogFile(std::string basedir, std::string basename, std::size_t roll_size);
    ~LogFile();

    LogFile(const LogFile&) = delete;
    LogFile& operator=(const LogFile&) = delete;

    // 追加写一条日志 payload。调用方应保证 data 是完整一条（含/不含换行皆可，本类不加）。
    // 失败时仅打到 stderr，不抛异常 —— 日志系统自身不应让上层崩溃。
    // 注意：这是"裸写"接口，不加时间戳、不加换行；通常用于写分隔符或调用方自己已经组装好整行。
    void append(std::string_view data);

    // 写一条带时间戳前缀 + 末尾换行的日志行，格式：
    //   [YYYY-MM-DD HH:MM:SS.mmm] <payload>\n
    // 这是日常落盘的推荐接口：调用方只关心业务 payload，时间戳由日志组件统一打。
    // 时间用 localtime（本地时区）+ 毫秒，便于压测时区分同一秒内的多条记录。
    void append_line(std::string_view payload);

    // 把 FILE* 的用户态缓冲冲到内核（不调 fsync，避免吃 IOPS）
    void flush();

    std::size_t written_bytes() const { return written_bytes_; }

private:
    // 按需滚动：若当天文件不存在 / 当前文件超 roll_size / 已经跨日，就开下一个文件。
    // 仅在 append 前调用，幂等。
    void roll_if_needed();

    // 实际打开新文件：根据 current_date_ + current_seq_ 拼路径，open，更新 written_bytes_
    void open_file();

    // 构造形如 basedir/basename.YYYYMMDD.log.N 的路径
    std::string make_filename(const std::string& date_str, int seq) const;

    // 扫 basedir 下所有匹配 basename.<date_str>.log.<N> 的文件，返回当天最大 N（不存在返回 0）
    int scan_today_max_seq(const std::string& date_str) const;

    // 当前本地日期 YYYYMMDD
    static std::string today_str();

    std::string basedir_;
    std::string basename_;
    std::size_t roll_size_;

    std::FILE* fp_ = nullptr;
    std::size_t written_bytes_ = 0;   // 当前文件已写字节数（含进程启动前复用文件的历史 size）

    std::string current_date_;        // 当前文件对应的 YYYYMMDD；跨日时与 today_str() 不等 → 触发滚动
    int current_seq_ = 0;             // 当前文件序号，1 起
};

}  // namespace log_server
