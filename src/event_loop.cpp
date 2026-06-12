#include "event_loop.h"

#include <sys/epoll.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>

namespace epoll_proj {

namespace {

constexpr int kMaxEvents = 64;

[[noreturn]] void die(const char* msg) {
    std::cerr << msg << " failed: " << std::strerror(errno) << std::endl;
    std::exit(EXIT_FAILURE);
}

}  // namespace

EventLoop::EventLoop() {
    epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0) die("epoll_create1");
}

EventLoop::~EventLoop() {
    // 只关 epfd；handlers_ 里登记的业务 fd 由各自的所有者（Connection / TcpServer）负责
    if (epfd_ >= 0) ::close(epfd_);
}

void EventLoop::add_fd(int fd, uint32_t events, EventCallback cb) {
    // 防御性检查：重复注册说明上层逻辑出 bug 了，宁可早暴露也不静默覆盖
    if (handlers_.count(fd)) {
        std::cerr << "[loop] add_fd: fd=" << fd << " already registered, ignored" << std::endl;
        return;
    }

    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        std::cerr << "[loop] epoll_ctl ADD fd=" << fd << " failed: "
                  << std::strerror(errno) << std::endl;
        return;
    }
    handlers_.emplace(fd, std::move(cb));
}

void EventLoop::mod_fd(int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        std::cerr << "[loop] epoll_ctl MOD fd=" << fd << " failed: "
                  << std::strerror(errno) << std::endl;
    }
}

void EventLoop::del_fd(int fd) {
    // 先从内部 map 移除回调，再 DEL；顺序原因：
    //   万一 DEL 后还有同批 epoll_wait 返回的事件指向这个 fd，
    //   下面 loop() 里查 handlers_ 找不到就会直接跳过，不会触发悬空回调。
    handlers_.erase(fd);

    // DEL 失败通常意味着 fd 已被 ::close 过（内核会自动从 epoll 移除）；
    // 这是 resource-safety.md 第 5 节描述的"反顺序但内核容忍"的妥协，
    // 所以这里只打调试日志、不视为错误。
    if (::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr) < 0 && errno != EBADF && errno != ENOENT) {
        std::cerr << "[loop] epoll_ctl DEL fd=" << fd << " failed: "
                  << std::strerror(errno) << std::endl;
    }
}

void EventLoop::loop() {
    epoll_event events[kMaxEvents];
    while (!quit_) {
        int nready = ::epoll_wait(epfd_, events, kMaxEvents, -1);
        if (nready < 0) {
            if (errno == EINTR) continue;
            die("epoll_wait");
        }

        for (int i = 0; i < nready; ++i) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            // 关键防御：本轮前面的某个回调可能已经把 fd 从 loop 里 del_fd 掉了
            // （例如 EPOLLIN 回调里关连接），后面如果还有事件指向这个 fd，找不到回调就跳过。
            // 等价于 resource-safety.md 第 4 节"二次检查 connections_.count(fd)"的下沉。
            auto it = handlers_.find(fd);
            if (it == handlers_.end()) continue;

            // 拷贝 cb 引用调用：调用过程中回调可能 del_fd(fd) 让 it 失效，
            // 这里用引用就够 —— 关键是不要在调用 cb 后还用 it。
            it->second(ev);
        }
    }
}

}  // namespace epoll_proj
