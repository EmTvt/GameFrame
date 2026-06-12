// EventLoop: 事件循环（第六步预备工作 —— 单线程 Reactor 拆分）
//
// 设计动机：
//   原先 TcpServer 同时承担两件事 ——
//     1) "怎么 accept、怎么管理连接表"（业务逻辑）
//     2) "epoll_create / epoll_wait / epoll_ctl 怎么调"（事件机制）
//   这两件事的变化方向完全不同：业务逻辑稳定，事件机制要为多线程 Reactor 演进。
//   把 (2) 抽到 EventLoop 里，TcpServer 只用它的接口，互不影响。
//
// 本步意图：
//   - 不引入 Channel 抽象（避免改动面爆炸，下一步再拆）
//   - 不引入跨线程唤醒（eventfd / runInLoop 等留给后续）
//   - 行为与拆分前 100% 等价：单线程、阻塞 epoll_wait
//
// 角色边界：
//   EventLoop 持有 epfd 和 "fd → 事件回调" 的映射；不关心业务语义。
//   上层（TcpServer / Connection）通过注册回调把自己挂到 EventLoop 上。
//
// 回调签名为 void(uint32_t events) 的取舍：
//   epoll 的事件位本来就是混合（EPOLLIN | EPOLLOUT | EPOLLERR | ...），
//   让上层自己拆分支比"在 EventLoop 里强行拆成 read_cb / write_cb / err_cb 三个"
//   更直接，也避免在没有 Channel 抽象时一个 fd 注册三份回调的麻烦。
//   等引入 Channel 后这里会再演进。

#pragma once

#include <cstdint>
#include <functional>
#include <unordered_map>

namespace epoll_proj {

class EventLoop {
public:
    // 事件回调：参数是 epoll_wait 返回的事件位掩码
    using EventCallback = std::function<void(uint32_t events)>;

    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // 注册 fd 到 epoll，附带它的事件回调
    // 同一个 fd 重复 add 会被视为参数错误（打日志 + 忽略），避免静默覆盖回调
    void add_fd(int fd, uint32_t events, EventCallback cb);

    // 修改 fd 关注的事件位（回调不变）
    void mod_fd(int fd, uint32_t events);

    // 从 epoll 注销 fd 并丢弃其回调
    // 注意：本函数只做 EPOLL_CTL_DEL + 移除回调，不 ::close(fd)；
    //       fd 的生命周期由调用方（Connection）负责，这里只管 epoll 视图。
    void del_fd(int fd);

    // 阻塞循环，直到外部调用 quit()
    void loop();
    void quit() { quit_ = true; }

private:
    int epfd_ = -1;
    bool quit_ = false;

    // fd → 事件回调
    // 用 unordered_map 是因为运行期会增删，且 epoll_wait 返回的是 fd，
    // 不像 muduo 那样直接把 Channel* 塞 epoll_data —— 后续引入 Channel 时会改。
    std::unordered_map<int, EventCallback> handlers_;
};

}  // namespace epoll_proj
