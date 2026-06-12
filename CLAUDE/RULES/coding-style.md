# RULE: 代码风格

> 这是项目的硬性约定。AI 协作者修改代码时**必须遵循**，否则会和现有代码不一致。

## 命名

| 元素 | 风格 | 示例 |
|---|---|---|
| 命名空间 | snake_case | `epoll_proj` |
| 类名 | PascalCase | `TcpServer`, `Connection`, `Buffer` |
| 函数 / 方法 | snake_case | `handle_read`, `set_message_callback` |
| 成员变量 | snake_case + **尾下划线** | `fd_`, `input_buffer_`, `connections_` |
| 局部变量 / 参数 | snake_case，**无下划线** | `conn_fd`, `peer_addr` |
| 常量 | k 前缀 + PascalCase | `kBacklog`, `kMaxEvents`, `kInitialSize` |
| 枚举类成员 | k 前缀 + PascalCase | `State::kConnected` |
| 宏 | UPPER_SNAKE（**尽量避免使用宏**） | — |

## 头文件

- `#pragma once`，不用 include guard
- include 顺序：
  1. 对应的 `.h`（如果是 `.cpp`）
  2. 系统头（`<sys/...>` `<netinet/...>`）—— 用尖括号
  3. 标准库头（`<string>` `<vector>`）—— 用尖括号
  4. 项目头（`"buffer.h"`）—— 用双引号
- 每组之间空一行

参考 `connection.cpp` 的 include 块。

## 类设计

- 默认 `delete` 拷贝构造和拷贝赋值（**所有 RAII 资源类必须**）
- 有需要再显式 `default` 移动语义
- public → private 顺序：方法在前，字段在最后
- 所有回调用 `std::function<...>` 包装，而不是模板

```cpp
class Connection {
public:
    using MessageCallback = std::function<void(Connection&, Buffer&)>;
    // ...

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

private:
    int fd_;
    // ...
};
```

## 注释

- **写"为什么"，不写"做了什么"**：代码本身能看出做了什么
- 关键决策点必须注释（如 `connection.cpp::close()` 里为什么不清零 `fd_`）
- 复杂数据结构的内存布局用 ASCII 图表示（参考 `buffer.h` 顶部）
- 文件头部用 `//` 注释一段，描述这个模块的职责和与其他模块的关系

## 错误处理

- 致命错误（启动期 socket/bind/listen/epoll_create 失败）→ 调 `die()` 直接退出
- 运行期错误（read/write 失败）→ 打 `std::cerr` 日志 + `close()` 关连接，**不抛异常**
- 业务可恢复的错误（accept EAGAIN、read EINTR）→ continue / break，不打日志
- **本项目禁止使用 C++ 异常**（与 epoll 事件循环风格不兼容）

## 现代 C++ 规则

- C++20 标准（CMakeLists 已设定）
- 优先 `std::string_view` 而不是 `const std::string&`
- 优先 `std::unique_ptr` 而不是裸 `new/delete`
- 用 `std::move` 避免不必要的拷贝（特别是 `std::function` 和 `std::string` 的 setter）
- `auto` 仅在类型显而易见或非常长时使用

## 不要做的事

- ❌ 不要引入 Boost、muduo、asio 等第三方库
- ❌ 不要写 `using namespace std;`（除非在小函数内部）
- ❌ 不要用宏代替 const / inline 函数
- ❌ 不要用 C 风格类型转换（`(int)x`）—— 用 `static_cast<int>(x)`
- ❌ 不要在头文件里定义大函数（只放小的 inline / setter / getter）
