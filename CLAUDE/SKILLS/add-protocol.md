# SKILL: 给项目接入新协议

> 适用任务："把 echo 改成按 `\n` 分隔的协议"、"接入长度前缀协议"、"做一个最简 HTTP"等。

## 核心原则

**框架层（TcpServer / Connection / Buffer）和协议无关**。接入新协议 = **写一个新的 message_callback**，从 `Buffer` 里按协议拆消息即可。

---

## 模板：按 `\n` 分隔的行协议

```cpp
server.set_message_callback([](Connection& conn, Buffer& input) {
    while (true) {
        auto view = input.readable_view();          // 不消费，只看
        auto pos = view.find('\n');
        if (pos == std::string_view::npos) break;   // 半包，留着等下次

        std::string line(view.data(), pos);         // 提取一行（不含 \n）
        input.retrieve(pos + 1);                    // 消费掉它（含 \n）

        // === 业务逻辑 ===
        conn.send(line + " (echoed)\n");
    }
});
```

**关键点**：
- 用 `while + break` 循环，一次回调里把缓冲区里**能拆的全拆完**
- `retrieve` 消费多少，剩下的（半包）会自动留在 buffer 里
- 永远先判断"够不够拆"再 `retrieve`

---

## 模板：长度前缀协议（big-endian uint32 + body）

```cpp
#include <arpa/inet.h>  // ntohl

server.set_message_callback([](Connection& conn, Buffer& input) {
    while (input.readable_bytes() >= 4) {
        // 1) 偷看长度（不消费）
        uint32_t net_len;
        std::memcpy(&net_len, input.peek(), 4);
        uint32_t body_len = ::ntohl(net_len);

        // 2) 防御：长度异常
        if (body_len > 10 * 1024 * 1024) {  // 10MB 上限
            conn.close();
            return;
        }

        // 3) 包还没到齐
        if (input.readable_bytes() < 4 + body_len) break;

        // 4) 取出完整消息
        std::string body(input.peek() + 4, body_len);
        input.retrieve(4 + body_len);

        // === 业务逻辑 ===
        // ...
    }
});
```

---

## 模板：最简 HTTP（玩具版）

```cpp
server.set_message_callback([](Connection& conn, Buffer& input) {
    auto view = input.readable_view();
    auto pos = view.find("\r\n\r\n");
    if (pos == std::string_view::npos) return;  // 头还没收完

    // 简单起见：不解析方法/路径/header，固定返回
    std::string body = "Hello from epoll_proj!\n";
    std::string resp =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: " + std::to_string(body.size()) + "\r\n"
        "Connection: close\r\n"
        "\r\n" + body;

    conn.send(resp);
    input.retrieve(pos + 4);
    conn.close();   // 短连接：发完就关
});
```

测试：`curl http://127.0.0.1:8888/`

⚠️ **这只是"长得像 HTTP"**：
- 没解析请求行/header/body（无法做真实业务）
- 不支持 Keep-Alive（每个请求都关连接）
- 不支持 chunked / Content-Length 读 body
- 大响应会撑爆 output_buffer_（目前没有水位线）

要做正经 HTTP，需要先给框架补：`Connection::set_context`、`WriteCompleteCallback`、水位线。详见 `CLAUDE/SKILLS/extend-framework.md`。

---

## 接入新协议的检查清单

- [ ] 回调里**永远先判长度再 `retrieve`**，不能边读边消费导致数据丢失
- [ ] **半包必须留在 buffer 里**，靠 `break` 退出循环，**不要清空**
- [ ] 给"消息体大小"加上限，防止恶意客户端发巨大长度撑爆内存
- [ ] 协议解析失败（格式错、长度异常）时调 `conn.close()`，不要让连接处于不一致状态
- [ ] 业务回调里**不要做阻塞操作**（数据库查询、文件 I/O 等），会卡死整个 epoll 循环
