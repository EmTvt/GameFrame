# SKILL: 构建、运行、调试

## 构建

```bash
cd /data/workspace/epoll_proj

# 首次：生成 build 目录
cmake -B build

# 增量构建（绝大多数情况用这个）
cmake --build build -j
```

产物：`build/epoll_proj`（也会有一个根目录的 `main` 历史产物，可忽略）。

## 运行

```bash
./build/epoll_proj
# 监听 0.0.0.0:8888，日志打到 stdout
```

测试连接：

```bash
# 方式 A：telnet
telnet 127.0.0.1 8888
# 输入任意字符回车 → 应被原样回显

# 方式 B：nc（更通用）
nc 127.0.0.1 8888

# 方式 C：脚本压测（验证粘/拆包）
python3 -c "
import socket, time
s = socket.create_connection(('127.0.0.1', 8888))
s.sendall(b'hi\n'); time.sleep(0.1)
s.sendall(b'bye\n')
print(s.recv(1024))
"
```

## 调试

### gdb

```bash
# CMakeLists 默认就是 Debug，含调试符号
gdb --args ./build/epoll_proj
(gdb) b epoll_proj::Connection::handle_read
(gdb) r
```

### 看连接状态

```bash
# 看监听端口和已建立连接
ss -tnp | grep 8888

# 看接收 / 发送队列堆积情况（验证 TCP 反压）
ss -tn '( sport = :8888 )'
# Recv-Q / Send-Q 堆得高 → 应用没读 / 对端没读
```

### 抓包（验证粘/拆包）

```bash
sudo tcpdump -i lo port 8888 -A -nn
```

## 修改后必做的检查

1. `cmake --build build -j` —— 编译过
2. 启动后用上面任意一种客户端验证 echo 没坏
3. 查看 stdout 日志：`new client fd=...` / `recv N bytes` / `client closed fd=...` 三类日志应都正常出现

## 常见问题

| 现象 | 排查 |
|---|---|
| `bind` 失败 `Address already in use` | 上次进程没退干净，等几秒或换端口；代码已开 `SO_REUSEADDR` |
| 连接不上 | 检查防火墙、确认 `./build/epoll_proj` 进程存活、监听地址是否 `0.0.0.0` |
| telnet 输入但服务端无 stdout 输出 | 可能是回调没注册；检查 `main.cpp` 里的 `set_message_callback` 是否调用了 |
