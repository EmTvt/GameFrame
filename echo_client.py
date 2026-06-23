#!/usr/bin/env python3
"""
epoll_proj echo server 的手动测试客户端 —— 用来驱动业务侧 LOG_* 落盘。

前置（你手动起两个进程）：
    终端1:  ./build/log_server 9099 ./res/log        # 日志收集 + 落盘
    终端2:  ./build/epoll_proj                        # echo demo（监听 :8888，已接 LOG_*）
    终端3:  ./echo_client.py                          # 本脚本：连 :8888 制造业务事件

epoll_proj 是裸字节 echo（不是 length-prefix）：发什么回显什么。它在业务里打这些日志，
最终经 LogSender → log_server 落到 ./res/log/epoll_proj.<日期>.log.N：

    连接建立      → LOG_INFO   "connected fd=.. peer=.."
    每收到一段数据 → LOG_DEBUG  "recv N bytes from .."
    10s 空闲被踢   → LOG_WARN   "idle kick fd=.. after 10000ms idle"
    连接断开      → LOG_INFO   "disconnected fd=.. peer=.."

用法：
    # 默认：连 127.0.0.1:8888，发几条内置消息（间隔 0.5s）后正常断开
    ./echo_client.py

    # 指定 host/port
    ./echo_client.py --host 127.0.0.1 --port 8888

    # 自定义消息（可多次 -m）
    ./echo_client.py -m "hello" -m "world"

    # 发 N 条压测消息
    ./echo_client.py --count 20 --size 64

    # 发完后**故意空闲**等服务端 10s idle 踢人 → 触发 LOG_WARN（然后 LOG_INFO 断开）
    ./echo_client.py --idle

    # 从 stdin 按行读消息
    printf 'a\\nb\\nc\\n' | ./echo_client.py --stdin

观察日志（另开终端）：
    tail -f ./res/log/epoll_proj.*.log.*
"""

import argparse
import socket
import sys
import time


DEFAULT_MESSAGES = [
    "hello epoll_proj",
    "second line",
    "third line with a longer payload",
]


def build_messages(args) -> list[bytes]:
    """装配要发送的 payload 列表（bytes）。每条会在发送时补一个换行当行分隔。"""
    if args.stdin:
        msgs = []
        for line in sys.stdin:
            msgs.append(line.rstrip("\n").rstrip("\r").encode("utf-8"))
        return msgs
    if args.message:
        return [m.encode("utf-8") for m in args.message]
    if args.count > 0:
        out = []
        for i in range(args.count):
            prefix = f"[{i:05d}] ".encode("ascii")
            pad = max(0, args.size - len(prefix))
            out.append(prefix + b"x" * pad)
        return out
    return [m.encode("utf-8") for m in DEFAULT_MESSAGES]


def recv_echo(sock: socket.socket, expect_bytes: int, timeout: float = 1.0) -> bytes:
    """读回显。echo server 把我们发的原样送回，最多等 expect_bytes 或超时。"""
    sock.settimeout(timeout)
    buf = bytearray()
    try:
        while len(buf) < expect_bytes:
            chunk = sock.recv(4096)
            if not chunk:           # 服务端关连接
                break
            buf.extend(chunk)
    except socket.timeout:
        pass
    return bytes(buf)


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8888)
    p.add_argument("-m", "--message", action="append", default=[],
                   help="发送一条消息；可多次指定")
    p.add_argument("--stdin", action="store_true",
                   help="从 stdin 按行读消息（每行一条）")
    p.add_argument("--count", type=int, default=0,
                   help="压测模式：发 N 条消息")
    p.add_argument("--size", type=int, default=64,
                   help="压测模式下每条消息字节数（默认 64）")
    p.add_argument("--interval", type=float, default=0.5,
                   help="相邻消息发送间隔秒数（默认 0.5，便于在日志里逐条区分 DEBUG）")
    p.add_argument("--idle", action="store_true",
                   help="发完后保持连接空闲，等服务端 10s idle 踢人（触发 LOG_WARN）")
    args = p.parse_args()

    msgs = build_messages(args)
    if not msgs:
        print("no message to send", file=sys.stderr)
        return 1

    print(f"[client] connecting to {args.host}:{args.port} ...", file=sys.stderr)
    with socket.create_connection((args.host, args.port)) as s:
        print("[client] connected  (→ 服务端应打 LOG_INFO 'connected')",
              file=sys.stderr)

        # 逐条发送 + 读回显：每条触发服务端一行 LOG_DEBUG 'recv N bytes'
        for i, m in enumerate(msgs):
            line = m + b"\n"
            s.sendall(line)
            echo = recv_echo(s, len(line))
            shown = echo.rstrip(b"\r\n").decode("utf-8", "replace")
            print(f"[client] sent#{i} {len(line)}B -> echo: {shown!r}"
                  f"   (→ 服务端 LOG_DEBUG 'recv {len(line)} bytes')",
                  file=sys.stderr)
            if args.interval > 0 and i < len(msgs) - 1:
                time.sleep(args.interval)

        if args.idle:
            # 故意不再发数据，等服务端 idle timer（10s）到点踢人。
            # 服务端会打 LOG_WARN 'idle kick' 然后关连接 → 这里 recv 读到 EOF。
            print("[client] now idling, waiting for server's 10s idle-kick "
                  "(→ LOG_WARN 'idle kick' + LOG_INFO 'disconnected') ...",
                  file=sys.stderr)
            s.settimeout(15.0)
            t0 = time.perf_counter()
            try:
                while True:
                    if not s.recv(4096):    # EOF：服务端关了
                        dt = time.perf_counter() - t0
                        print(f"[client] server closed us after {dt:.1f}s idle "
                              "(idle-kick fired)", file=sys.stderr)
                        break
            except socket.timeout:
                print("[client] timed out waiting for idle-kick "
                      "(idle timeout 改过？)", file=sys.stderr)
        else:
            # 正常收尾：关写端，让服务端读到 EOF 后断开 → LOG_INFO 'disconnected'
            s.shutdown(socket.SHUT_WR)
            print("[client] sent all, shutting down "
                  "(→ 服务端 LOG_INFO 'disconnected')", file=sys.stderr)
            s.settimeout(1.0)
            try:
                while s.recv(4096):
                    pass
            except socket.timeout:
                pass

    print("[client] done. 去 ./res/log/epoll_proj.*.log.* 看落盘日志。",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
