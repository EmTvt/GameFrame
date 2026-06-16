#!/usr/bin/env python3
"""
log_server smoke test client.

协议：4 字节大端长度 + payload。每条消息独立编码后顺序写入同一个 TCP 连接。

用法：
    # 默认：连 127.0.0.1:9000，发几条内置示例
    ./smoke_client.py

    # 指定 host/port
    ./smoke_client.py --host 127.0.0.1 --port 9000

    # 从命令行额外追加消息
    ./smoke_client.py -m "hello world" -m "another line"

    # 从 stdin 按行读消息（每行一条）
    printf 'line1\nline2\n' | ./smoke_client.py --stdin

    # 压测：发 N 条，payload 长度可调
    ./smoke_client.py --count 10000 --size 128

    # 故意发坏帧（length 字段超大）验证 server 防御逻辑
    ./smoke_client.py --bad-length

验证方式：
    跑完后到 log_server 的 basedir 下 tail 一下最新的 epoll_proj.*.log 文件。
"""

import argparse
import socket
import struct
import sys
import time


DEFAULT_MESSAGES = [
    "hello log_server",
    "second line",
    "third line with some longer payload to make sure framing works",
]


def encode_frame(payload: bytes) -> bytes:
    """编码一帧：4 字节大端长度 + payload。"""
    if len(payload) > 0xFFFFFFFF:
        raise ValueError("payload too large for uint32 length prefix")
    return struct.pack(">I", len(payload)) + payload


def send_frames(sock: socket.socket, frames: list[bytes]) -> None:
    """把多帧一次性 sendall 出去；走同一条 TCP 连接，验证 server 的循环拆帧。"""
    # 一次性 join 再 sendall，让多帧大概率粘在同一个 EPOLLIN 里到达 server，
    # 这样能压到 message_callback 里的 while 循环拆帧路径。
    sock.sendall(b"".join(frames))


def build_messages(args) -> list[bytes]:
    """根据命令行参数装配要发送的 payload 列表（bytes）。"""
    msgs: list[bytes] = []

    if args.stdin:
        for line in sys.stdin:
            # 去掉行尾换行，但保留行内其他空白
            line = line.rstrip("\n").rstrip("\r")
            msgs.append(line.encode("utf-8"))
    elif args.message:
        msgs.extend(m.encode("utf-8") for m in args.message)
    elif args.count > 0:
        # 压测模式：生成 count 条固定 size 的消息
        # 每条形如 "[000123] " + 填充字符 'x' 到 size 字节
        for i in range(args.count):
            prefix = f"[{i:07d}] ".encode("ascii")
            pad_len = max(0, args.size - len(prefix))
            msgs.append(prefix + b"x" * pad_len)
    else:
        msgs.extend(m.encode("utf-8") for m in DEFAULT_MESSAGES)

    return msgs


def run_normal(args) -> int:
    msgs = build_messages(args)
    if not msgs:
        print("no message to send", file=sys.stderr)
        return 1

    frames = [encode_frame(m) for m in msgs]
    total_bytes = sum(len(f) for f in frames)

    print(f"connecting to {args.host}:{args.port} ...", file=sys.stderr)
    t0 = time.perf_counter()
    with socket.create_connection((args.host, args.port)) as s:
        # 批量发：把 send_frames 拆成 chunk，避免一次性塞太大
        chunk = 256
        for i in range(0, len(frames), chunk):
            send_frames(s, frames[i:i + chunk])
        # 主动 shutdown 写端：让 server 知道我们发完了；server 端会读到 EOF 然后关连接
        s.shutdown(socket.SHUT_WR)
        # 等 server 关连接（read 返回空）。不强求，超时即可走。
        s.settimeout(1.0)
        try:
            while s.recv(4096):
                pass
        except socket.timeout:
            pass
    elapsed = time.perf_counter() - t0

    print(
        f"sent {len(msgs)} messages, {total_bytes} bytes in {elapsed * 1000:.1f} ms",
        file=sys.stderr,
    )
    if elapsed > 0:
        print(
            f"throughput: {len(msgs) / elapsed:.0f} msg/s, "
            f"{total_bytes / elapsed / 1024 / 1024:.2f} MiB/s",
            file=sys.stderr,
        )
    return 0


def run_bad_length(args) -> int:
    """构造一个超长 length 字段，验证 server 的 kMaxFrameSize 防御。"""
    bad_header = struct.pack(">I", 0x7FFFFFFF)  # ~2 GiB
    print(
        f"connecting to {args.host}:{args.port} and sending a bad-length header ...",
        file=sys.stderr,
    )
    with socket.create_connection((args.host, args.port)) as s:
        s.sendall(bad_header)
        # server 应该立刻关连接
        s.settimeout(2.0)
        try:
            data = s.recv(1024)
            if not data:
                print("server closed connection as expected (good)", file=sys.stderr)
                return 0
            print(f"unexpected data from server: {data!r}", file=sys.stderr)
            return 1
        except socket.timeout:
            print("timed out waiting for server close (server may be misbehaving)",
                  file=sys.stderr)
            return 2


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("-m", "--message", action="append", default=[],
                   help="发送一条消息；可多次指定")
    p.add_argument("--stdin", action="store_true",
                   help="从 stdin 按行读消息（每行一条）")
    p.add_argument("--count", type=int, default=0,
                   help="压测模式：发 N 条消息（与 -m / --stdin 互斥时优先级最低）")
    p.add_argument("--size", type=int, default=128,
                   help="压测模式下每条消息的字节数（默认 128）")
    p.add_argument("--bad-length", action="store_true",
                   help="只发一个超大 length 字段，验证 server 防御")
    args = p.parse_args()

    if args.bad_length:
        return run_bad_length(args)
    return run_normal(args)


if __name__ == "__main__":
    sys.exit(main())
