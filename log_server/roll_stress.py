#!/usr/bin/env python3
"""
log_server roll-over 压测脚本。

用途：
    往 log_server 持续灌字节流，触发 LogFile 的按 size 滚动（默认 10 MiB / 文件）。
    单条 payload 不大（默认 1 KiB），目的是制造"多次 EPOLLIN + 多次 append"的真实节奏，
    而不是一次性塞超大块，避开 server 端 socket 缓冲一口气吃饱后 read 只来一次的伪场景。

协议：
    与 smoke_client.py 完全一致：4 字节大端 length + payload。
    所有数据走同一条 TCP 长连接顺序写入。

示例：
    # 默认：连 127.0.0.1:9000，发 12 MiB，每条 1 KiB —— 稳定触发一次 .log.1 → .log.2
    ./roll_stress.py

    # 多翻几次（总量 35 MiB，会出现 .log.1 / .log.2 / .log.3 / .log.4）
    ./roll_stress.py --port 9099 --total 35M

    # 调小每条 payload，让 server 端 append 调用次数更多
    ./roll_stress.py --total 12M --size 256

    # 限速（每秒最多发 N 条，便于慢慢观察 server 端日志）
    ./roll_stress.py --total 12M --size 1K --rate 500
"""

import argparse
import socket
import struct
import sys
import time


def parse_size(s: str) -> int:
    """支持 '1024' / '1K' / '12M' / '1G' 形式（二进制单位 KiB/MiB/GiB）。"""
    s = s.strip()
    if not s:
        raise argparse.ArgumentTypeError("empty size")
    unit = s[-1].upper()
    if unit in ("K", "M", "G"):
        num = float(s[:-1])
        mul = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}[unit]
        return int(num * mul)
    return int(s)


def make_payload(seq: int, size: int) -> bytes:
    """造一条 size 字节的 payload，前缀带序号便于事后核对落盘内容。"""
    head = f"[{seq:010d}] ".encode("ascii")
    if size <= len(head):
        return head[:size]
    return head + b"x" * (size - len(head))


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--total", type=parse_size, default="12M",
                   help="要发送的 payload 总字节数（不含 4 字节 length 头），"
                        "支持 K/M/G 后缀。默认 12M。")
    p.add_argument("--size", type=parse_size, default="1K",
                   help="每条 payload 的字节数。默认 1K。")
    p.add_argument("--rate", type=int, default=0,
                   help="限速：每秒最多发多少条；0 表示不限速。默认 0。")
    args = p.parse_args()

    if args.size <= 0 or args.total <= 0:
        print("size and total must be positive", file=sys.stderr)
        return 2

    n_msgs = (args.total + args.size - 1) // args.size   # 向上取整
    print(f"target: send {n_msgs} messages × {args.size} B = ~{n_msgs * args.size / 1024 / 1024:.2f} MiB",
          file=sys.stderr)
    print(f"connecting to {args.host}:{args.port} ...", file=sys.stderr)

    sent_bytes = 0
    sent_msgs = 0
    next_progress_mark = 1 * 1024 * 1024   # 每 1 MiB 打一次进度
    t0 = time.perf_counter()
    last_tick = t0

    with socket.create_connection((args.host, args.port)) as s:
        # 关掉 Nagle，让小包尽快到 server，触发更频繁的 EPOLLIN —— 测试场景下友好
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        for i in range(n_msgs):
            payload = make_payload(i, args.size)
            frame = struct.pack(">I", len(payload)) + payload
            try:
                s.sendall(frame)
            except (BrokenPipeError, ConnectionResetError) as e:
                # server 主动关了（比如 bad frame / 超大 length）就别再灌了
                print(f"\nconnection lost after {sent_msgs} msgs / {sent_bytes} B: {e}",
                      file=sys.stderr)
                break

            sent_msgs += 1
            sent_bytes += len(payload)

            if sent_bytes >= next_progress_mark:
                elapsed = time.perf_counter() - t0
                mb = sent_bytes / 1024 / 1024
                rate = mb / elapsed if elapsed > 0 else 0
                print(f"  ...sent {sent_msgs} msgs, {mb:.2f} MiB, {rate:.2f} MiB/s",
                      file=sys.stderr)
                next_progress_mark += 1 * 1024 * 1024

            # 限速：按目标速率算下一条该在什么时间发，没到就 sleep
            if args.rate > 0:
                target_t = t0 + (sent_msgs / args.rate)
                now = time.perf_counter()
                if target_t > now:
                    time.sleep(target_t - now)

        # 主动关写端，让 server 把 buffer 里最后一条读完再 close
        # （这条路径就是上次修过的 EOF 派发逻辑）
        try:
            s.shutdown(socket.SHUT_WR)
        except OSError:
            pass

        # 等 server 关连接；超时直接走
        s.settimeout(2.0)
        try:
            while s.recv(4096):
                pass
        except (socket.timeout, OSError):
            pass

    elapsed = time.perf_counter() - t0
    mb = sent_bytes / 1024 / 1024
    print(f"\ndone: {sent_msgs} msgs, {mb:.2f} MiB in {elapsed:.2f} s "
          f"({mb / elapsed:.2f} MiB/s)" if elapsed > 0 else f"done: {sent_msgs} msgs",
          file=sys.stderr)
    print(f"verify: ls -la $(dirname YOUR_LOG_DIR)/* and look for .log.1, .log.2 ...",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
