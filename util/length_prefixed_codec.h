// LengthPrefixedCodec: client/server 共享的最小帧编解码
//
// 协议：
//   +-----------------+--------------------+
//   |  4 字节 length  |  payload (length B) |
//   +-----------------+--------------------+
//   length 为 payload 字节数（不含自身），网络字节序（大端）。
//
// 设计动机：
//   - log_server 和未来的 LogSender 都要拼/拆这一格式；不抽出来就会有两份重复
//     的 peek_uint32_be + 循环消费逻辑，改协议要改两处。
//   - 纯静态函数、无状态：解码状态全在传入的 Buffer 上（已经是连接私有的
//     input_buffer_），codec 自己不存任何东西，可以放心给任意线程调用。
//   - 头文件实现：函数体很短，省一个 .cpp，避免链接期再添依赖。

#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "util/buffer.h"

namespace epoll_proj {

struct LengthPrefixedCodec {
    // 单条 payload 字节数上限。防恶意/异常 length 把 input_buffer 撑爆。
    // 16 MiB 对日志足够大；想更严的业务自己传更小的 max_frame_len。
    static constexpr uint32_t kDefaultMaxFrameLen = 16 * 1024 * 1024;

    // 把 payload 编成可直接 conn->send() 的字符串。
    // 选 string 而不是 vector<char>：Connection::send 接受 string_view，
    // 而 string 在 SSO 内（payload 很短时）能省一次堆分配。
    static std::string encode(std::string_view payload) {
        const uint32_t len = static_cast<uint32_t>(payload.size());
        std::string out;
        out.resize(4 + payload.size());
        // 大端写入 length。手写而非 htonl 是为了不引头文件依赖、且语义自明。
        out[0] = static_cast<char>((len >> 24) & 0xFF);
        out[1] = static_cast<char>((len >> 16) & 0xFF);
        out[2] = static_cast<char>((len >> 8) & 0xFF);
        out[3] = static_cast<char>(len & 0xFF);
        if (!payload.empty()) {
            std::memcpy(out.data() + 4, payload.data(), payload.size());
        }
        return out;
    }

    // 从 input 中循环拆帧，把完整消息追加到 out。
    // 半包/粘包都在这里处理：
    //   - 头不够 4 字节  → 直接返回 true，等下次 EPOLLIN 攒够再来
    //   - 头够但 body 不够 → 同上
    //   - length == 0 或 length > max_frame_len → 视为协议错误，返回 false
    //     （调用方应 close 连接；继续读没意义，buffer 已经对不上边界）
    //
    // 返回 true 表示"目前所见数据均合法"（不一定有解出新消息）；返回 false 表示
    // 遇到非法帧、buffer 已脏、必须断连。
    static bool decode(Buffer& input,
                       std::vector<std::string>& out,
                       uint32_t max_frame_len = kDefaultMaxFrameLen) {
        while (true) {
            if (input.readable_bytes() < 4) return true;   // 头还没收齐

            const uint32_t len = peek_uint32_be(input);
            if (len == 0 || len > max_frame_len) return false;   // 非法帧

            if (input.readable_bytes() < 4u + len) return true;  // body 还不够

            input.retrieve(4);
            out.emplace_back(input.retrieve_as_string(len));
        }
    }

private:
    // 从 buffer 可读区前 4 字节读大端 uint32。调用方需保证 readable_bytes() >= 4。
    static uint32_t peek_uint32_be(const Buffer& buf) {
        const char* p = buf.peek();
        uint32_t n = 0;
        n |= static_cast<uint32_t>(static_cast<uint8_t>(p[0])) << 24;
        n |= static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 16;
        n |= static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 8;
        n |= static_cast<uint32_t>(static_cast<uint8_t>(p[3]));
        return n;
    }
};

}  // namespace epoll_proj
