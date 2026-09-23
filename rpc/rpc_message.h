// RpcMessage + RpcCodec: RPC 的内层协议，套在 LengthPrefixedCodec 内层
//
// 两层帧的关系（docs/RPC_DESIGN.md §1.2）：
//   +----------------+--------------------------------------------+
//   |  4B length(BE) |  payload = [15B 定长头][method][body]      |
//   +----------------+--------------------------------------------+
//    \_ 外层：LengthPrefixedCodec _/ \____ 内层：本文件负责 ____/
//
// 内层定长头（15B，全部大端）：
//   uint8   type        0 = Request, 1 = Response
//   uint64  request_id  客户端自增、服务端原样回填 —— 响应路由键（§1.4）
//   uint32  status      仅 Response 有意义；0 = kOk
//   uint16  method_len  仅 Request 有意义；method 紧随头部之后
//   bytes   method      形如 "EchoService.Echo"
//   bytes   body        剩余全部，序列化后的参数/返回值
//
// 设计动机：
//   - **为什么定长头，而不是「按 type 决定字段在不在」**：两种 type 的解码步骤
//     其实完全相同，只是「哪些字段有意义」不同。定长后 decode 只有一条路径，
//     边界校验写一遍就够，不会漏分支。代价是 Response 白带 2B method_len、
//     Request 白带 4B status，合计 6B —— 相对 body 可以忽略。
//   - **为什么无状态 + 纯头文件**：拆帧状态全都在外层的 Buffer 里（与
//     LengthPrefixedCodec 同款理由），codec 自己不存任何东西，任意线程可调。
//   - **为什么手写 shift 而不用 htonl/htobe64**：与 LengthPrefixedCodec 保持
//     一致，不引额外头文件，也不受 glibc 的 htobe64 是宏还是函数这类差异影响。

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace epoll_proj {

enum class RpcType : uint8_t {
    kRequest  = 0,
    kResponse = 1,

    // 哨兵：不是会出现在线上的类型值，只用来给 decode 提供「合法 type」的上界。
    // 将来加 kPush（DESIGN.md §6.3 的服务端主动推送）时插在这一行**之前**，
    // decode 一行都不用改 —— 那个坑（「上界写死成 kResponse，忘了改，新消息被
    // 当非法帧丢弃并关连接」）就从源头不存在了。
    kTypeCount,
};

// 框架层错误码。
// ★ status 字段上出现的值**可以越出这张枚举**：RPC-2 约定业务 handler 的返回码
//   （int，0 = 成功）原样透传进同一个字段。底层类型固定为 uint32_t，因此越界值
//   仍是合法的枚举值，decode 故意不做范围校验 —— 校验了就把业务码给拦了。
enum class RpcStatus : uint32_t {
    kOk = 0,
    kMethodNotFound,
    kDeserializeError,
    kServerError,
    kTimeout,
};

struct RpcMessage {
    RpcType     type       = RpcType::kRequest;
    uint64_t    request_id = 0;
    RpcStatus   status     = RpcStatus::kOk;   // 仅 Response 有意义
    std::string method;                        // 仅 Request 有意义
    std::string body;
};

struct RpcCodec {
    static constexpr size_t kHeaderLen = 15;   // 1 + 8 + 4 + 2

    // method_len 是 uint16，方法名长度上限随之而来。
    // 方法名在本框架里都是编译期常量（"Echo.Echo"），正常不可能撞上限。
    static constexpr size_t kMaxMethodLen = 0xFFFF;

    // 把 RpcMessage 编成内层 payload。
    // 发送侧最终形态：conn->send(LengthPrefixedCodec::encode(RpcCodec::encode(msg)))。
    //
    // method 超过 kMaxMethodLen 时返回空串 —— 那是调用方的 bug（长度写不进
    // uint16，硬编下去会产出一个错位的帧），让它显式失败而不是静默截断。
    static std::string encode(const RpcMessage& msg) {
        if (msg.method.size() > kMaxMethodLen) return {};

        std::string out;
        out.reserve(kHeaderLen + msg.method.size() + msg.body.size());
        out.push_back(static_cast<char>(msg.type));
        append_be<uint64_t>(out, msg.request_id);
        append_be<uint32_t>(out, static_cast<uint32_t>(msg.status));
        append_be<uint16_t>(out, static_cast<uint16_t>(msg.method.size()));
        out.append(msg.method);
        out.append(msg.body);
        return out;
    }

    // 从内层 payload 解出 RpcMessage。payload 由 LengthPrefixedCodec::decode 给出，
    // 所以这里**不需要**再处理半包：要么是一条完整 payload，要么就是坏帧。
    //
    // 返回 false 表示这条 payload 非法（短于头 / type 不认识 / method_len 越界），
    // 调用方应 conn->close()：坏帧意味着对端实现或字节流已经出问题，继续解没意义。
    // ★ 全部校验通过后才写 out，失败时 out 保持原样（不留半份解析结果）。
    static bool decode(std::string_view payload, RpcMessage& out) {
        if (payload.size() < kHeaderLen) return false;

        const char* p = payload.data();

        const uint8_t type = static_cast<uint8_t>(p[0]);
        if (type >= static_cast<uint8_t>(RpcType::kTypeCount)) return false;

        // method_len 声称的长度必须落在「头之后剩下的字节」里，否则 body 的起点
        // 会算到 payload 之外。这是本 decode 唯一的越界风险点。
        const uint16_t method_len = read_be<uint16_t>(p + 13);
        if (method_len > payload.size() - kHeaderLen) return false;

        out.type       = static_cast<RpcType>(type);
        out.request_id = read_be<uint64_t>(p + 1);
        out.status     = static_cast<RpcStatus>(read_be<uint32_t>(p + 9));
        out.method.assign(p + kHeaderLen, method_len);
        out.body.assign(p + kHeaderLen + method_len,
                        payload.size() - kHeaderLen - method_len);
        return true;
    }

private:
    template <class T>
    static void append_be(std::string& s, T v) {
        for (int shift = (static_cast<int>(sizeof(T)) - 1) * 8; shift >= 0; shift -= 8) {
            s.push_back(static_cast<char>((v >> shift) & 0xFF));
        }
    }

    // 调用方需保证 p 起至少有 sizeof(T) 字节可读。
    template <class T>
    static T read_be(const char* p) {
        T v = 0;
        for (size_t i = 0; i < sizeof(T); ++i) {
            v = static_cast<T>((v << 8) | static_cast<uint8_t>(p[i]));
        }
        return v;
    }
};

}  // namespace epoll_proj
