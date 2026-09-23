// test_rpc_codec: RpcMessage / RpcCodec 的自包含单测（RPC-1 验收）
//
// rpc/rpc_message.h 是纯头文件、无状态，所以这个测试**不碰网络、不起线程、
// 不依赖外部进程**，直接 `./build/test_rpc_codec` 一键跑，返回码即结论。
//
// 覆盖六件事：
//   1) round-trip：Request（带 method + body）encode → decode 字段逐个一致；
//   2) 线上字节真的是大端：逐字节核对 request_id 的 8 个字节，
//      防止将来有人改成 htobe64 时把字节序悄悄弄反；
//   3) Response：空 method、含 '\0' 的二进制 body、越出枚举的业务错误码；
//   4) 坏帧拒收：短于头部 / method_len 声称的长度越界 / type 不认识；
//      且失败时不许在 out 里留半份解析结果；
//   5) 边界：body 为空（method_len 正好等于头后剩余长度）仍合法；
//   6) 两层叠加：内层 RpcCodec + 外层 LengthPrefixedCodec 连续拆 3 帧，
//      并走一次「字节流从中间切断」的半包路径。
//
// 结果判定：所有 CHECK 通过 → 打印 [PASS] 返回 0；任一失败 → [FAIL ...] 返回 1。

#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "rpc/rpc_message.h"
#include "util/buffer.h"
#include "util/length_prefixed_codec.h"

using namespace epoll_proj;

namespace {

int g_failed = 0;

#define CHECK(cond)                                                        \
    if (!(cond)) {                                                         \
        std::cout << "[FAIL] line " << __LINE__                            \
                << ": CHECK(" #cond ")\n";                                 \
        ++g_failed;                                                        \
    }                                                                      

// ---- 1) + 2) Request round-trip，并核对大端字节序 ----
void test_request_round_trip() {
    RpcMessage req;
    req.type       = RpcType::kRequest;
    req.request_id = 0x0102030405060708ull;   // 8 个字节互不相同，方便逐字节核对
    req.status     = RpcStatus::kOk;
    req.method     = "EchoService.Echo";
    req.body       = "hello";

    const std::string payload = RpcCodec::encode(req);
    CHECK(payload.size() == RpcCodec::kHeaderLen + req.method.size() + req.body.size());

    // request_id 占 payload[1..8]，必须高位在前。
    for (int i = 0; i < 8; ++i) {
        CHECK(static_cast<uint8_t>(payload[1 + i]) == static_cast<uint8_t>(i + 1));
    }
    // method_len 占 payload[13..14]，"EchoService.Echo" = 16 字节 → 0x00 0x10。
    CHECK(static_cast<uint8_t>(payload[13]) == 0x00);
    CHECK(static_cast<uint8_t>(payload[14]) == 0x10);

    RpcMessage got;
    CHECK(RpcCodec::decode(payload, got));
    CHECK(got.type == RpcType::kRequest);
    CHECK(got.request_id == req.request_id);
    CHECK(got.status == RpcStatus::kOk);
    CHECK(got.method == req.method);
    CHECK(got.body == req.body);
}

// ---- 3) Response：空 method + 二进制 body + 越出枚举的业务错误码 ----
void test_response_round_trip() {
    RpcMessage rsp;
    rsp.type       = RpcType::kResponse;
    rsp.request_id = 42;
    rsp.status     = static_cast<RpcStatus>(7);   // 业务码，故意越出枚举
    rsp.body       = std::string("a\0b", 3);      // 中间带 '\0'，不能被当结尾

    RpcMessage got;
    CHECK(RpcCodec::decode(RpcCodec::encode(rsp), got));
    CHECK(got.type == RpcType::kResponse);
    CHECK(got.request_id == 42);
    CHECK(static_cast<uint32_t>(got.status) == 7);
    CHECK(got.method.empty());
    CHECK(got.body.size() == 3);
    CHECK(got.body == rsp.body);
}

// ---- 5) 边界：body 为空（method_len 正好吃掉头后全部剩余）----
void test_empty_body() {
    RpcMessage req;
    req.method = "Echo.Echo";
    // body 留空

    const std::string payload = RpcCodec::encode(req);
    CHECK(payload.size() == RpcCodec::kHeaderLen + req.method.size());

    RpcMessage got;
    got.body = "旧值必须被 decode 清掉";
    CHECK(RpcCodec::decode(payload, got));
    CHECK(got.method == "Echo.Echo");
    CHECK(got.body.empty());
}

// ---- 4) 坏帧拒收 ----
void test_reject_bad_frames() {
    RpcMessage tmpl;
    tmpl.type   = RpcType::kRequest;
    tmpl.method = "ab";
    tmpl.body   = "xy";
    const std::string good = RpcCodec::encode(tmpl);

    RpcMessage out;

    // 短于定长头：空 payload / 3 字节 / 差 1 字节到 15。
    CHECK(!RpcCodec::decode(std::string_view(), out));
    CHECK(!RpcCodec::decode(good.substr(0, 3), out));
    CHECK(!RpcCodec::decode(good.substr(0, RpcCodec::kHeaderLen - 1), out));

    // method_len 声称 100，实际头后只有 4 字节（"ab" + "xy"）。
    std::string lying = good;
    lying[13] = 0x00;
    lying[14] = 100;
    CHECK(!RpcCodec::decode(lying, out));

    // type 不认识：7 是没定义过的值。
    std::string bad_type = good;
    bad_type[0] = 7;
    CHECK(!RpcCodec::decode(bad_type, out));

    // 哨兵值本身也必须被拒（今天 kTypeCount == 2，即「还没实现的 kPush」）。
    std::string sentinel_type = good;
    sentinel_type[0] = static_cast<char>(RpcType::kTypeCount);
    CHECK(!RpcCodec::decode(sentinel_type, out));

    // 以上失败都不该在 out 里留半份解析结果。
    CHECK(out.request_id == 0);
    CHECK(out.method.empty());
    CHECK(out.body.empty());

    // method_len = 0、body 为空 —— 合法的最小帧（只有 15B 头）。
    RpcMessage minimal;
    CHECK(RpcCodec::decode(RpcCodec::encode(minimal), out));
}

// ---- 6) 两层叠加：内层 RpcCodec 套在外层 LengthPrefixedCodec 里 ----
void test_two_layer_framing() {
    std::vector<RpcMessage> sent(3);
    for (int i = 0; i < 3; ++i) {
        sent[i].type       = (i % 2 == 0) ? RpcType::kRequest : RpcType::kResponse;
        sent[i].request_id = 1000 + i;
        sent[i].method     = (i % 2 == 0) ? "Echo.Echo" : "";
        sent[i].body       = "body-" + std::to_string(i);
    }

    // 三条消息拼成一条连续字节流，模拟一次 read 就收到 3 个完整帧（粘包）。
    std::string stream;
    for (const auto& m : sent) {
        stream += LengthPrefixedCodec::encode(RpcCodec::encode(m));
    }

    auto decode_all = [](Buffer& buf, std::vector<RpcMessage>& out) {
        std::vector<std::string> payloads;
        if (!LengthPrefixedCodec::decode(buf, payloads)) return false;
        for (const auto& p : payloads) {
            RpcMessage msg;
            if (!RpcCodec::decode(p, msg)) return false;
            out.push_back(std::move(msg));
        }
        return true;
    };

    auto same_as_sent = [&sent](const std::vector<RpcMessage>& got) {
        if (got.size() != sent.size()) return false;
        for (size_t i = 0; i < got.size(); ++i) {
            if (got[i].type != sent[i].type) return false;
            if (got[i].request_id != sent[i].request_id) return false;
            if (got[i].method != sent[i].method) return false;
            if (got[i].body != sent[i].body) return false;
        }
        return true;
    };

    // 粘包路径：一次性灌进去，应拆出 3 条。
    {
        Buffer buf;
        buf.append(stream);
        std::vector<RpcMessage> got;
        CHECK(decode_all(buf, got));
        CHECK(got.size() == 3);
        CHECK(same_as_sent(got));
        CHECK(buf.readable_bytes() == 0);   // 3 帧都被完整消费，无残留
    }

    // 半包路径：在最后一帧中间切断。前半只能拆出 2 条，补上后半才凑齐第 3 条。
    {
        const size_t cut = stream.size() - 3;
        Buffer buf;
        buf.append(std::string_view(stream).substr(0, cut));

        std::vector<RpcMessage> got;
        CHECK(decode_all(buf, got));
        CHECK(got.size() == 2);             // 第 3 帧还不完整，留在 buffer 里等
        CHECK(buf.readable_bytes() > 0);

        buf.append(std::string_view(stream).substr(cut));
        CHECK(decode_all(buf, got));
        CHECK(got.size() == 3);
        CHECK(same_as_sent(got));
        CHECK(buf.readable_bytes() == 0);
    }
}

}  // namespace

int main() {
    test_request_round_trip();
    test_response_round_trip();
    test_empty_body();
    test_reject_bad_frames();
    test_two_layer_framing();

    if (g_failed != 0) {
        std::cout << "[FAIL] test_rpc_codec: " << g_failed << " check(s) failed\n";
        return 1;
    }
    std::cout << "[PASS] test_rpc_codec: all checks passed\n";
    return 0;
}
