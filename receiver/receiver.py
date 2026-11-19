#!/usr/bin/env python3
"""本地遥测接收器：默认原样回显报文，可延迟回包，可配置返回不同内容。

延迟来源（优先级从高到低）：
1. 报文以 "DELAY:<秒>:" 开头时，按该秒数延迟（回包仍包含完整原始报文）；
2. 否则用环境变量 REPLY_DELAY（秒，默认 0）。

返回内容（用于演示影子校验的“差异”）：
1. 环境变量 REPLY_TEMPLATE 非空时，回包内容 = 模板中的 "{payload}" 替换为
   原始报文（按 UTF-8 解码；无法解码则按原字节回显）。例如 "echo:{payload}"；
2. 否则原样回显。

环境变量：
  RECEIVER_NAME   显示名（默认 receiver）
  LISTEN_PORT     监听端口（默认 9001）
  REPLY_DELAY     默认回包延迟秒数（默认 0）
  REPLY_TEMPLATE  自定义回包内容模板，{payload} 为原始报文（默认空=原样回显）
"""

import asyncio
import os
import re

NAME = os.environ.get("RECEIVER_NAME", "receiver")
PORT = int(os.environ.get("LISTEN_PORT", "9001"))
DEFAULT_DELAY = float(os.environ.get("REPLY_DELAY", "0"))
MAX_DELAY = float(os.environ.get("MAX_DELAY", "60"))
REPLY_TEMPLATE = os.environ.get("REPLY_TEMPLATE", "")
REPLY_PREFIX = os.environ.get("REPLY_PREFIX", "")  # 便捷写法：等价于模板 "{PREFIX}{payload}"

DELAY_RE = re.compile(rb"^DELAY:([0-9]+(?:\.[0-9]+)?):")


def build_reply(data: bytes) -> bytes:
    """按配置生成回包内容；未配置时原样回显。"""
    if REPLY_PREFIX:
        return REPLY_PREFIX.encode() + data
    if REPLY_TEMPLATE:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            return data
        return REPLY_TEMPLATE.replace("{payload}", text).encode("utf-8")
    return data


class ReceiverProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        delay = DEFAULT_DELAY
        m = DELAY_RE.match(data)
        if m:
            delay = min(float(m.group(1)), MAX_DELAY)
        print(f"[{NAME}] 收到 {len(data)}B 来自 {addr[0]}:{addr[1]}，{delay}s 后回包", flush=True)
        asyncio.get_running_loop().call_later(delay, self._reply, data, addr)

    def _reply(self, data, addr):
        try:
            reply = build_reply(data)
            self.transport.sendto(reply, addr)
            print(f"[{NAME}] 已回包 {len(reply)}B 到 {addr[0]}:{addr[1]}", flush=True)
        except Exception as exc:
            print(f"[{NAME}] 回包失败: {exc}", flush=True)


async def main():
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(
        lambda: ReceiverProtocol(), local_addr=("0.0.0.0", PORT)
    )
    print(f"[{NAME}] UDP 监听 0.0.0.0:{PORT}，默认延迟 {DEFAULT_DELAY}s", flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
