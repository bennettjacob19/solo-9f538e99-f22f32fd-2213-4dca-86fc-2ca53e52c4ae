#!/usr/bin/env python3
"""本地遥测接收器：原样回显报文，可延迟回包。

延迟来源（优先级从高到低）：
1. 报文以 "DELAY:<秒>:" 开头时，按该秒数延迟（回包仍包含完整原始报文）；
2. 否则用环境变量 REPLY_DELAY（秒，默认 0）。

环境变量：
  RECEIVER_NAME  显示名（默认 receiver）
  LISTEN_PORT    监听端口（默认 9001）
  REPLY_DELAY    默认回包延迟秒数（默认 0）
"""

import asyncio
import os
import re

NAME = os.environ.get("RECEIVER_NAME", "receiver")
PORT = int(os.environ.get("LISTEN_PORT", "9001"))
DEFAULT_DELAY = float(os.environ.get("REPLY_DELAY", "0"))
MAX_DELAY = float(os.environ.get("MAX_DELAY", "60"))

DELAY_RE = re.compile(rb"^DELAY:([0-9]+(?:\.[0-9]+)?):")


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
            self.transport.sendto(data, addr)  # 原样回显
            print(f"[{NAME}] 已回包 {len(data)}B 到 {addr[0]}:{addr[1]}", flush=True)
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
