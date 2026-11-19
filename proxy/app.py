#!/usr/bin/env python3
"""遥测 UDP 代理 + 操作网页。

功能要点：
- 按客户端 IP:端口建立会话，空闲超时（默认 5 秒）后过期；
- 建会话时把该会话固定到当前目标接收器（A/B），会话期间不随切换改变；
- 每个会话使用一条独立的、已 connect 的上游 UDP 套接字（独立本地端口），
  因此回包天然只能属于一个会话：只把该会话对应接收器的回包送回该客户端；
- 会话过期即关闭其上游端口，且端口进入冷却期不再分配，
  所以旧会话迟到的回包不可能落到同 IP:端口重建的新会话里；
- 报文与回包原样透传，不做任何修改；
- 当前目标保存在 SQLite，重启后保留；会话全在内存，重启即清空；
- 切换目标时先落库再生效，保存失败则保留原目标；
- Flask 网页可切换目标、发送试传报文、查看会话与报文日志（实际去向/回包）。
"""

import asyncio
import os
import socket
import sqlite3
import threading
import time
from collections import Counter

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

# ---------------- 配置 ----------------
UDP_LISTEN_HOST = os.environ.get("UDP_LISTEN_HOST", "0.0.0.0")
UDP_LISTEN_PORT = int(os.environ.get("UDP_LISTEN_PORT", "9000"))
HTTP_HOST = os.environ.get("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))
DB_PATH = os.environ.get(
    "DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy.db")
)
SESSION_IDLE_TIMEOUT = float(os.environ.get("SESSION_IDLE_TIMEOUT", "5"))
UPSTREAM_PORT_MIN = int(os.environ.get("UPSTREAM_PORT_MIN", "20000"))
UPSTREAM_PORT_MAX = int(os.environ.get("UPSTREAM_PORT_MAX", "29999"))
# 上游端口关闭后的冷却时间：冷却期内不再分配该端口，
# 保证旧会话迟到的回包不会被复用同端口的新会话收到。应大于接收器最大回包延迟。
UPSTREAM_PORT_COOLDOWN = float(os.environ.get("UPSTREAM_PORT_COOLDOWN", "30"))
LOG_KEEP = int(os.environ.get("LOG_KEEP", "1000"))
# 影子校验：开启后 window 秒内新建的会话会被绑定；其前 limit 条上行同时镜像到另一台接收器。
SHADOW_WINDOW = float(os.environ.get("SHADOW_WINDOW", "10"))
SHADOW_LIMIT = max(1, int(os.environ.get("SHADOW_LIMIT", "16")))
# 一次校验的总时长上限：超过则结束并（若仍有未决证据）标记证据不足。
SHADOW_MAX_DURATION = float(os.environ.get(
    "SHADOW_MAX_DURATION", str(SHADOW_WINDOW + SESSION_IDLE_TIMEOUT + 60)))
# 手动结束后，仍等待在途回包到达的收尾时间（秒）。
SHADOW_DRAIN = max(0.0, float(os.environ.get("SHADOW_DRAIN", "1.0")))

RECEIVERS = {
    "A": (
        os.environ.get("RECEIVER_A_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_A_PORT", "9001")),
    ),
    "B": (
        os.environ.get("RECEIVER_B_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_B_PORT", "9002")),
    ),
}
DEFAULT_TARGET = os.environ.get("DEFAULT_TARGET", "A")


def fmt_addr(addr):
    return f"{addr[0]}:{addr[1]}"


def preview(data: bytes) -> str:
    """报文内容展示：可打印文本原样显示，否则用 hex。"""
    try:
        text = data.decode("utf-8")
        if all(ch.isprintable() or ch in "\r\n\t" for ch in text):
            return text if len(text) <= 200 else text[:200] + "…"
    except UnicodeDecodeError:
        pass
    hexed = data.hex()
    return "hex:" + (hexed if len(hexed) <= 200 else hexed[:200] + "…")


def fmt_ts(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


# ---------------- SQLite：目标持久化 + 报文日志 ----------------
class Store:
    """线程安全的 SQLite 存取。目标(target)落库，重启后保留。"""

    def __init__(self, path):
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS message_log (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   ts REAL NOT NULL,
                   client TEXT NOT NULL DEFAULT '',
                   session_id INTEGER NOT NULL DEFAULT 0,
                   direction TEXT NOT NULL,
                   receiver TEXT NOT NULL DEFAULT '',
                   payload TEXT NOT NULL DEFAULT '',
                   note TEXT NOT NULL DEFAULT '')"""
        )
        self._conn.commit()
        row = self._conn.execute("SELECT value FROM settings WHERE key='target'").fetchone()
        if row and row[0] in RECEIVERS:
            self._target = row[0]
        else:
            self._target = DEFAULT_TARGET if DEFAULT_TARGET in RECEIVERS else sorted(RECEIVERS)[0]
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (self._target,)
            )
            self._conn.commit()
        self._inserts = 0

    def get_target(self):
        with self._lock:
            return self._target

    def set_target(self, target):
        """先持久化，成功后才切换内存中的当前目标；失败则抛异常并保留原目标。"""
        if target not in RECEIVERS:
            raise ValueError(f"未知接收器: {target!r}")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (target,)
            )
            self._conn.commit()
            self._target = target

    def log(self, direction, client="", session_id=0, receiver="", payload="", note=""):
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT INTO message_log(ts,client,session_id,direction,receiver,payload,note)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (time.time(), client, session_id, direction, receiver, payload, note),
                )
                self._conn.commit()
                self._inserts += 1
                if self._inserts % 200 == 0:
                    self._conn.execute(
                        "DELETE FROM message_log WHERE id < "
                        "(SELECT COALESCE(MIN(id),0) FROM "
                        "(SELECT id FROM message_log ORDER BY id DESC LIMIT ?))",
                        (LOG_KEEP,),
                    )
                    self._conn.commit()
        except sqlite3.Error:
            pass  # 日志失败绝不影响转发

    def recent_messages(self, limit=100):
        with self._lock:
            rows = self._conn.execute(
                "SELECT id,ts,client,session_id,direction,receiver,payload,note "
                "FROM message_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        cols = ("id", "ts", "client", "session_id", "direction", "receiver", "payload", "note")
        return [dict(zip(cols, r)) for r in rows]

    def latest_receiver_for_client(self, client):
        with self._lock:
            row = self._conn.execute(
                "SELECT receiver FROM message_log WHERE client=? AND direction='c2r' "
                "ORDER BY id DESC LIMIT 1",
                (client,),
            ).fetchone()
        return row[0] if row else None


# ---------------- 上游端口分配（带冷却期） ----------------
class PortAllocator:
    """显式管理上游本地端口：关闭后冷却期内不再分配，
    确保旧会话迟到的回包不会被复用同端口的新会话收到。"""

    def __init__(self, low, high, cooldown):
        self.low, self.high, self.cooldown = low, high, cooldown
        self._next = low
        self._in_use = set()
        self._closed = {}  # port -> 关闭时刻(monotonic)
        self._lock = threading.Lock()

    def allocate(self):
        with self._lock:
            now = time.monotonic()
            for _ in range(self.high - self.low + 1):
                port = self._next
                self._next += 1
                if self._next > self.high:
                    self._next = self.low
                if port in self._in_use:
                    continue
                closed_at = self._closed.get(port)
                if closed_at is not None and now - closed_at < self.cooldown:
                    continue
                self._in_use.add(port)
                self._closed.pop(port, None)
                return port
        raise RuntimeError("上游 UDP 端口耗尽")

    def release(self, port):
        with self._lock:
            self._in_use.discard(port)
            self._closed[port] = time.monotonic()

    def discard(self, port):
        """分配后绑定失败：不进入冷却，直接可用。"""
        with self._lock:
            self._in_use.discard(port)


# ---------------- 影子校验：回包多重集合比较 ----------------
def compare_replies(fixed_replies, shadow_replies):
    """把两侧回包按“原文 + 次数”做多重集合比较，天然忽略到达顺序。

    返回 (verdict, detail)：
    - consistent：两侧回包原文及各自次数完全一致；
    - different：两侧都能确认收到回包，但原文/次数不一致；
    - insufficient：任一侧没有收到任何回包（缺包 / 发送失败），无法下结论。
    多出来的报文不属于本次比较，由调用方另行标记证据不足。
    """
    if not fixed_replies or not shadow_replies:
        if not fixed_replies and not shadow_replies:
            return "insufficient", "两侧均未收到回包"
        missing = "固定接收器" if not fixed_replies else "影子接收器"
        return "insufficient", f"{missing}未收到回包（缺包或发送失败）"

    fixed = Counter(fixed_replies)
    shadow = Counter(shadow_replies)
    if fixed == shadow:
        return "consistent", f"两侧各 {len(fixed_replies)} 条回包，原文与次数一致"
    only_fixed = list((fixed - shadow).elements())
    only_shadow = list((shadow - fixed).elements())

    # 差异内容用统一的可读形式展示（文本原样，二进制转 hex）
    def _show(items):
        return "、".join(preview(k) for k in items[:3]) or "无"

    parts = []
    if only_fixed:
        parts.append("仅固定接收器有: " + _show(only_fixed))
    if only_shadow:
        parts.append("仅影子接收器有: " + _show(only_shadow))
    count_note = "" if len(fixed_replies) == len(shadow_replies) else (
        f"（固定 {len(fixed_replies)} 条 / 影子 {len(shadow_replies)} 条）")
    return "different", "回包不一致" + count_note + "：" + "；".join(parts)


# ---------------- 影子校验 ----------------
class ShadowCheck:
    """一次影子校验（打开网页后限时窗口内绑定一个新会话）。

    生命周期：
    - armed：已开启，等待窗口内新建会话绑定；
    - active：已绑定影子会话，开始镜像；
    - ended：已结束/超时/重启，不再接收任何镜像回包。
    每次校验有唯一 check_id，镜像套接字只把回包交给创建它的那一次校验，
    迟到的回包因校验已结束而被丢弃，不会串入下一次。
    """

    def __init__(self, cid, fixed, shadow, window, limit):
        self.id = cid
        self.fixed = fixed
        self.shadow = shadow
        self.limit = limit
        self.created = time.monotonic()
        self.deadline = self.created + window
        self.deadline_extra = self.created + max(window, SHADOW_MAX_DURATION)
        self.state = "armed"  # armed -> active -> ended
        self.client = None
        self.session = None
        self.sent = 0
        self.overflow = 0
        self.send_errors = 0
        self.fixed_replies = []
        self.shadow_replies = []
        self.transport = None
        self.end_reason = ""
        self.verdict = "insufficient"
        self.detail = "校验进行中，尚无结论"
        self.evaluated = False
        self.draining = False  # 已请求结束，仍在收尾窗口内等待在途回包
        self.drain_until = 0.0

    def evaluate(self, reason):
        """结束并给出结论；幂等，只计算一次。

        - 窗口内未绑定会话 → 证据不足；
        - 超过 16 条上行（多出的报文未纳入比较）→ 证据不足；
        - 任一侧镜像发送失败 / 任一侧没有回包 → 证据不足；
        - 否则按两侧回包原文与次数比较，一致或差异。
        """
        if self.evaluated:
            return
        self.evaluated = True
        self.end_reason = reason
        if self.state == "armed":
            self.verdict = "insufficient"
            self.detail = "校验窗口内没有建立新会话，未采集到任何证据"
            return
        if self.sent == 0:
            self.verdict = "insufficient"
            self.detail = "影子会话未采集到上行报文，证据不足"
            return
        if self.send_errors:
            self.verdict = "insufficient"
            self.detail = f"有 {self.send_errors} 次发送失败，证据不足"
            return
        # 缺包：任一接收器的回包数少于已镜像的上行数，说明有回包丢失/被丢弃。
        fixed_missing = max(0, self.sent - len(self.fixed_replies))
        shadow_missing = max(0, self.sent - len(self.shadow_replies))
        verdict, detail = compare_replies(self.fixed_replies, self.shadow_replies)
        if fixed_missing or shadow_missing:
            self.verdict = "insufficient"
            lost = []
            if fixed_missing:
                lost.append(f"固定侧缺 {fixed_missing} 条")
            if shadow_missing:
                lost.append(f"影子侧缺 {shadow_missing} 条")
            self.detail = f"存在缺包（{'，'.join(lost)}），证据不足；比较范围内：{detail}"
            return
        if self.overflow:
            self.verdict = "insufficient"
            self.detail = (f"超过 {self.limit} 条上行，多出 {self.overflow} 条未纳入比较，"
                           f"证据不足；比较范围内：{detail}")
            return
        self.verdict = verdict
        self.detail = detail

    def snapshot(self):
        """当前状态快照。进行中的校验也给出实时比较结果（结论以最终结束为准）。"""
        fixed_count = len(self.fixed_replies)
        shadow_count = len(self.shadow_replies)
        fixed_missing = max(0, self.sent - fixed_count)
        shadow_missing = max(0, self.sent - shadow_count)
        if self.evaluated:
            verdict, detail = self.verdict, self.detail
        elif self.state == "armed":
            verdict, detail = "insufficient", "等待窗口内建立新会话"
        elif self.state == "active":
            if self.draining:
                verdict = "insufficient"
                detail = f"正在收尾，等待在途回包（{(self.end_reason or '结束校验')}）"
            else:
                verdict, detail = compare_replies(self.fixed_replies, self.shadow_replies)
                notes = []
                if self.overflow:
                    notes.append(f"已超出 {self.limit} 条（多出 {self.overflow} 条未比较）")
                if self.send_errors:
                    notes.append(f"发送失败 {self.send_errors} 次")
                if fixed_missing or shadow_missing:
                    notes.append(f"缺包（固定侧 {fixed_missing} / 影子侧 {shadow_missing}）")
                if notes:
                    verdict = "insufficient"
                    detail = "；".join(notes) + "；" + detail
        else:
            verdict, detail = self.verdict, self.detail
        return {
            "id": self.id,
            "state": self.state,
            "fixed": self.fixed,
            "shadow": self.shadow,
            "client": self.client,
            "limit": self.limit,
            "sent": self.sent,
            "overflow": self.overflow,
            "send_errors": self.send_errors,
            "fixed_replies": [preview(r) for r in self.fixed_replies],
            "shadow_replies": [preview(r) for r in self.shadow_replies],
            "fixed_count": fixed_count,
            "shadow_count": shadow_count,
            "fixed_missing": fixed_missing,
            "shadow_missing": shadow_missing,
            "verdict": verdict,
            "detail": detail,
            "end_reason": self.end_reason,
            "age": round(time.monotonic() - self.created, 1),
        }


# ---------------- 会话与转发 ----------------
class Session:
    __slots__ = (
        "id", "client_addr", "receiver_name", "receiver_addr",
        "upstream_port", "transport", "state", "closed",
        "created", "last_activity", "pending",
        "shadow", "shadow_sent", "shadow_transport",
    )

    def __init__(self, sid, client_addr):
        self.id = sid
        self.client_addr = client_addr
        self.receiver_name = ""
        self.receiver_addr = None
        self.upstream_port = None
        self.transport = None
        self.state = "opening"  # opening -> open -> closed
        self.closed = False
        self.created = time.monotonic()
        self.last_activity = self.created
        self.pending = []
        self.shadow = None            # 绑定的 ShadowCheck（仅前 16 条镜像）
        self.shadow_sent = 0
        self.shadow_transport = None  # 已 connect 到影子接收器的独立套接字


class ClientProtocol(asyncio.DatagramProtocol):
    """面向遥测终端的 UDP 端。"""

    def __init__(self, state):
        self.state = state

    def datagram_received(self, data, addr):
        self.state.on_client_datagram(data, addr)

    def error_received(self, exc):
        pass


class UpstreamProtocol(asyncio.DatagramProtocol):
    """每个会话一条、已 connect 到其接收器的上游 UDP 端。
    只有该会话对应接收器的回包会到达这里。"""

    def __init__(self, state, shadow=False):
        self.state = state
        self.session = None
        self.shadow = shadow

    def datagram_received(self, data, addr):
        sess = self.session
        if sess is not None:
            self.state.on_upstream_datagram(sess, data, shadow=self.shadow)

    def error_received(self, exc):
        pass  # 如对端 ICMP 不可达，忽略


class ProxyState:
    def __init__(self, store):
        self.store = store
        self.loop = None
        self.client_transport = None
        self.sessions = {}  # client_addr -> Session（仅事件循环线程修改）
        self.lock = threading.RLock()
        self.next_session_id = 1
        self.next_check_id = 1
        self.ports = PortAllocator(UPSTREAM_PORT_MIN, UPSTREAM_PORT_MAX, UPSTREAM_PORT_COOLDOWN)
        self.check = None  # 当前影子校验（armed/active），ended 后换到 last_check
        self.last_check = None

    async def start(self):
        transport, _ = await self.loop.create_datagram_endpoint(
            lambda: ClientProtocol(self),
            local_addr=(UDP_LISTEN_HOST, UDP_LISTEN_PORT),
        )
        self.client_transport = transport
        self.loop.create_task(self._sweeper())
        print(f"[proxy] 客户端 UDP 监听 {UDP_LISTEN_HOST}:{UDP_LISTEN_PORT}", flush=True)
        print(f"[proxy] 空闲会话超时 {SESSION_IDLE_TIMEOUT}s，当前目标 {self.store.get_target()}",
              flush=True)

    # ---- 影子校验控制 ----
    def start_check(self):
        """开启一次影子校验：两侧固定为当前目标与另一台接收器。

        说明：本次比较的两侧在开启时即固定，之后切换目标只影响新会话的去向，
        不会改变本次校验正在比较的两侧。
        """
        with self.lock:
            if self.check is not None:
                if self.check.draining or self.check.state == "armed":
                    # 上一次已请求结束或还没绑定会话：立即定稿，迟到的回包不会串入新校验。
                    self._end_check(self.check, self.check.end_reason or "被新的校验替换")
                else:
                    return None, "已有影子校验在进行中"
            fixed = self.store.get_target()
            other = sorted(k for k in RECEIVERS if k != fixed)
            if not other:
                return None, "没有可用于对比的接收器"
            check = ShadowCheck(self.next_check_id, fixed, other[0],
                                SHADOW_WINDOW, SHADOW_LIMIT)
            self.next_check_id += 1
            self.check = check
            self.last_check = check
        self.store.log("event", receiver=fixed,
                       note=f"影子校验 #{check.id} 开启：固定侧 {fixed} / 影子侧 {check.shadow}，"
                            f"窗口 {SHADOW_WINDOW:g}s，最多比较 {SHADOW_LIMIT} 条")
        return check, None

    def _end_check(self, check, reason):
        """立即定稿（调用方持有 self.lock）。关闭镜像套接字，迟到回包不会再被接收。"""
        if check.state != "ended":
            check.evaluate(reason)
            check.state = "ended"
            check.draining = False
        transport, check.transport = check.transport, None
        if transport is not None:
            transport.close()
        if self.check is check:
            self.check = None
        self.store.log("event", receiver=check.fixed,
                       note=f"影子校验 #{check.id} 结束（{reason}）：结论 {check.verdict}，"
                            f"{check.detail}")

    def _replies_complete(self, check):
        return (check.sent > 0
                and len(check.fixed_replies) >= check.sent
                and len(check.shadow_replies) >= check.sent)

    def request_end(self, check, reason):
        """请求结束校验（调用方持有 self.lock）。

        仍在采集中时先进入短暂收尾窗口，等待在途回包到达后再定稿；
        两侧回包已齐全或收尾窗口已到则立即定稿。
        """
        if check.state == "ended":
            return
        now = time.monotonic()
        if (check.state != "active" or SHADOW_DRAIN <= 0
                or self._replies_complete(check)):
            self._end_check(check, reason)
            return
        check.draining = True
        check.end_reason = reason
        check.drain_until = now + SHADOW_DRAIN

    def end_check(self, reason):
        with self.lock:
            if self.check is not None:
                self.request_end(self.check, reason)

    def _on_shadow_session_closed(self, sess, reason):
        check = sess.shadow
        if check is not None and self.check is check:
            self._end_check(check, reason)

    def snapshot_check(self):
        with self.lock:
            c = self.check or self.last_check
            return c.snapshot() if c else None

    # ---- 客户端方向 ----
    def on_client_datagram(self, data, addr):
        now = time.monotonic()
        with self.lock:
            sess = self.sessions.get(addr)
            if sess is not None and sess.closed:
                sess = None
            if (sess is not None and sess.state == "open"
                    and now - sess.last_activity > SESSION_IDLE_TIMEOUT):
                self._close_session(sess, "idle-timeout")
                sess = None
            if sess is None:
                # 建会话：固定到“当前”目标接收器
                sess = Session(self.next_session_id, addr)
                self.next_session_id += 1
                self.sessions[addr] = sess
                # 影子校验：窗口内第一个新建会话绑定为影子会话。
                # 该会话的“固定侧”锁定为校验开启时的目标，即使期间切换了目标也不改，
                # 保证本次比较的两侧始终是开启时确定的那两台接收器。
                check = self.check
                if (check is not None and self.check is check
                        and check.state == "armed"
                        and time.monotonic() <= check.deadline):
                    check.state = "active"
                    check.client = fmt_addr(addr)
                    check.session = sess
                    sess.shadow = check
                sess.pending.append(data)
                self.loop.create_task(self._open_session(sess))
                return
            sess.last_activity = now
            if sess.state == "opening":
                sess.pending.append(data)
                return
        self._forward(sess, data)

    async def _open_session(self, sess):
        # 建会话时固定接收器；影子会话固定到校验开启时的“固定侧”，不随切换改变。
        bound = sess.shadow is not None
        check = sess.shadow
        target = check.fixed if bound else self.store.get_target()
        host, port = RECEIVERS[target]
        client = fmt_addr(sess.client_addr)
        try:
            transport = None
            upstream_port = None
            last_err = None
            for _ in range(8):
                upstream_port = self.ports.allocate()
                try:
                    transport, proto = await self.loop.create_datagram_endpoint(
                        lambda: UpstreamProtocol(self),
                        local_addr=("0.0.0.0", upstream_port),
                        remote_addr=(host, port),  # connect：只收该接收器的回包
                    )
                    break
                except OSError as exc:
                    last_err = exc
                    self.ports.discard(upstream_port)
            if transport is None:
                raise last_err or RuntimeError("无可用上游端口")
            with self.lock:
                if sess.closed:
                    transport.close()
                    self.ports.release(upstream_port)
                    self._on_shadow_session_closed(sess, "open-failed")
                    return
                sess.receiver_name = target
                sess.receiver_addr = (host, port)
                sess.upstream_port = upstream_port
                sess.transport = transport
                sess.state = "open"
                proto.session = sess
                if bound and (check is None or self.check is not check
                              or check.state != "active"):
                    bound = False
                pending, sess.pending = sess.pending, []
            self.store.log("event", client=client, session_id=sess.id, receiver=target,
                           note=f"会话建立，固定到接收器 {target}（上游端口 {upstream_port}）")
            if bound:
                self.store.log("event", client=client, session_id=sess.id, receiver=check.fixed,
                               note=f"影子校验 #{check.id} 绑定此会话：固定侧 {check.fixed} / "
                                    f"影子侧 {check.shadow}")
                await self._open_shadow(sess, check, client)
            for data in pending:
                self._forward(sess, data)
        except Exception as exc:
            self.store.log("event", client=client, session_id=sess.id,
                           note=f"会话建立失败: {exc}")
            self.close_session(sess, f"open-failed: {exc}")

    async def _open_shadow(self, sess, check, client):
        """为影子校验建立独立镜像套接字（不阻塞正常转发：失败只记发送失败）。"""
        host, port = RECEIVERS[check.shadow]
        try:
            transport, proto = await self.loop.create_datagram_endpoint(
                lambda: UpstreamProtocol(self, shadow=True),
                local_addr=("0.0.0.0", 0),
                remote_addr=(host, port),  # connect：只收影子接收器的回包
            )
            with self.lock:
                if sess.closed or check.state != "active" or self.check is not check:
                    transport.close()
                    return
                sess.shadow_transport = transport
                proto.session = sess
                check.transport = transport
        except Exception as exc:
            with self.lock:
                if check.state == "active" and self.check is check:
                    check.send_errors = max(check.send_errors, 1)
            self.store.log("event", client=client, session_id=sess.id,
                           receiver=check.shadow,
                           note=f"影子校验 #{check.id} 镜像套接字建立失败：{exc}")

    def _forward(self, sess, data):
        with self.lock:
            if sess.closed or sess.transport is None:
                return
            sess.last_activity = time.monotonic()
            check = sess.shadow
            mirror = (check is not None and self.check is check
                      and check.state == "active" and not check.draining
                      and self.loop is not None)
            if mirror:
                n = check.sent + 1
                if n <= check.limit:
                    check.sent = n
                    sess.shadow_sent += 1
                else:
                    check.overflow += 1
                    mirror = False
        try:
            sess.transport.sendto(data)  # 已 connect，原样发往该会话的固定接收器
        except Exception as exc:
            self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                           note=f"转发失败: {exc}")
            if mirror:
                with self.lock:
                    check.send_errors += 1
            return
        self.store.log("c2r", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data),
                       note=f"影子校验 #{check.id} 镜像" if mirror else "")
        if mirror:
            self._forward_shadow(sess, check, data)

    def _forward_shadow(self, sess, check, data):
        """把同一条上行原样发往影子接收器；套接字未就绪/失败都记为发送失败证据。"""
        client = fmt_addr(sess.client_addr)
        try:
            transport = sess.shadow_transport
            if transport is None:
                raise RuntimeError("镜像套接字未就绪")
            transport.sendto(data)
        except Exception as exc:
            with self.lock:
                if check.state == "active" and self.check is check:
                    check.send_errors += 1
            self.store.log("event", client=client, session_id=sess.id, receiver=check.shadow,
                           note=f"影子校验 #{check.id} 镜像发送失败：{exc}")
            return
        self.store.log("c2r", client=client, session_id=sess.id, receiver=check.shadow,
                       payload=preview(data), note=f"影子校验 #{check.id} 镜像")

    # ---- 接收器方向 ----
    def on_upstream_datagram(self, sess, data, shadow=False):
        if shadow:
            self._on_shadow_reply(sess, data)
            return
        now = time.monotonic()
        with self.lock:
            if sess.closed or self.sessions.get(sess.client_addr) is not sess:
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="旧会话迟到的回包，已丢弃")
                return
            if now - sess.last_activity > SESSION_IDLE_TIMEOUT:
                self._close_session(sess, "idle-timeout")
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="回包到达时会话已过期，已丢弃")
                return
            sess.last_activity = now
            # 影子校验：记录固定侧回包原文用于与影子侧比较（仍照常下发终端）。
            check = sess.shadow
            if (check is not None and self.check is check and check.state == "active"
                    and self.sessions.get(sess.client_addr) is sess):
                check.fixed_replies.append(data)
        self.client_transport.sendto(data, sess.client_addr)  # 原样送回该客户端
        self.store.log("r2c", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data))
        if sess.shadow is not None:
            self._finish_drain_if_complete(sess.shadow)

    def _on_shadow_reply(self, sess, data):
        """影子接收器的回包：只做统计，绝不下发给终端。"""
        check = sess.shadow
        with self.lock:
            if (check is None or self.check is not check or check.state != "active"
                    or self.sessions.get(sess.client_addr) is not sess):
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=check.shadow if check else "",
                               payload=preview(data),
                               note="影子回包迟到或校验已结束，已丢弃")
                return
            check.shadow_replies.append(data)
        self.store.log("r2s", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=check.shadow, payload=preview(data),
                       note=f"影子校验 #{check.id} 回包（仅供比较，不下发）")
        self._finish_drain_if_complete(check)

    def _finish_drain_if_complete(self, check):
        """收尾期内两侧回包一旦收齐就立即定稿。"""
        with self.lock:
            if (self.check is check and check.state == "active" and check.draining
                    and self._replies_complete(check)):
                self._end_check(check, check.end_reason or "收尾完成")

    # ---- 会话生命周期 ----
    def _close_session(self, sess, reason):
        """调用方需持有 self.lock。"""
        if sess.closed:
            return
        sess.closed = True
        sess.state = "closed"
        if self.sessions.get(sess.client_addr) is sess:
            del self.sessions[sess.client_addr]
        transport, sess.transport = sess.transport, None
        port, sess.upstream_port = sess.upstream_port, None
        shadow_transport, sess.shadow_transport = sess.shadow_transport, None
        if transport is not None:
            transport.close()  # 迟到的回包到此端口将被系统丢弃
        if port is not None:
            self.ports.release(port)  # 进入冷却期，冷却内不再分配
        if shadow_transport is not None:
            shadow_transport.close()  # 镜像端口关闭，影子回包全部丢弃
        self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, note=f"会话关闭（{reason}）")
        # 影子会话会话过期/关闭：本次校验随之结束，迟到的回包不会串入下一次校验。
        self._on_shadow_session_closed(sess, f"影子会话关闭（{reason}）")

    def close_session(self, sess, reason):
        with self.lock:
            self._close_session(sess, reason)

    async def _sweeper(self):
        while True:
            await asyncio.sleep(0.25)
            now = time.monotonic()
            with self.lock:
                expired = [s for s in self.sessions.values()
                           if not s.closed and now - s.last_activity > SESSION_IDLE_TIMEOUT]
                for sess in expired:
                    self._close_session(sess, "idle-timeout")
                # 收尾中的校验：在途回包收齐或收尾窗口到期即定稿。
                check = self.check
                if check is not None and check.draining:
                    if (self._replies_complete(check)
                            or now >= check.drain_until):
                        self._end_check(check, check.end_reason or "收尾完成")
                # 校验总超时：窗口到期仍未绑定时收尾为“无会话”，
                # 已绑定的会话则按总时长上限收尾，避免无限期占用镜像资源。
                check = self.check
                if check is not None:
                    if check.state == "armed" and now > check.deadline:
                        self._end_check(check, "校验窗口内没有建立新会话")
                    elif check.state == "active" and now > check.deadline_extra:
                        self._end_check(check, "校验总超时")

    def snapshot_sessions(self):
        now = time.monotonic()
        with self.lock:
            return [
                {
                    "id": s.id,
                    "client": fmt_addr(s.client_addr),
                    "receiver": s.receiver_name or "?",
                    "upstream_port": s.upstream_port,
                    "state": s.state,
                    "idle": round(now - s.last_activity, 1),
                    "age": round(now - s.created, 1),
                    "shadow": s.shadow is not None,
                    "shadow_sent": s.shadow_sent,
                }
                for s in sorted(self.sessions.values(), key=lambda x: x.id)
            ]


# ---------------- Flask 网页 ----------------
store = Store(DB_PATH)
state = ProxyState(store)
app = Flask(__name__)
app.secret_key = os.urandom(16)

_last_test = {}
_last_test_lock = threading.Lock()

DIRECTION_LABELS = {
    "c2r": "客户端→接收器",
    "r2c": "接收器→客户端",
    "r2s": "影子回包（仅比较）",
    "event": "事件",
    "drop": "丢弃",
}

VERDICT_LABELS = {
    "consistent": "一致",
    "different": "差异",
    "insufficient": "证据不足",
}


def _format_message(m):
    m = dict(m)
    m["ts"] = fmt_ts(m["ts"])
    m["direction"] = DIRECTION_LABELS.get(m["direction"], m["direction"])
    return m


@app.get("/")
def index():
    with _last_test_lock:
        lt = dict(_last_test) if _last_test else None
    return render_template(
        "index.html",
        target=store.get_target(),
        receivers={k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        idle_timeout=SESSION_IDLE_TIMEOUT,
        udp_listen_port=UDP_LISTEN_PORT,
        last_test=lt,
        check=state.snapshot_check(),
        shadow_window=SHADOW_WINDOW,
        shadow_limit=SHADOW_LIMIT,
        verdict_labels=VERDICT_LABELS,
    )


@app.get("/api/state")
def api_state():
    with _last_test_lock:
        lt = dict(_last_test) if _last_test else None
    return jsonify({
        "target": store.get_target(),
        "receivers": {k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        "idle_timeout": SESSION_IDLE_TIMEOUT,
        "sessions": state.snapshot_sessions(),
        "messages": [_format_message(m) for m in store.recent_messages(100)],
        "last_test": lt,
        "shadow": state.snapshot_check(),
        "shadow_window": SHADOW_WINDOW,
        "shadow_limit": SHADOW_LIMIT,
        "verdict_labels": VERDICT_LABELS,
    })


@app.post("/shadow/start")
def shadow_start():
    check, err = state.start_check()
    if check is None:
        flash(f"无法开启影子校验：{err}", "error")
    else:
        flash(f"影子校验 #{check.id} 已开启：窗口 {SHADOW_WINDOW:g} 秒内请发送试传报文建立会话，"
              f"比较固定侧 {check.fixed} 与影子侧 {check.shadow} 的前 {SHADOW_LIMIT} 条回包。", "ok")
    return redirect(url_for("index"))


@app.post("/shadow/stop")
def shadow_stop():
    state.end_check("网页手动结束")
    flash("影子校验已结束，镜像已停止。", "ok")
    return redirect(url_for("index"))


@app.post("/switch")
def switch():
    target = request.form.get("target", "")
    try:
        store.set_target(target)
        store.log("event", note=f"网页切换目标为接收器 {target}")
        flash(f"已切换到接收器 {target}。新会话走 {target}，旧会话继续走原接收器直至过期。", "ok")
    except Exception as exc:
        flash(f"切换保存失败，保留原目标 {store.get_target()}。原因：{exc}", "error")
    return redirect(url_for("index"))


@app.post("/test")
def test():
    global _last_test
    result = run_test(
        payload=request.form.get("payload", "PING"),
        delay_s=request.form.get("delay", "").strip(),
        client_port_s=request.form.get("client_port", "").strip(),
        wait_s=request.form.get("wait", "10").strip(),
    )
    with _last_test_lock:
        _last_test = result
    return redirect(url_for("index"))


def run_test(payload, delay_s, client_port_s, wait_s):
    """从网页后端经代理发一条试传报文并等待回包。"""
    try:
        wait = min(max(float(wait_s), 0.5), 60.0)
    except ValueError:
        wait = 10.0
    wire = payload
    try:
        delay = float(delay_s)
        if delay > 0:
            wire = f"DELAY:{delay}:{payload}"  # 接收器识别的延迟回包指令
    except ValueError:
        pass
    data = wire.encode()
    result = {"time": fmt_ts(time.time()), "payload": wire, "ok": False,
              "reply": "", "error": "", "rtt": None, "client": "", "receiver": None}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if client_port_s:
            sock.bind(("0.0.0.0", int(client_port_s)))
        sock.settimeout(wait)
        sock.connect(("127.0.0.1", UDP_LISTEN_PORT))
        result["client"] = fmt_addr(sock.getsockname())
        t0 = time.monotonic()
        sock.send(data)
        try:
            reply = sock.recv(65535)
            result["ok"] = True
            result["rtt"] = round(time.monotonic() - t0, 3)
            result["reply"] = preview(reply)
        except socket.timeout:
            result["error"] = f"等待回包超时（{wait}s）"
    except Exception as exc:
        result["error"] = f"发送失败：{exc}"
    finally:
        sock.close()
    time.sleep(0.05)  # 等代理把日志落库
    if result["client"]:
        result["receiver"] = store.latest_receiver_for_client(result["client"])
    return result


# ---------------- 启动：asyncio 线程 + Flask 主线程 ----------------
def _run_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    state.loop = loop
    loop.run_until_complete(state.start())
    loop.run_forever()


def main():
    t = threading.Thread(target=_run_loop, name="udp-proxy-loop", daemon=True)
    t.start()
    print(f"[proxy] 操作网页 http://{HTTP_HOST}:{HTTP_PORT}", flush=True)
    app.run(host=HTTP_HOST, port=HTTP_PORT, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
