#!/usr/bin/env python3
"""影子校验端到端验证：不依赖 Docker。

启动两个接收器与代理，覆盖：
- 开启后限时窗口内建立的新会话，前 16 条上行同时发往固定/另一台接收器；
- 只有固定接收器的回包下发终端，另一侧只做统计；
- 两侧回包原文/次数一致 → 一致；内容不同 → 差异；
- 乱序、同文重发不误判；缺包、超过 16 条 → 证据不足；
- 多出的报文仍按原规则透传；切换目标不改变本次比较的两侧；
- 校验结束/会话过期/重启后停止镜像，迟到回包不串入下一次。

用法：python3 scripts/verify_shadow.py
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROXY_PY = os.path.join(ROOT, "proxy", "app.py")
RECEIVER_PY = os.path.join(ROOT, "receiver", "receiver.py")

UDP_PORT = 19100
HTTP_PORT = 18180
BASE = f"http://127.0.0.1:{HTTP_PORT}"

_failures = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def http_get(path):
    with urllib.request.urlopen(BASE + path, timeout=10) as r:
        return json.load(r)


def http_post(path, data=None):
    body = urllib.parse.urlencode(data or {}).encode()
    with urllib.request.urlopen(BASE + path, data=body, timeout=40) as r:
        return r.status


def wait_http_up(timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            http_get("/api/state")
            return True
        except Exception:
            time.sleep(0.3)
    return False


def start_receiver(name, port, logdir, extra_env=None):
    env = dict(os.environ, RECEIVER_NAME=name, LISTEN_PORT=str(port), REPLY_DELAY="0")
    env.update(extra_env or {})
    log = open(os.path.join(logdir, f"receiver-{name}.log"), "w")
    return subprocess.Popen([sys.executable, RECEIVER_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_proxy(db_path, logdir, tag):
    env = dict(
        os.environ,
        UDP_LISTEN_PORT=str(UDP_PORT), HTTP_PORT=str(HTTP_PORT),
        RECEIVER_A_HOST="127.0.0.1", RECEIVER_A_PORT="19101",
        RECEIVER_B_HOST="127.0.0.1", RECEIVER_B_PORT="19102",
        SESSION_IDLE_TIMEOUT="5", DB_PATH=db_path,
        SHADOW_WINDOW="10", SHADOW_LIMIT="16", SHADOW_MAX_DURATION="40",
    )
    log = open(os.path.join(logdir, f"proxy-{tag}.log"), "w")
    return subprocess.Popen([sys.executable, PROXY_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def udp_client():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("127.0.0.1", UDP_PORT))
    return s


def send_recv(sock, payload: bytes, timeout=4.0):
    sock.settimeout(timeout)
    sock.send(payload)
    return sock.recv(65535)


def sock_close(s):
    try:
        s.close()
    except Exception:
        pass


def main():
    tmp = tempfile.mkdtemp(prefix="shadow-verify-")
    db_path = os.path.join(tmp, "proxy.db")
    procs = [start_receiver("A", 19101, tmp), start_receiver("B", 19102, tmp)]
    proxy = start_proxy(db_path, tmp, "run1")
    procs.append(proxy)
    try:
        if not wait_http_up():
            check("代理启动", False, f"日志见 {tmp}")
            return 1
        print(f"--- 环境就绪（日志目录 {tmp}）---")

        st = http_get("/api/state")
        check("初始目标为 A", st["target"] == "A")
        check("初始无影子校验", st["shadow"] is None)

        # ---------- 1. 一致 ----------
        http_post("/shadow/start")
        sh = http_get("/api/state")["shadow"]
        check("开启后处于等待会话状态", sh and sh["state"] == "armed", str(sh))
        check("比较两侧为当前目标 A 与另一台 B",
              sh and sh["fixed"] == "A" and sh["shadow"] == "B", str(sh))
        c1 = udp_client()
        got = [send_recv(c1, p) for p in (b"alpha", b"beta", b"beta", b"gamma")]
        check("终端只收到固定侧回包（含重复原文）", got == [b"alpha", b"beta", b"beta", b"gamma"],
              str(got))
        http_post("/shadow/stop")
        sh = http_get("/api/state")["shadow"]
        check("一致：结论为一致", sh["verdict"] == "consistent", str(sh))
        check("一致：固定侧 4 条、影子侧 4 条",
              sh["fixed_count"] == 4 and sh["shadow_count"] == 4, str(sh))
        check("一致：详情含原文与次数一致", "一致" in sh["detail"], str(sh))
        sock_close(c1)
        time.sleep(0.3)

        # ---------- 2. 网页试传报文作为影子会话 + 同文重发/二进制/乱序 ----------
        http_post("/shadow/start")
        http_post("/test", {"payload": "repeat", "wait": "3", "delay": "0", "client_port": ""})
        http_post("/shadow/stop")
        sh = http_get("/api/state")["shadow"]
        check("试传报文可绑定影子会话", sh and sh["sent"] >= 1, str(sh))

        http_post("/shadow/start")
        c2 = udp_client()
        # 前 16 条内：同文重发 3 次 + 二进制 2 次 + 不同延迟制造乱序
        seq = [b"dup", b"dup", b"dup", bytes(range(16)), bytes(range(16)),
               b"DELAY:0.4:r1", b"DELAY:0.1:r2"]
        for p in seq:
            send_recv(c2, p)
        http_post("/shadow/stop")
        sh = http_get("/api/state")["shadow"]
        check("同文重发/二进制/乱序：两侧判一致", sh["verdict"] == "consistent", str(sh))
        check("同文重发：两侧均为 7 条",
              sh["fixed_count"] == 7 and sh["shadow_count"] == 7, str(sh))
        sock_close(c2)
        time.sleep(0.3)

        # ---------- 3. 差异（本地接收器 B 返回不同内容） ----------
        # 重启接收器 B，带 REPLY_TEMPLATE，制造内容差异。
        procs[1].terminate()
        procs[1].wait(timeout=5)
        procs[1] = start_receiver("B", 19102, tmp, {"REPLY_TEMPLATE": "B:{payload}"})
        time.sleep(0.5)
        http_post("/shadow/start")
        c3 = udp_client()
        m = send_recv(c3, b"diffme")
        check("差异：终端仍只收到固定侧 A 的原文", m == b"diffme", str(m))
        http_post("/shadow/stop")
        sh = http_get("/api/state")["shadow"]
        check("差异：结论为差异", sh["verdict"] == "different", str(sh))
        check("差异：固定侧原文在列", "diffme" in " ".join(sh["fixed_replies"]), str(sh))
        check("差异：影子侧自定义内容在列", any("B:" in x for x in sh["shadow_replies"]), str(sh))
        sock_close(c3)
        time.sleep(0.3)

        # 恢复 B 原样回显
        procs[1].terminate()
        procs[1].wait(timeout=5)
        procs[1] = start_receiver("B", 19102, tmp)
        time.sleep(0.5)

        # ---------- 4. 超过 16 条 → 证据不足，且多出的仍透传 ----------
        http_post("/shadow/start")
        c4 = udp_client()
        replies = []
        for i in range(18):
            replies.append(send_recv(c4, f"n{i}".encode()))
        check("超 16 条：18 条均有回包（多出的仍透传）",
              replies == [f"n{i}".encode() for i in range(18)], str(len(replies)))
        time.sleep(0.3)
        sh = http_get("/api/state")["shadow"]
        check("超 16 条：已镜像为 16", sh["sent"] == 16, str(sh))
        check("超 16 条：多出 2 条被记录", sh["overflow"] == 2, str(sh))
        # 让会话空闲过期以冻结结论（保持窗口也允许手动结束）
        http_post("/shadow/stop")
        sh = http_get("/api/state")["shadow"]
        check("超 16 条：结论为证据不足", sh["verdict"] == "insufficient", str(sh))
        check("超 16 条：详情说明多出 2 条", "多出 2 条" in sh["detail"], str(sh))
        sock_close(c4)
        time.sleep(0.3)

        # ---------- 5. 缺包 → 证据不足 ----------
        # 把影子侧 B 的默认延迟设为很长：镜像发出但回包在校验结束前收不到 → 缺包。
        procs[1].terminate()
        procs[1].wait(timeout=5)
        procs[1] = start_receiver("B", 19102, tmp, {"REPLY_DELAY": "30"})
        time.sleep(0.5)
        http_post("/shadow/start")
        c5 = udp_client()
        r = send_recv(c5, b"lost-shadow")
        check("缺包：终端仍收到固定侧回包", r == b"lost-shadow", str(r))
        http_post("/shadow/stop")
        sh = http_get("/api/state")["shadow"]
        check("缺包：结论为证据不足", sh["verdict"] == "insufficient", str(sh))
        check("缺包：影子侧缺 1 条", sh["shadow_missing"] == 1, str(sh))
        sock_close(c5)
        time.sleep(0.3)
        procs[1].terminate()
        procs[1].wait(timeout=5)
        procs[1] = start_receiver("B", 19102, tmp)
        time.sleep(0.5)

        # ---------- 6. 切换不改变本次比较两侧 ----------
        http_post("/shadow/start")
        sh = http_get("/api/state")["shadow"]
        check("切换前比较两侧 A/B", sh["fixed"] == "A" and sh["shadow"] == "B", str(sh))
        http_post("/switch", {"target": "B"})
        sh = http_get("/api/state")["shadow"]
        check("切换后本次校验两侧不变", sh["fixed"] == "A" and sh["shadow"] == "B", str(sh))
        c6 = udp_client()
        got = send_recv(c6, b"switch-mid")
        check("切换后进行中的校验：终端收到固定侧 A 回包", got == b"switch-mid", str(got))
        http_post("/shadow/stop")
        sh = http_get("/api/state")["shadow"]
        check("切换中校验：仍按 A/B 比较且一致", sh["verdict"] == "consistent", str(sh))
        sock_close(c6)
        time.sleep(0.3)
        # 切回 A，便于后续
        http_post("/switch", {"target": "A"})

        # ---------- 7. 迟到回包不串入下一次校验 ----------
        # 影子侧 B 延迟 6 秒回包；本次校验很快结束（手动），迟到的回包应被丢弃，
        # 下一次校验不应把旧回包计入。
        procs[1].terminate()
        procs[1].wait(timeout=5)
        procs[1] = start_receiver("B", 19102, tmp, {"REPLY_DELAY": "6"})
        time.sleep(0.5)
        http_post("/shadow/start")
        c7 = udp_client()
        got = send_recv(c7, b"late-shadow")
        check("迟到场景：终端收到固定侧回包", got == b"late-shadow", str(got))
        http_post("/shadow/stop")
        # 立即开启下一次校验（新客户端会话），窗口内不发送
        time.sleep(0.3)
        http_post("/shadow/start")
        sh_next = http_get("/api/state")["shadow"]
        # 等待旧回包应在的时刻过去
        time.sleep(7)
        sh_next = http_get("/api/state")["shadow"]
        check("迟到回包：下一次校验未收到旧影子回包",
              sh_next and sh_next["shadow_count"] == 0, str(sh_next))
        check("迟到回包：下一次校验（无会话）为证据不足",
              sh_next and sh_next["verdict"] == "insufficient", str(sh_next))
        http_post("/shadow/stop")
        sock_close(c7)
        time.sleep(0.3)
        procs[1].terminate()
        procs[1].wait(timeout=5)
        procs[1] = start_receiver("B", 19102, tmp)
        time.sleep(0.5)

        # ---------- 8. 窗口内无新会话 → 证据不足 ----------
        http_post("/shadow/start")
        time.sleep(1.0)
        # 现有旧会话继续收发不影响新校验（旧会话不绑定）
        http_post("/shadow/stop")
        sh = http_get("/api/state")["shadow"]
        check("窗口内无会话：证据不足", sh["verdict"] == "insufficient", str(sh))

        # ---------- 9. 重启后停止镜像、下次比较从干净状态开始 ----------
        http_post("/shadow/start")
        c9 = udp_client()
        send_recv(c9, b"pre-restart-shadow")
        time.sleep(0.3)
        proxy.terminate()
        proxy.wait(timeout=10)
        proxy = start_proxy(db_path, tmp, "run2")
        procs.append(proxy)
        check("重启后代理恢复", wait_http_up())
        st = http_get("/api/state")
        check("重启后不残留进行中的校验", st["shadow"] is None, str(st.get("shadow")))
        # 重启后新校验应仍可用且从零开始
        http_post("/shadow/start")
        sh = http_get("/api/state")["shadow"]
        check("重启后可重新开启校验", sh and sh["state"] == "armed" and sh["sent"] == 0, str(sh))
        c9b = udp_client()
        got = send_recv(c9b, b"after-restart-shadow")
        check("重启后新校验正常镜像", got == b"after-restart-shadow", str(got))
        http_post("/shadow/stop")
        sock_close(c9)
        sock_close(c9b)

    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    print("---")
    if _failures:
        print(f"共 {len(_failures)} 项失败: {_failures}")
        return 1
    print("影子校验验证全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
