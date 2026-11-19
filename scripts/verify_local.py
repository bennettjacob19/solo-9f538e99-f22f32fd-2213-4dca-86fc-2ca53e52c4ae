#!/usr/bin/env python3
"""本地端到端验证：不依赖 Docker，直接以子进程启动两个接收器和代理，
逐项验证需求行为。用法：python3 scripts/verify_local.py
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

UDP_PORT = 19000
HTTP_PORT = 18080
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


def http_post(path, data):
    with urllib.request.urlopen(
        BASE + path, data=urllib.parse.urlencode(data).encode(), timeout=40
    ) as r:
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


def udp_client():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("127.0.0.1", UDP_PORT))
    return s


def send_recv(sock, payload: bytes, timeout=5.0):
    sock.settimeout(timeout)
    sock.send(payload)
    return sock.recv(65535)


def log_entries(client=None, direction=None, text=None):
    msgs = http_get("/api/state")["messages"]
    out = []
    for m in msgs:
        if client and m["client"] != client:
            continue
        if direction and m["direction"] != direction:
            continue
        if text and text not in m["payload"] and text not in m["note"]:
            continue
        out.append(m)
    return out


def start_receiver(name, port, logdir):
    env = dict(os.environ, RECEIVER_NAME=name, LISTEN_PORT=str(port), REPLY_DELAY="0")
    log = open(os.path.join(logdir, f"receiver-{name}.log"), "w")
    return subprocess.Popen([sys.executable, RECEIVER_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_proxy(db_path, logdir, tag):
    env = dict(
        os.environ,
        UDP_LISTEN_PORT=str(UDP_PORT), HTTP_PORT=str(HTTP_PORT),
        RECEIVER_A_HOST="127.0.0.1", RECEIVER_A_PORT="19001",
        RECEIVER_B_HOST="127.0.0.1", RECEIVER_B_PORT="19002",
        SESSION_IDLE_TIMEOUT="5", DB_PATH=db_path,
    )
    log = open(os.path.join(logdir, f"proxy-{tag}.log"), "w")
    return subprocess.Popen([sys.executable, PROXY_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def main():
    tmp = tempfile.mkdtemp(prefix="proxy-verify-")
    db_path = os.path.join(tmp, "proxy.db")
    procs = [start_receiver("A", 19001, tmp), start_receiver("B", 19002, tmp)]
    proxy = start_proxy(db_path, tmp, "run1")
    procs.append(proxy)
    try:
        if not wait_http_up():
            check("代理启动", False, f"日志见 {tmp}")
            return 1
        print(f"--- 环境就绪（日志目录 {tmp}）---")

        # 1. 初始目标为 A，报文原样往返
        st = http_get("/api/state")
        check("初始目标为 A", st["target"] == "A")
        c1 = udp_client()
        client1 = f"127.0.0.1:{c1.getsockname()[1]}"
        m1 = b"m1-hello"
        check("报文原样回传", send_recv(c1, m1) == m1)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m1-hello")
        check("m1 实际去向为接收器 A", e and e[0]["receiver"] == "A", str(e))

        # 2. 二进制原样透传
        blob = bytes(range(256))
        check("二进制报文原样透传", send_recv(c1, blob) == blob)

        # 3. 网页切换目标到 B
        http_post("/switch", {"target": "B"})
        check("切换后当前目标为 B", http_get("/api/state")["target"] == "B")

        # 4. 旧会话（空闲超时内）仍走原接收器 A
        m2 = b"m2-stick"
        check("旧会话报文仍有回包", send_recv(c1, m2) == m2)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m2-stick")
        check("切换后旧会话继续走接收器 A", e and e[0]["receiver"] == "A", str(e))

        # 5. 空闲 5 秒过期后，同 IP:端口 重建会话走新接收器 B
        print("--- 等待 6 秒让会话空闲过期 ---")
        time.sleep(6)
        m3 = b"m3-newsession"
        check("新会话报文有回包", send_recv(c1, m3) == m3)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m3-newsession")
        check("过期重建的新会话走接收器 B", e and e[0]["receiver"] == "B", str(e))

        # 6. 旧会话迟到的回包：同 IP:端口 重建会话也收不到
        c2 = udp_client()
        client2 = f"127.0.0.1:{c2.getsockname()[1]}"
        t0 = time.time()
        c2.send(b"DELAY:7:late1")          # 接收器 7 秒后才回包；会话 5 秒即过期
        time.sleep(6 - (time.time() - t0))  # 等到会话过期之后
        check("过期后新会话报文有回包", send_recv(c2, b"ping2", timeout=3) == b"ping2")
        # 旧回包在 t0+7 到达，等到 t0+9 确认没有迟到回包送进来
        got_late = False
        c2.settimeout(0.2)
        while time.time() - t0 < 9:
            try:
                if b"late1" in c2.recv(65535):
                    got_late = True
            except socket.timeout:
                pass
        check("旧会话迟到的回包未送达重建的会话", not got_late)
        time.sleep(0.3)
        e = log_entries(client2, "接收器→客户端", "late1")
        check("日志中无 late1 的回包下发记录", not e, str(e))

        # 7. 网页试传报文
        http_post("/test", {"payload": "webtest1", "wait": "5", "delay": "0",
                            "client_port": ""})
        time.sleep(0.2)
        lt = http_get("/api/state")["last_test"]
        check("网页试传成功且回包一致",
              lt and lt["ok"] and lt["reply"] == "webtest1", str(lt))
        check("网页试传显示实际去向为 B", lt and lt["receiver"] == "B", str(lt))

        # 8. 重启：目标保留、会话清空
        c3 = udp_client()
        c3.send(b"before-restart")
        c3.settimeout(3)
        c3.recv(65535)
        proxy.terminate()
        proxy.wait(timeout=10)
        proxy = start_proxy(db_path, tmp, "run2")
        procs.append(proxy)
        check("代理重启后恢复", wait_http_up())
        st = http_get("/api/state")
        check("重启后目标保留为 B", st["target"] == "B")
        check("重启后会话已清空", st["sessions"] == [])
        c3.send(b"after-restart")  # 旧会话若未清空仍在 5 秒空闲窗口内
        c3.settimeout(3)
        c3.recv(65535)
        time.sleep(0.2)
        e = log_entries(f"127.0.0.1:{c3.getsockname()[1]}", "事件", "会话建立")
        check("重启后同客户端重建了全新会话", len(e) >= 1, str(e))

        # 9. 切换保存失败时保留原目标（进程内模拟数据库故障）
        sys.path.insert(0, os.path.join(ROOT, "proxy"))
        os.environ["DB_PATH"] = os.path.join(tmp, "other.db")
        import importlib
        import app as proxy_app
        importlib.reload(proxy_app)
        s = proxy_app.Store(os.path.join(tmp, "t.db"))
        s.set_target("B")
        s._conn.close()  # 模拟存储故障
        failed = False
        try:
            s.set_target("A")
        except Exception:
            failed = True
        check("保存失败时切换报错且保留原目标", failed and s.get_target() == "B")

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
    print("全部验证通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
