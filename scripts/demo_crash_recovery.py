#!/usr/bin/env python3
"""演示采集进程异常退出后，无需人工解锁即可从最后成功截止日续采。

采集锁是 SQLite 库级写事务：持锁进程被 kill（含 kill -9 / 容器重启）后，
SQLite 在下次打开库时自动回滚未提交事务并释放锁，不存在需要人工清理的
“锁文件”。

本脚本只依赖标准库。支持两种环境：

1) Docker Compose（默认）：
   docker compose up --build
   python3 scripts/demo_crash_recovery.py
   脚本会用 `docker compose kill harvester-api-a` 模拟实例 A 崩溃，
   再通过实例 B 续采。

2) 本地进程（无 Docker）：
   LOCAL=1 python3 scripts/demo_crash_recovery.py
   脚本用 tests/_harvest_worker.py 的 crash 模式模拟持锁进程 os._exit。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

API_A = os.environ.get("API_A", "http://localhost:8000")
API_B = os.environ.get("API_B", "http://localhost:8001")
OAI_ADMIN = os.environ.get("OAI_ADMIN", "http://localhost:8080")
LOCAL = os.environ.get("LOCAL") == "1"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _request(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def _wait_healthy(url: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=3) as resp:
                if resp.status == 200:
                    return
        except OSError:
            time.sleep(1)
    raise RuntimeError(f"等待 {url} 健康检查超时")


def _admin(path: str) -> dict:
    with urllib.request.urlopen(f"{OAI_ADMIN}{path}", timeout=10) as resp:
        return json.loads(resp.read().decode())


def _run_worker(mode: str, db_path: str, endpoint: str, *dates: str) -> dict:
    """本地模式：直接用子进程工作进程执行补采/崩溃，不依赖 HTTP API。"""
    proc = subprocess.run(
        [sys.executable, "-I", os.path.join(ROOT, "tests", "_harvest_worker.py"),
         mode, db_path, endpoint, *dates],
        capture_output=True, text=True, check=False,
    )
    events = [
        json.loads(line[len("WORKER "):])
        for line in proc.stdout.splitlines() if line.startswith("WORKER ")
    ]
    return {"returncode": proc.returncode, "events": events}


def main() -> int:
    _admin("/admin/reset")
    _admin("/admin/slow?seconds=0")

    if LOCAL:
        db_path = os.environ.get("APP_DB_PATH", "/tmp/crash-demo/harvester.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        endpoint = os.environ.get("OAI_ENDPOINT", "http://127.0.0.1:8080/oai")

        print("== 1) 先成功采集到 2026-10-10，建立“最后成功截止日” ==")
        first = _run_worker(
            "harvest", db_path, endpoint, "2026-10-01", "2026-10-10"
        )
        print(json.dumps(first["events"][-1], ensure_ascii=False))
        assert first["returncode"] == 0

        print("\n== 2) 启动持锁进程并令其在未提交时 os._exit（模拟崩溃） ==")
        crashed = _run_worker(
            "crash", db_path, endpoint, "2026-10-10", "2026-10-27"
        )
        print(f"崩溃进程退出码：{crashed['returncode']}（预期 42）")
        assert crashed["returncode"] == 42

        print("\n== 3) 不做任何人工解锁，新进程直接从最后成功截止日续采 ==")
        resumed = _run_worker(
            "harvest", db_path, endpoint, "2026-10-27"
        )
        summary = resumed["events"][-1]
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        assert resumed["returncode"] == 0
        assert summary["from_date"] == "2026-10-10", "应自动从 last_until（含当天）续采"
        assert summary["until_date"] == "2026-10-27"
        print("\n演示完成：崩溃后无需人工解锁，已从最后成功截止日无缝续采。")
        return 0

    print("== 1) 先成功采集到 2026-10-10，建立“最后成功截止日” ==")
    status, body = _request(
        "POST", f"{API_A}/api/v1/harvest",
        {"from": "2026-10-01", "until": "2026-10-10"},
    )
    print(json.dumps(body, ensure_ascii=False))
    assert status == 200

    print("\n== 2) 对实例 A 发起慢补采，随即 kill 容器（模拟崩溃，未提交） ==")
    _admin("/admin/slow?seconds=30")

    def _slow_harvest() -> None:
        _request("POST", f"{API_A}/api/v1/harvest",
                 {"from": "2026-10-10", "until": "2026-10-27"})

    thread = threading.Thread(target=_slow_harvest, daemon=True)
    thread.start()
    time.sleep(3)  # 确保已取锁并进入翻页
    subprocess.run(
        ["docker", "compose", "kill", "harvester-api-a"], check=True
    )
    _admin("/admin/slow?seconds=0")
    print("实例 A 已被 kill；未提交事务随进程死亡，SQLite 将自动回滚")
    # 重启 A，恢复双实例部署形态（B 全程在线）
    subprocess.run(
        ["docker", "compose", "up", "-d", "harvester-api-a"], check=True
    )
    _wait_healthy(API_A)

    print("\n== 3) 不做任何人工解锁，实例 B 直接从最后成功截止日续采 ==")
    _wait_healthy(API_B)
    status, body = _request(
        "POST", f"{API_B}/api/v1/harvest", {"until": "2026-10-27"}
    )
    print(json.dumps(body, ensure_ascii=False, indent=2))
    assert status == 200, body
    assert body["from_date"] == "2026-10-10", "应自动从 last_until（含当天）续采"

    progress = _request("GET", f"{API_A}/api/v1/progress")[1]
    runs = _request("GET", f"{API_A}/api/v1/runs")[1]
    print("\nprogress:", progress)
    print("运行历史（崩溃那笔无 running 残留）：")
    for run in runs:
        print(" ", run["id"], run["status"], run["from_date"], "->", run["until_date"])
    assert progress["last_until"] == "2026-10-27"
    print("\n演示完成：崩溃后无需人工解锁，已从最后成功截止日无缝续采。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
