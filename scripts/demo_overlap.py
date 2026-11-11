#!/usr/bin/env python3
"""演示两个采集实例（共用一个 SQLite）发起重叠补采时的 409 与续采流程。

前置：docker compose up --build
  - 实例 A Swagger：http://localhost:8000/docs
  - 实例 B Swagger：http://localhost:8001/docs
  - 样例 OAI 源：  http://localhost:8080（PAGE_DELAY=2s，翻页期间持锁约 6 秒）

本脚本只依赖标准库，可在容器外直接运行：
  python3 scripts/demo_overlap.py
也可通过环境变量改地址：
  API_A=http://localhost:8000 API_B=http://localhost:8001 \
  OAI_ADMIN=http://localhost:8080 python3 scripts/demo_overlap.py

观察点：
1. 实例 A 的补采在慢翻页中持有跨进程采集锁；
2. 实例 B 的重叠请求立即收到 409——不请求 OAI 源、不新增运行记录；
3. A 提交后，B 用相同截止日重新发起即可从 last_until（含当天）续采。
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

API_A = os.environ.get("API_A", "http://localhost:8000")
API_B = os.environ.get("API_B", "http://localhost:8001")
OAI_ADMIN = os.environ.get("OAI_ADMIN", "http://localhost:8080")


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


def _admin(path: str) -> dict:
    with urllib.request.urlopen(f"{OAI_ADMIN}{path}", timeout=10) as resp:
        return json.loads(resp.read().decode())


def _print(title: str, status: int, body: dict) -> None:
    print(f"\n--- {title}（HTTP {status}）---")
    print(json.dumps(body, ensure_ascii=False, indent=2))


def main() -> int:
    print("== 重置样例源并确认翻页延迟（放慢翻页，制造重叠窗口） ==")
    _admin("/admin/reset")
    print(_admin("/admin/slow?seconds=2"))

    result_a: dict = {}
    error_a: list[Exception] = []

    def _harvest_a() -> None:
        try:
            status, body = _request(
                "POST", f"{API_A}/api/v1/harvest",
                {"from": "2026-10-01", "until": "2026-10-27"},
            )
            result_a["status"] = status
            result_a["body"] = body
        except Exception as exc:  # noqa: BLE001
            error_a.append(exc)

    print("\n== 1) 实例 A 发起全区间补采（后台线程，慢翻页持锁） ==")
    thread = threading.Thread(target=_harvest_a)
    thread.start()
    time.sleep(3)  # 等待 A 取到锁并进入翻页

    requests_before = _admin("/admin/state")["list_records_requests"]
    runs_before = _request("GET", f"{API_A}/api/v1/runs")[1]

    print("\n== 2) 实例 B 同时发起重叠补采：预期立即 409 ==")
    status_b, body_b = _request(
        "POST", f"{API_B}/api/v1/harvest",
        {"from": "2026-10-01", "until": "2026-10-20"},
    )
    _print("实例 B 重叠请求", status_b, body_b)
    assert status_b == 409, f"预期 409，实际 {status_b}"

    requests_after = _admin("/admin/state")["list_records_requests"]
    runs_after = _request("GET", f"{API_A}/api/v1/runs")[1]
    print(f"OAI 源 ListRecords 请求数：{requests_before} -> {requests_after}（应不变）")
    print(f"运行记录条数：{len(runs_before)} -> {len(runs_after)}（应不变）")
    assert requests_after == requests_before, "409 请求不应触达 OAI 源"
    assert len(runs_after) == len(runs_before), "409 请求不应新增运行记录"

    print("\n== 3) 采集未提交期间，目录/进度照常查询，只返回已提交数据 ==")
    print("progress:", _request("GET", f"{API_B}/api/v1/progress")[1])

    thread.join(timeout=60)
    assert not error_a, error_a
    _print("实例 A 补采结果", result_a["status"], result_a["body"])
    assert result_a["status"] == 200

    print("\n== 4) A 提交后，B 收到 409 的那笔用相同截止日重新发起即续采 ==")
    status_r, body_r = _request(
        "POST", f"{API_B}/api/v1/harvest", {"until": "2026-10-27"}
    )
    _print("实例 B 续采（只填截止日，自动取 last_until 含当天）", status_r, body_r)
    assert status_r == 200

    print("\nprogress:", _request("GET", f"{API_A}/api/v1/progress")[1])
    print("\n演示完成：重叠请求被 409 拦截、未触源未留痕；锁释放后正常续采。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
