"""跨进程采集锁行为测试。

覆盖需求：
- 同一个 SQLite 库，从读取续采截止日到整批提交/回滚只允许一笔补采；
- 重叠请求立即 409，不请求 OAI-PMH 源、也不新增运行记录；
- 补采未提交时目录/进度只返回已提交数据；
- 采集进程异常退出后无需人工解锁，可从最后成功的截止日续采。

测试通过 subprocess 启动独立 Python 进程持有/竞争同一 SQLite 文件，
复现“两个采集实例落到共用 SQLite 的不同进程”的部署形态。
"""
from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "tests" / "_harvest_worker.py"


def _start_worker(mode, db_path, endpoint, *dates):
    return subprocess.Popen(
        [sys.executable, "-I", str(WORKER), mode, str(db_path), endpoint, *dates],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(ROOT),
    )


def _wait_event(proc, event, timeout=20):
    """读取工作进程输出，直到出现指定事件。"""
    deadline = time.monotonic() + timeout
    lines = []
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                break
            time.sleep(0.02)
            continue
        lines.append(line.strip())
        if line.startswith("WORKER "):
            payload = json.loads(line[len("WORKER "):])
            if payload.get("event") == event:
                return payload, lines
    raise AssertionError(f"未等到事件 {event}，已收到：{lines}")


@pytest.fixture()
def shared_db(tmp_path, oai_server):
    server_mod, endpoint = oai_server
    db_path = tmp_path / "shared.db"

    from app import db as db_mod
    from app import main as main_mod

    db_mod.DB_PATH = str(db_path)
    db_mod.init_db(str(db_path))
    main_mod.OAI_ENDPOINT = endpoint
    with TestClient(main_mod.app) as api:
        yield api, db_mod, server_mod, endpoint, db_path


def test_overlapping_request_from_other_process_gets_409(shared_db):
    api, db_mod, server_mod, endpoint, db_path = shared_db
    # 放慢翻页：第一笔补采会在持锁状态下停留数秒，给重叠请求留出窗口
    server_mod.set_page_delay(0.6)

    holder = _start_worker(
        "hold", db_path, endpoint, "2026-10-01", "2026-10-27"
    )
    try:
        _wait_event(holder, "LOCK_HELD")
        # 再给慢翻页一点时间，确保 holder 仍在采集窗口内
        time.sleep(0.3)

        requests_before = server_mod.get_request_count()
        runs_before = len(api.get("/api/v1/runs").json())

        # 第二个进程对同一个 SQLite 库发起重叠补采：立即 409
        rival = _start_worker(
            "harvest", db_path, endpoint, "2026-10-01", "2026-10-20"
        )
        payload, _ = _wait_event(rival, "CONFLICT")
        assert rival.wait(timeout=10) == 3
        assert "正在补采" in payload["message"]

        # 通过 HTTP（第二个“实例”的 Swagger）同样立即得到 409
        resp = api.post(
            "/api/v1/harvest",
            json={"from": "2026-10-01", "until": "2026-10-20"},
        )
        assert resp.status_code == 409
        assert "hint" in resp.json()["detail"]

        # 重叠请求没有请求 OAI-PMH 源，也没有新增运行记录
        assert server_mod.get_request_count() == requests_before
        assert len(api.get("/api/v1/runs").json()) == runs_before
    finally:
        holder.wait(timeout=30)
        if holder.poll() is None:
            holder.kill()

    # 持锁补采正常提交：27 条全部入库，截止日推进
    assert holder.returncode == 0
    assert db_mod.get_last_until(db_mod.get_connection(str(db_path))) == "2026-10-27"
    conn = db_mod.get_connection(str(db_path))
    try:
        assert conn.execute("SELECT COUNT(*) FROM bib_records").fetchone()[0] == 27
        # 只有持锁那一笔留下运行历史（重叠的两笔都没有）
        assert conn.execute(
            "SELECT COUNT(*) FROM harvest_runs"
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_reads_during_in_flight_harvest_see_only_committed_data(shared_db):
    api, db_mod, server_mod, endpoint, db_path = shared_db
    # 先提交一笔：10-01~10-10 共 10 条，截止日 2026-10-10
    first = api.post(
        "/api/v1/harvest",
        json={"from": "2026-10-01", "until": "2026-10-10"},
    )
    assert first.status_code == 200

    server_mod.set_page_delay(0.7)
    holder = _start_worker(
        "hold", db_path, endpoint, "2026-10-10", "2026-10-27"
    )
    try:
        _wait_event(holder, "LOCK_HELD")
        time.sleep(0.3)

        # 采集未提交：目录与进度仍可查询，且只反映已提交数据
        catalog = api.get("/api/v1/catalog?status=all").json()
        assert len(catalog) == 10
        assert all(r["identifier"] <= "oai:sample.lib:rec-010" for r in catalog)
        assert api.get("/api/v1/progress").json()["last_until"] == "2026-10-10"
        # 未提交的 running 行不可见
        assert len(api.get("/api/v1/runs").json()) == 1
    finally:
        holder.wait(timeout=30)
        if holder.poll() is None:
            holder.kill()

    # 提交后新数据与新截止日才可见
    assert holder.returncode == 0
    assert len(api.get("/api/v1/catalog?status=all").json()) == 27
    assert api.get("/api/v1/progress").json()["last_until"] == "2026-10-27"
    assert len(api.get("/api/v1/runs").json()) == 2


def test_crashed_process_releases_lock_and_resume_needs_no_manual_unlock(shared_db):
    api, db_mod, server_mod, endpoint, db_path = shared_db
    # 先成功采集到 2026-10-10，作为“最后成功的截止日”
    ok = api.post(
        "/api/v1/harvest",
        json={"from": "2026-10-01", "until": "2026-10-10"},
    )
    assert ok.status_code == 200

    # 另一个进程取得锁后异常死亡（模拟 kill -9，未提交也未释放锁）
    crashed = _start_worker(
        "crash", db_path, endpoint, "2026-10-10", "2026-10-27"
    )
    _wait_event(crashed, "LOCK_HELD")
    assert crashed.wait(timeout=10) == 42

    # 无需任何人工解锁：新进程立刻就能取得锁并从最后成功截止日续采
    server_mod.set_page_delay(0)
    resumed = _start_worker(
        "harvest", db_path, endpoint, "2026-10-27"
    )
    try:
        payload, _ = _wait_event(resumed, "SUMMARY")
        assert resumed.wait(timeout=20) == 0
    finally:
        if resumed.poll() is None:
            resumed.kill()

    # 起始日自动取上次成功截止日（含当天）
    assert payload["from_date"] == "2026-10-10"
    assert payload["until_date"] == "2026-10-27"
    assert payload["fetched"] == 18  # 10-10..10-27 共 18 条
    assert api.get("/api/v1/progress").json()["last_until"] == "2026-10-27"
    assert len(api.get("/api/v1/catalog?status=all").json()) == 27

    # 崩溃进程未提交的 running 行已随事务回滚消失，只留两次成功记录
    runs = api.get("/api/v1/runs").json()
    assert [r["status"] for r in runs] == ["success", "success"]


def test_in_process_overlap_returns_409_without_oai_request(shared_db):
    """同进程内并发也由同一把 SQLite 锁拦截（threading 锁已移除）。"""
    api, db_mod, server_mod, endpoint, db_path = shared_db
    server_mod.set_page_delay(0.6)

    errors: list[Exception] = []

    def _hold():
        try:
            api.post(
                "/api/v1/harvest",
                json={"from": "2026-10-01", "until": "2026-10-27"},
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    holder_thread = threading.Thread(target=_hold)
    holder_thread.start()
    time.sleep(0.4)  # 等待持锁并进入慢翻页

    requests_before = server_mod.get_request_count()
    resp = api.post(
        "/api/v1/harvest",
        json={"from": "2026-10-01", "until": "2026-10-20"},
    )
    assert resp.status_code == 409
    assert server_mod.get_request_count() == requests_before

    holder_thread.join(timeout=30)
    assert not errors
    runs = api.get("/api/v1/runs").json()
    assert len(runs) == 1 and runs[0]["status"] == "success"
