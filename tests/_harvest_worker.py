"""跨进程测试用的采集工作进程（由 pytest 以 subprocess 方式启动）。

用法：
  python -I tests/_harvest_worker.py hold <db> <oai_endpoint> [from until]
      执行一次真实补采（测试端会把样例源翻页延迟调大，使其长时间持锁），
      取到锁后打印 LOCK_HELD 便于测试同步。

  python -I tests/_harvest_worker.py harvest <db> <oai_endpoint> [from until]
      普通补采；拿不到锁时以退出码 3 结束并打印 CONFLICT。

环境变量 APP_DB_PATH 也可指定库路径；这里显式用参数传入。
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "sample-oai"))


def _emit(payload: dict) -> None:
    print("WORKER " + json.dumps(payload, ensure_ascii=False), flush=True)


def main() -> int:
    mode = sys.argv[1]
    db_path = sys.argv[2]
    endpoint = sys.argv[3]
    date_args = sys.argv[4:]
    if len(date_args) == 2:
        from_date, until_date = date_args
    elif len(date_args) == 1:
        # 只传截止日：from 留空，验证从 last_until 自动续采
        from_date, until_date = None, date_args[0]
    else:
        from_date, until_date = None, "2026-10-27"

    os.environ["APP_DB_PATH"] = db_path

    from app import db, harvester
    from app.oai_client import OAIClient

    db.init_db(db_path)

    if mode == "crash":
        # 取锁后立即“异常死亡”：不 commit/rollback，不释放锁，
        # 模拟采集进程在翻页途中被 kill -9。
        lock_conn = db.acquire_harvest_lock(f"crash-worker:{os.getpid()}")
        _emit({"event": "LOCK_HELD", "pid": os.getpid()})
        lock_conn.execute(
            "INSERT INTO harvest_runs (started_at, from_date, until_date, status) "
            "VALUES (?, ?, ?, 'running')",
            (db.utc_now_iso(), from_date or "", until_date),
        )
        os._exit(42)

    if mode == "hold":
        # 让测试端在锁被取得的瞬间收到信号（慢翻页由测试端配置）
        original_acquire = db.acquire_harvest_lock

        def _acquire_with_signal(holder, db_path=None):
            conn = original_acquire(holder, db_path)
            _emit({"event": "LOCK_HELD", "pid": os.getpid()})
            return conn

        db.acquire_harvest_lock = _acquire_with_signal

    try:
        with OAIClient(endpoint) as client:
            summary = harvester.harvest(
                client, until_date=until_date, from_date=from_date
            )
    except harvester.HarvestConflictError as exc:
        _emit({"event": "CONFLICT", "message": str(exc)})
        return 3
    except Exception as exc:  # noqa: BLE001 - 工作进程需要把任何失败透给测试
        _emit({"event": "ERROR", "message": str(exc), "type": type(exc).__name__})
        return 1

    _emit({"event": "SUMMARY", **summary.__dict__})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
