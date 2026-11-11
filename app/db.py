"""SQLite 持久化：书目记录、采集进度、采集运行历史与跨进程采集锁。

所有写操作在单连接单事务内完成，保证“整批采完才提交”。

跨进程互斥：补采启动时在专用连接上执行 BEGIN IMMEDIATE 取得库级写锁，
从读取续采截止日到整批提交/回滚期间，同一个 SQLite 库只允许一笔补采。
进程异常退出时 SQLite 自动回滚未提交事务并释放锁，无需人工解锁。
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone

DB_PATH = os.environ.get("APP_DB_PATH", "/data/harvester.db")

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS bib_records (
    identifier      TEXT PRIMARY KEY,            -- OAI 记录唯一标识
    datestamp       TEXT NOT NULL,               -- OAI header datestamp
    status          TEXT NOT NULL,               -- active / withdrawn
    title           TEXT,
    creator         TEXT,
    publisher       TEXT,
    subject         TEXT,
    description     TEXT,
    date            TEXT,
    type            TEXT,
    format          TEXT,
    identifier_dc   TEXT,
    source          TEXT,
    language        TEXT,
    relation        TEXT,
    coverage        TEXT,
    rights          TEXT,
    raw_dc          TEXT NOT NULL,               -- 原始 Dublin Core JSON
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS harvest_progress (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    last_until      TEXT NOT NULL,               -- 上次成功采集的截止日（含）
    last_harvest_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS harvest_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    from_date       TEXT NOT NULL,
    until_date      TEXT NOT NULL,
    status          TEXT NOT NULL,               -- success / failed / running
    pages           INTEGER NOT NULL DEFAULT 0,
    fetched         INTEGER NOT NULL DEFAULT 0,
    inserted        INTEGER NOT NULL DEFAULT 0,
    updated         INTEGER NOT NULL DEFAULT 0,
    withdrawn       INTEGER NOT NULL DEFAULT 0,
    retries         INTEGER NOT NULL DEFAULT 0,
    error           TEXT
);

-- 采集锁持有者信息随锁事务一起写入：未提交时其他连接不可见，
-- 提交/回滚后分别留下最近一次成功采集者或自动消失。
CREATE TABLE IF NOT EXISTS harvest_lock (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    holder          TEXT NOT NULL,               -- 主机:进程号
    acquired_at     TEXT NOT NULL
);
"""


class HarvestLockBusy(RuntimeError):
    """已有补采持有库级写锁，重叠请求应立即返回 409。"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_connection(db_path: str | None = None,
                   busy_timeout: int = 30000) -> sqlite3.Connection:
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, timeout=max(busy_timeout, 0) / 1000.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(f"PRAGMA busy_timeout={int(busy_timeout)}")
    return conn


def init_db(db_path: str | None = None) -> None:
    conn = get_connection(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()


def acquire_harvest_lock(holder: str,
                         db_path: str | None = None) -> sqlite3.Connection:
    """取得跨进程采集锁，返回由调用方持有的锁连接。

    以 busy_timeout=0 立即尝试 BEGIN IMMEDIATE：库中已有补采在执行时
    立刻抛 HarvestLockBusy，不等待、不重试，便于上层直接返回 409。
    锁连接的事务直到整批 commit/rollback（或进程退出由 SQLite 自动回滚）
    才结束，因此覆盖“读取截止日 -> 翻页 -> 提交/回滚”的整个窗口。
    """
    conn = get_connection(db_path, busy_timeout=0)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO harvest_lock (id, holder, acquired_at)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                holder = excluded.holder,
                acquired_at = excluded.acquired_at
            """,
            (holder, utc_now_iso()),
        )
    except sqlite3.OperationalError as exc:
        conn.close()
        if "locked" in str(exc).lower():
            raise HarvestLockBusy(
                "另一个采集实例正在补采同一目录，请稍后再试"
            ) from exc
        raise
    return conn


def release_harvest_lock(conn: sqlite3.Connection) -> None:
    """释放采集锁：正常提交或异常回滚后关闭连接。"""
    try:
        if conn.in_transaction:
            conn.rollback()
    finally:
        conn.close()


def get_last_until(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT last_until FROM harvest_progress WHERE id = 1"
    ).fetchone()
    return row["last_until"] if row else None


def advance_progress(conn: sqlite3.Connection, until_date: str) -> None:
    """整批成功后推进截止日（单行 upsert）。"""
    conn.execute(
        """
        INSERT INTO harvest_progress (id, last_until, last_harvest_at)
        VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            last_until = excluded.last_until,
            last_harvest_at = excluded.last_harvest_at
        """,
        (until_date, utc_now_iso()),
    )


def start_run(conn: sqlite3.Connection, from_date: str, until_date: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO harvest_runs (started_at, from_date, until_date, status)
        VALUES (?, ?, ?, 'running')
        """,
        (utc_now_iso(), from_date, until_date),
    )
    return int(cur.lastrowid)


def finish_run(conn: sqlite3.Connection, run_id: int, *, status: str,
               pages: int, fetched: int, inserted: int, updated: int,
               withdrawn: int, retries: int, error: str | None) -> None:
    conn.execute(
        """
        UPDATE harvest_runs SET
            finished_at = ?, status = ?, pages = ?, fetched = ?,
            inserted = ?, updated = ?, withdrawn = ?, retries = ?, error = ?
        WHERE id = ?
        """,
        (utc_now_iso(), status, pages, fetched, inserted, updated,
         withdrawn, retries, error, run_id),
    )


def insert_failed_run(conn: sqlite3.Connection, *, from_date: str,
                      until_date: str, error: str) -> int:
    """锁事务回滚导致 running 行消失后，在独立事务补记一条 failed 历史。"""
    cur = conn.execute(
        """
        INSERT INTO harvest_runs (
            started_at, finished_at, from_date, until_date,
            status, error
        ) VALUES (?, ?, ?, ?, 'failed', ?)
        """,
        (utc_now_iso(), utc_now_iso(), from_date, until_date, error),
    )
    return int(cur.lastrowid)


def upsert_bib_record(conn: sqlite3.Connection, *, identifier: str,
                      datestamp: str, fields: dict, raw_dc: str) -> str:
    """按标识 upsert 书目；返回 'inserted' 或 'updated'。"""
    existing = conn.execute(
        "SELECT identifier FROM bib_records WHERE identifier = ?",
        (identifier,),
    ).fetchone()
    now = utc_now_iso()
    params = (
        identifier, datestamp, "active",
        fields.get("title"), fields.get("creator"), fields.get("publisher"),
        fields.get("subject"), fields.get("description"), fields.get("date"),
        fields.get("type"), fields.get("format"), fields.get("identifier"),
        fields.get("source"), fields.get("language"), fields.get("relation"),
        fields.get("coverage"), fields.get("rights"), raw_dc, now,
    )
    if existing is None:
        conn.execute(
            """
            INSERT INTO bib_records (
                identifier, datestamp, status, title, creator, publisher,
                subject, description, date, type, format, identifier_dc,
                source, language, relation, coverage, rights, raw_dc, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        return "inserted"
    conn.execute(
        """
        UPDATE bib_records SET
            datestamp = ?, status = ?, title = ?, creator = ?, publisher = ?,
            subject = ?, description = ?, date = ?, type = ?, format = ?,
            identifier_dc = ?, source = ?, language = ?, relation = ?,
            coverage = ?, rights = ?, raw_dc = ?, updated_at = ?
        WHERE identifier = ?
        """,
        params[1:] + (identifier,),
    )
    return "updated"


def withdraw_bib_record(conn: sqlite3.Connection, *, identifier: str,
                        datestamp: str) -> bool:
    """遇删除标记则撤下（标记 withdrawn）。不存在返回 False。"""
    cur = conn.execute(
        """
        UPDATE bib_records
        SET status = 'withdrawn', datestamp = ?, updated_at = ?
        WHERE identifier = ?
        """,
        (datestamp, utc_now_iso(), identifier),
    )
    return cur.rowcount > 0


def insert_withdrawn_tombstone(conn: sqlite3.Connection, *, identifier: str,
                               datestamp: str) -> None:
    """删除标记记录此前不在库中时，写入 withdrawn 墓碑，保留撤下痕迹。"""
    conn.execute(
        """
        INSERT INTO bib_records (
            identifier, datestamp, status, raw_dc, updated_at
        ) VALUES (?, ?, 'withdrawn', ?, ?)
        ON CONFLICT(identifier) DO NOTHING
        """,
        (identifier, datestamp, "{}", utc_now_iso()),
    )
