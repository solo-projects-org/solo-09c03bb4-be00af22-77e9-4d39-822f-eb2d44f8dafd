"""采集编排：保证“翻页失败不让半批记录入库”，且全库同一时刻只允许一笔补采。

流程：
1. 首次采集由调用方提供起始日；之后起始日取 harvest_progress.last_until
   （上次成功采集的截止日，含当天），实现无缝续采。
2. 采集开始即取得 SQLite 库级写锁（BEGIN IMMEDIATE），跨进程互斥：
   重叠请求立刻得到 409，不请求 OAI-PMH 源、也不新增运行记录。
3. ListRecords 按日期拉取 oai_dc，续页仅用 resumptionToken。
4. resumptionToken 失效（badResumptionToken）时丢弃已取页，按原区间整体重抓一次。
5. 所有页取完后，在锁事务内 upsert/撤下书目并推进截止日；
   无匹配记录同样推进截止日。事务失败则整批回滚，进度不变。
6. 进程异常退出时 SQLite 自动回滚未提交事务并释放锁，重启后可从
   最后成功的截止日续采，无需人工解锁。
"""
from __future__ import annotations

import json
import logging
import os
import socket
from dataclasses import dataclass

from . import db
from .oai_client import OAIClient, OAIError, OAIRecord

logger = logging.getLogger("harvester")


class HarvestConflictError(RuntimeError):
    pass


@dataclass
class HarvestSummary:
    from_date: str
    until_date: str
    pages: int
    fetched: int
    inserted: int
    updated: int
    withdrawn: int
    retries: int
    run_id: int
    status: str = "success"
    note: str | None = None


def _fetch_full_window(client: OAIClient, from_date: str | None,
                       until_date: str) -> tuple[list[OAIRecord], int, int]:
    """完整拉取一个日期区间。

    返回 (全部记录, 页数, 因 badResumptionToken 触发的重抓次数)。
    令牌失效时丢弃已取页，按原区间重新开始，只重抓一次。
    """
    retries = 0

    def _attempt() -> tuple[list[OAIRecord], int]:
        nonlocal retries
        collected: list[OAIRecord] = []
        pages = 0
        page = client.list_records_first(
            from_date=from_date, until_date=until_date
        )
        pages += 1
        collected.extend(page.records)
        token = page.resumption_token

        while token:
            try:
                page = client.list_records_resume(token)
            except OAIError as exc:
                if exc.code != "badResumptionToken":
                    raise
                # 令牌失效：本批已取页全部作废，按原区间重抓一次
                retries += 1
                logger.warning(
                    "resumptionToken 失效，丢弃已取的 %d 页，按区间 %s~%s 重抓",
                    pages, from_date, until_date,
                )
                return _retry_once()
            pages += 1
            collected.extend(page.records)
            token = page.resumption_token
        return collected, pages

    def _retry_once() -> tuple[list[OAIRecord], int]:
        collected: list[OAIRecord] = []
        pages = 0
        page = client.list_records_first(
            from_date=from_date, until_date=until_date
        )
        pages += 1
        collected.extend(page.records)
        token = page.resumption_token
        while token:
            # 重抓期间再次失效属于不可恢复错误，直接失败（不循环重试）
            page = client.list_records_resume(token)
            pages += 1
            collected.extend(page.records)
            token = page.resumption_token
        return collected, pages

    records, pages = _attempt()
    return records, pages, retries


def _holder_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def harvest(client: OAIClient, *, until_date: str,
            from_date: str | None) -> HarvestSummary:
    """执行一次采集。from_date 为 None 时从上次成功截止日续采。

    先取跨进程采集锁：拿不到锁立即抛 HarvestConflictError（409），
    此时尚未读取截止日、未请求 OAI 源、未写入任何运行记录。
    """
    try:
        lock_conn = db.acquire_harvest_lock(_holder_name())
    except db.HarvestLockBusy as exc:
        raise HarvestConflictError(str(exc)) from exc
    try:
        return _harvest_locked(lock_conn, client, until_date=until_date,
                               from_date=from_date)
    finally:
        # 正常路径已在内部 commit；异常路径确保 rollback 后再释放锁
        db.release_harvest_lock(lock_conn)


def _harvest_locked(conn, client: OAIClient, *, until_date: str,
                    from_date: str | None) -> HarvestSummary:
    """持有库级写锁执行整笔补采：全部变更在同一个事务内提交或回滚。"""
    run_id: int | None = None
    effective_from: str | None = None
    try:
        # 起始日：显式传入优先，否则取上次成功采集的截止日（含当天）。
        # 该读取发生在锁事务内，保证与后续提交之间不会插入另一笔补采。
        effective_from = from_date or db.get_last_until(conn)
        # 运行记录先留在锁事务里、不提交：重叠请求看不到它，失败回滚也一并撤销
        run_id = db.start_run(conn, effective_from or "", until_date)

        try:
            records, pages, retries = _fetch_full_window(
                client, effective_from, until_date
            )
        except OAIError as exc:
            if exc.code == "noRecordsMatch":
                # 无匹配：不写书目，但仍推进截止日
                return _commit_empty_window(
                    conn, run_id, effective_from, until_date
                )
            raise

        # 整批落库：全部页已取完才写库，与截止日推进、运行历史在同一事务提交
        inserted = updated = withdrawn = 0
        for record in records:
            if record.deleted:
                # 遇删除标记就撤下；库中不存在则落 withdrawn 墓碑
                if db.withdraw_bib_record(
                    conn, identifier=record.identifier,
                    datestamp=record.datestamp,
                ):
                    withdrawn += 1
                else:
                    db.insert_withdrawn_tombstone(
                        conn, identifier=record.identifier,
                        datestamp=record.datestamp,
                    )
                    withdrawn += 1
                continue
            outcome = db.upsert_bib_record(
                conn,
                identifier=record.identifier,
                datestamp=record.datestamp,
                fields=_first_values(record.dc),
                raw_dc=json.dumps(record.dc, ensure_ascii=False),
            )
            if outcome == "inserted":
                inserted += 1
            else:
                updated += 1

        db.advance_progress(conn, until_date)
        db.finish_run(
            conn, run_id, status="success", pages=pages,
            fetched=len(records), inserted=inserted, updated=updated,
            withdrawn=withdrawn, retries=retries, error=None,
        )
        conn.commit()

        logger.info(
            "采集成功 区间=%s~%s 页=%d 取=%d 增=%d 改=%d 撤=%d 重抓=%d",
            effective_from, until_date, pages, len(records),
            inserted, updated, withdrawn, retries,
        )
        return HarvestSummary(
            from_date=effective_from or "",
            until_date=until_date,
            pages=pages,
            fetched=len(records),
            inserted=inserted,
            updated=updated,
            withdrawn=withdrawn,
            retries=retries,
            run_id=run_id,
        )
    except Exception as exc:
        # 失败：锁事务整体回滚，书目与截止日恢复到采集前，running 行也撤销
        conn.rollback()
        logger.exception("采集失败，已回滚本批全部变更：%s", exc)
        # 锁已随回滚释放；另起事务补记 failed 历史，不影响“半批不入库”
        try:
            fail_conn = db.get_connection()
            try:
                db.insert_failed_run(
                    fail_conn,
                    from_date=effective_from or "",
                    until_date=until_date,
                    error=str(exc),
                )
                fail_conn.commit()
            finally:
                fail_conn.close()
        except Exception:  # pragma: no cover - 历史记录失败不掩盖主错误
            logger.exception("写入失败运行历史时出错")
        raise


def _commit_empty_window(conn, run_id: int, from_date: str | None,
                         until_date: str) -> HarvestSummary:
    db.advance_progress(conn, until_date)
    db.finish_run(
        conn, run_id, status="success", pages=0, fetched=0,
        inserted=0, updated=0, withdrawn=0, retries=0, error=None,
    )
    conn.commit()
    logger.info("区间 %s~%s 无匹配记录，截止日仍推进至 %s",
                from_date, until_date, until_date)
    return HarvestSummary(
        from_date=from_date or "",
        until_date=until_date,
        pages=0,
        fetched=0,
        inserted=0,
        updated=0,
        withdrawn=0,
        retries=0,
        run_id=run_id,
        note="区间内无匹配记录，截止日已推进",
    )


def _first_values(dc: dict) -> dict:
    """Dublin Core 多值字段取首值落列，完整多值保留在 raw_dc。"""
    return {key: (values[0] if values else None) for key, values in dc.items()}
