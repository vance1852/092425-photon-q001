"""芯片批次和测量记录的 SQLite 结构及事务辅助函数。"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS chip_lots(
 lot_id TEXT PRIMARY KEY, product TEXT NOT NULL, process_rev TEXT NOT NULL,
 wafer_count INTEGER NOT NULL, status TEXT NOT NULL, owner TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS measurements(
 measurement_id TEXT PRIMARY KEY, lot_id TEXT NOT NULL REFERENCES chip_lots(lot_id),
 measurement_no TEXT NOT NULL,
 wavelength_nm REAL NOT NULL, response REAL NOT NULL, noise REAL NOT NULL,
 instrument TEXT NOT NULL, operator TEXT NOT NULL, measured_at TEXT NOT NULL,
 UNIQUE(lot_id,measurement_no));
CREATE TABLE IF NOT EXISTS measurement_requests(
 lot_id TEXT NOT NULL, measurement_no TEXT NOT NULL,
 request_sha256 TEXT NOT NULL, response_json TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(lot_id,measurement_no));
CREATE TABLE IF NOT EXISTS lot_events(
 event_id INTEGER PRIMARY KEY AUTOINCREMENT, lot_id TEXT NOT NULL,
 event_type TEXT NOT NULL, actor TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS approvals(
 lot_id TEXT NOT NULL, reviewer TEXT NOT NULL, decision TEXT NOT NULL,
 reason TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(lot_id,reviewer));
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: object) -> str:
    """生成稳定紧凑的 JSON 文本，用于请求内容的幂等比对。"""

    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def connect(path: str = ":memory:") -> sqlite3.Connection:
    # ThreadingHTTPServer 会在多个工作线程间共享同一个连接；写入由服务层
    # RLock 串行化，这里允许跨线程使用并开启忙等待以支持多进程并发提交。
    db = sqlite3.connect(path, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA busy_timeout=5000")
    if path != ":memory:":
        db.execute("PRAGMA journal_mode=WAL")
    db.executescript(SCHEMA)
    _migrate(db)
    db.commit()
    return db


def _migrate(db: sqlite3.Connection) -> None:
    """把早期数据库补齐到当前模式；新库不受影响。"""

    columns = {row[1] for row in db.execute("PRAGMA table_info(measurements)")}
    if "measurement_no" in columns:
        return
    db.execute("ALTER TABLE measurements ADD COLUMN measurement_no TEXT")
    # 旧记录没有业务编号，沿用其 measurement_id 作为回填编号。
    db.execute("UPDATE measurements SET measurement_no=measurement_id")
    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_measurements_lot_no "
        "ON measurements(lot_id,measurement_no)"
    )


@contextmanager
def transaction(db: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        db.execute("BEGIN IMMEDIATE")
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise


def event(db: sqlite3.Connection, lot_id: str, event_type: str, actor: str, payload: dict) -> None:
    db.execute("INSERT INTO lot_events(lot_id,event_type,actor,payload,created_at) VALUES(?,?,?,?,?)", (lot_id, event_type, actor, json.dumps(payload, sort_keys=True), utcnow()))
