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
 wavelength_nm REAL NOT NULL, response REAL NOT NULL, noise REAL NOT NULL,
 instrument TEXT NOT NULL, operator TEXT NOT NULL, measured_at TEXT NOT NULL,
 UNIQUE(lot_id,measurement_id));
CREATE TABLE IF NOT EXISTS measurement_requests(
 scope TEXT NOT NULL, client_key TEXT NOT NULL,
 instrument TEXT NOT NULL, wavelength_nm REAL NOT NULL,
 request_sha256 TEXT NOT NULL, measurement_id TEXT NOT NULL,
 created_at TEXT NOT NULL, PRIMARY KEY(scope,client_key));
CREATE TABLE IF NOT EXISTS lot_events(
 event_id INTEGER PRIMARY KEY AUTOINCREMENT, lot_id TEXT NOT NULL,
 event_type TEXT NOT NULL, actor TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS approvals(
 lot_id TEXT NOT NULL, reviewer TEXT NOT NULL, decision TEXT NOT NULL,
 reason TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(lot_id,reviewer));
"""

# 业务测量身份：同一批次 + 同一仪器 + 同一波长只允许一条测量。
BUSINESS_KEY_INDEX = "measurement_business_key"


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(path: str = ":memory:") -> sqlite3.Connection:
    # HTTP 服务多线程共享一个连接；isolation_level=None 让事务完全由
    # transaction() 中的 BEGIN IMMEDIATE 显式控制，避免隐式提交串扰。
    db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA busy_timeout=5000")
    db.executescript(SCHEMA)
    _upgrade_measurements(db)
    db.commit()
    return db


def _upgrade_measurements(db: sqlite3.Connection) -> None:
    """补齐测量幂等索引和请求台账，并清理修复前产生的重复行。

    每个 (lot_id,instrument,wavelength_nm) 保留最早写入的一条；老数据按
    measurement_id 回填台账，使进程重启后的重放仍能识别原始记录。
    """
    has_index = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?", (BUSINESS_KEY_INDEX,)
    ).fetchone()
    if not has_index:
        duplicate_ids = [
            row[0]
            for row in db.execute(
                "SELECT measurement_id FROM measurements WHERE rowid NOT IN "
                "(SELECT min(rowid) FROM measurements GROUP BY lot_id,instrument,wavelength_nm)"
            ).fetchall()
        ]
        db.execute(
            "DELETE FROM measurements WHERE rowid NOT IN "
            "(SELECT min(rowid) FROM measurements GROUP BY lot_id,instrument,wavelength_nm)"
        )
        if duplicate_ids:
            # 同步清理被去重记录的测量审计事件，使审计链与实际记录一致。
            placeholders = ",".join("?" for _ in duplicate_ids)
            db.execute(
                f"DELETE FROM lot_events WHERE event_type='measurement' "
                f"AND json_extract(payload,'$.measurement_id') IN ({placeholders})",
                duplicate_ids,
            )
        db.execute(
            f"CREATE UNIQUE INDEX {BUSINESS_KEY_INDEX} "
            "ON measurements(lot_id,instrument,wavelength_nm)"
        )
    db.execute(
        "INSERT INTO measurement_requests"
        "(scope,client_key,instrument,wavelength_nm,request_sha256,measurement_id,created_at) "
        "SELECT m.lot_id, 'legacy:'||m.measurement_id, m.instrument, m.wavelength_nm, '', "
        "m.measurement_id, m.measured_at FROM measurements m WHERE NOT EXISTS "
        "(SELECT 1 FROM measurement_requests r WHERE r.scope=m.lot_id AND r.measurement_id=m.measurement_id)"
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
