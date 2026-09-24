"""协调认证、批次、测试和放行门禁的应用服务。"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import threading
import uuid

from .analytics import confidence_interval, summarize_spectrum, yield_rate
from .auth import Auth
from .errors import Conflict
from .storage import connect, event, transaction, utcnow


class PhotonService:
    def __init__(self, database: str = ":memory:"):
        self.db = connect(database)
        # HTTP 服务以多线程共享同一个 SQLite 连接；写操作经此锁串行化，
        # 与 BEGIN IMMEDIATE 及业务键唯一索引共同保证并发提交一致性。
        self.write_lock = threading.RLock()
        self.auth = Auth(self.db, self.write_lock)

    def bootstrap_admin(self, user_id: str = "admin", password: str = "photon-admin") -> None:
        try:
            with self.write_lock:
                self.auth.create_user(user_id, password, "admin")
        except Exception:
            pass

    def create_lot(self, token: str, lot_id: str, product: str, process_rev: str, wafer_count: int) -> dict:
        actor = self.auth.require(token, "submit")
        if wafer_count <= 0 or not lot_id.strip() or not process_rev.strip():
            raise ValueError("lot fields are invalid")
        now = utcnow()
        with self.write_lock, transaction(self.db):
            self.db.execute("INSERT INTO chip_lots VALUES(?,?,?,?,?,?,?,?)", (lot_id, product, process_rev, wafer_count, "engineering", actor.user_id, now, now))
            event(self.db, lot_id, "created", actor.user_id, {"product": product, "process_rev": process_rev})
        return self.get_lot(token, lot_id)

    def get_lot(self, token: str, lot_id: str) -> dict:
        self.auth.require(token, "read")
        row = self.db.execute("SELECT * FROM chip_lots WHERE lot_id=?", (lot_id,)).fetchone()
        if not row:
            raise KeyError(lot_id)
        return dict(row)

    @staticmethod
    def _request_digest(measurement_no: str, wavelength_nm: float, response: float, noise: float, instrument: str) -> str:
        body = json.dumps(
            {
                "measurement_no": measurement_no,
                "wavelength_nm": wavelength_nm,
                "response": response,
                "noise": noise,
                "instrument": instrument,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(body.encode()).hexdigest()

    def _measurement_result(self, measurement_id: str, lot_id: str, replayed: bool) -> dict:
        return {"measurement_id": measurement_id, "lot_id": lot_id, "replayed": replayed}

    def add_measurement(
        self,
        token: str,
        lot_id: str,
        wavelength_nm: float,
        response: float,
        noise: float,
        instrument: str,
        measurement_no: str | None = None,
    ) -> dict:
        """写入一条测量。

        业务身份为 (批次, 仪器, 测量编号)，并有 (批次, 仪器, 波长) 唯一约束
        兜底。相同测量编号重放时返回原记录、不写审计；编号相同但波长或
        仪器（或其他载荷）不同时抛出 Conflict。
        """
        actor = self.auth.require(token, "measure")
        wavelength_nm, response, noise = float(wavelength_nm), float(response), float(noise)
        if not all(math.isfinite(v) for v in (wavelength_nm, response, noise)):
            raise ValueError("measurement values must be finite")
        if not instrument.strip():
            raise ValueError("instrument is required")
        # 未携带测量编号时，以业务身份本身作为幂等键，仍受唯一索引保护。
        client_key = measurement_no.strip() if measurement_no and measurement_no.strip() else f"wavelength:{wavelength_nm!r}"
        digest = self._request_digest(client_key, wavelength_nm, response, noise, instrument)
        with self.write_lock:
            try:
                with transaction(self.db):
                    outcome, measurement_id = self._insert_measurement(
                        lot_id, wavelength_nm, response, noise, instrument, client_key, digest, actor.user_id
                    )
            except sqlite3.IntegrityError as exc:
                # 跨进程并发时由唯一约束兜底：若对方写入的正是同一请求，
                # 按重放返回原记录，否则明确报冲突。
                replay_id = self._recover_replay(lot_id, client_key, instrument, wavelength_nm, digest)
                if replay_id is None:
                    raise Conflict("测量与既有记录冲突或并发提交冲突") from exc
                outcome, measurement_id = "replayed", replay_id
        return self._measurement_result(measurement_id, lot_id, replayed=outcome == "replayed")

    def _recover_replay(
        self, lot_id: str, client_key: str, instrument: str, wavelength_nm: float, digest: str
    ) -> str | None:
        with transaction(self.db):
            ledger = self.db.execute(
                "SELECT instrument,wavelength_nm,request_sha256,measurement_id "
                "FROM measurement_requests WHERE scope=? AND client_key=?",
                (lot_id, client_key),
            ).fetchone()
        if ledger is None:
            return None
        try:
            self._check_replay(ledger, client_key, instrument, wavelength_nm, digest)
        except Conflict:
            return None
        return ledger["measurement_id"]

    def _insert_measurement(
        self,
        lot_id: str,
        wavelength_nm: float,
        response: float,
        noise: float,
        instrument: str,
        client_key: str,
        digest: str,
        operator: str,
    ) -> tuple[str, str]:
        if not self.db.execute("SELECT 1 FROM chip_lots WHERE lot_id=?", (lot_id,)).fetchone():
            raise KeyError(lot_id)
        ledger = self.db.execute(
            "SELECT instrument,wavelength_nm,request_sha256,measurement_id "
            "FROM measurement_requests WHERE scope=? AND client_key=?",
            (lot_id, client_key),
        ).fetchone()
        if ledger is not None:
            self._check_replay(ledger, client_key, instrument, wavelength_nm, digest)
            return "replayed", ledger["measurement_id"]
        business = self.db.execute(
            "SELECT measurement_id,response,noise FROM measurements "
            "WHERE lot_id=? AND instrument=? AND wavelength_nm=?",
            (lot_id, instrument, wavelength_nm),
        ).fetchone()
        if business is not None:
            # 该业务身份已被另一条记录占用（历史数据或使用了不同测量编号）。
            # 测量编号是客户侧身份：编号不同就不能静默合并为同一条记录。
            raise Conflict(
                f"批次 {lot_id} 在仪器 {instrument}、波长 {wavelength_nm}nm 的测量已存在"
                f"（记录 {business['measurement_id']}），测量编号 {client_key!r} 与之冲突"
            )
        measurement_id = uuid.uuid4().hex
        self.db.execute(
            "INSERT INTO measurements VALUES(?,?,?,?,?,?,?,?)",
            (measurement_id, lot_id, wavelength_nm, response, noise, instrument, operator, utcnow()),
        )
        self._link_request(lot_id, client_key, instrument, wavelength_nm, digest, measurement_id)
        event(
            self.db,
            lot_id,
            "measurement",
            operator,
            {"measurement_id": measurement_id, "measurement_no": client_key, "wavelength_nm": wavelength_nm},
        )
        return "created", measurement_id

    def _link_request(
        self,
        lot_id: str,
        client_key: str,
        instrument: str,
        wavelength_nm: float,
        digest: str,
        measurement_id: str,
    ) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO measurement_requests"
            "(scope,client_key,instrument,wavelength_nm,request_sha256,measurement_id,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (lot_id, client_key, instrument, wavelength_nm, digest, measurement_id, utcnow()),
        )

    @staticmethod
    def _check_replay(ledger, client_key: str, instrument: str, wavelength_nm: float, digest: str) -> None:
        if ledger["instrument"] != instrument or ledger["wavelength_nm"] != wavelength_nm:
            raise Conflict(
                f"测量冲突：编号 {client_key!r} 已用于仪器 {ledger['instrument']}、"
                f"波长 {ledger['wavelength_nm']}nm，不能改记为仪器 {instrument}、波长 {wavelength_nm}nm"
            )
        if ledger["request_sha256"] and ledger["request_sha256"] != digest:
            raise Conflict(f"测量冲突：编号 {client_key!r} 的请求内容与首次提交不一致")

    def analyze(self, token: str, lot_id: str) -> dict:
        self.auth.require(token, "analyze")
        rows = self.db.execute("SELECT wavelength_nm,response FROM measurements WHERE lot_id=? ORDER BY wavelength_nm", (lot_id,)).fetchall()
        if len(rows) < 3:
            raise ValueError("three measurements are required")
        summary = summarize_spectrum([r[0] for r in rows], [r[1] for r in rows])
        rates = yield_rate(self.get_lot(token, lot_id)["wafer_count"], sum(1 for r in rows if r[1] >= 0.8), 0)
        ci = confidence_interval([r[1] for r in rows])
        return {"lot_id": lot_id, "spectrum": summary.__dict__, "yield": rates, "response_ci": ci}

    def approve(self, token: str, lot_id: str, decision: str, reason: str) -> dict:
        actor = self.auth.require(token, "approve")
        if decision not in {"release", "hold", "reject"} or not reason.strip():
            raise ValueError("decision and reason are required")
        with self.write_lock, transaction(self.db):
            self.db.execute("INSERT OR REPLACE INTO approvals VALUES(?,?,?,?,?)", (lot_id, actor.user_id, decision, reason, utcnow()))
            status = {"release": "released", "hold": "hold", "reject": "rejected"}[decision]
            self.db.execute("UPDATE chip_lots SET status=?,updated_at=? WHERE lot_id=?", (status, utcnow(), lot_id))
            event(self.db, lot_id, "approval", actor.user_id, {"decision": decision, "reason": reason})
        return self.get_lot(token, lot_id)

    def audit(self, token: str, lot_id: str) -> list[dict]:
        self.auth.require(token, "read")
        return [dict(r) for r in self.db.execute("SELECT * FROM lot_events WHERE lot_id=? ORDER BY event_id", (lot_id,)).fetchall()]
