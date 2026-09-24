"""协调认证、批次、测试和放行门禁的应用服务。"""

from __future__ import annotations

import hashlib
import json
import threading
import uuid
from typing import Sequence

from .analytics import confidence_interval, summarize_spectrum, yield_rate
from .auth import Auth
from .errors import Conflict
from .storage import canonical_json, connect, event, transaction, utcnow


class PhotonService:
    def __init__(self, database: str = ":memory:"):
        # ThreadingHTTPServer 在线程间共享这一个连接；用锁把写入串行化，
        # 配合 BEGIN IMMEDIATE 与 UNIQUE 约束，保证并发提交与进程内重试幂等。
        self._lock = threading.RLock()
        self.db = connect(database)
        self.auth = Auth(self.db, self._lock)

    def bootstrap_admin(self, user_id: str = "admin", password: str = "photon-admin") -> None:
        try:
            self.auth.create_user(user_id, password, "admin")
        except Exception:
            pass

    def create_lot(self, token: str, lot_id: str, product: str, process_rev: str, wafer_count: int) -> dict:
        actor = self.auth.require(token, "submit")
        if wafer_count <= 0 or not lot_id.strip() or not process_rev.strip():
            raise ValueError("lot fields are invalid")
        now = utcnow()
        with self._lock, transaction(self.db):
            self.db.execute("INSERT INTO chip_lots VALUES(?,?,?,?,?,?,?,?)", (lot_id, product, process_rev, wafer_count, "engineering", actor.user_id, now, now))
            event(self.db, lot_id, "created", actor.user_id, {"product": product, "process_rev": process_rev})
        return self.get_lot(token, lot_id)

    def get_lot(self, token: str, lot_id: str) -> dict:
        self.auth.require(token, "read")
        with self._lock:
            row = self.db.execute("SELECT * FROM chip_lots WHERE lot_id=?", (lot_id,)).fetchone()
        if not row:
            raise KeyError(lot_id)
        return dict(row)

    @staticmethod
    def _request_digest(lot_id: str, measurement_no: str, wavelength_nm: float,
                        response: float, noise: float, instrument: str) -> str:
        body = canonical_json({
            "lot_id": lot_id,
            "measurement_no": measurement_no,
            "wavelength_nm": wavelength_nm,
            "response": response,
            "noise": noise,
            "instrument": instrument,
        })
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    def _stored_request(self, lot_id: str, measurement_no: str):
        return self.db.execute(
            "SELECT request_sha256,response_json FROM measurement_requests "
            "WHERE lot_id=? AND measurement_no=?",
            (lot_id, measurement_no),
        ).fetchone()

    def add_measurement(self, token: str, lot_id: str, wavelength_nm: float, response: float,
                        noise: float, instrument: str, measurement_no: str | None = None) -> dict:
        """登记一条测量。

        measurement_no 是测试工程师侧的业务测量编号（同批次+同仪器+同编号
        唯一标识一次测量）。相同编号重放：内容一致则返回原记录且不新增审计
        事件；波长、响应、噪声或仪器不同则明确报冲突。
        """

        actor = self.auth.require(token, "measure")
        if measurement_no is None or not str(measurement_no).strip():
            raise ValueError("measurement_no is required")
        measurement_no = str(measurement_no).strip()
        if not str(instrument or "").strip():
            raise ValueError("instrument is required")
        wavelength_nm, response, noise = float(wavelength_nm), float(response), float(noise)
        request_digest = self._request_digest(
            lot_id, measurement_no, wavelength_nm, response, noise, instrument
        )
        with self._lock:
            # 快速重放路径：服务进程未重启时，绝大多数网络重试在这里返回，
            # 不开启写事务、不产生审计事件。
            stored = self._stored_request(lot_id, measurement_no)
            if stored is not None:
                if stored["request_sha256"] != request_digest:
                    raise Conflict(
                        f"measurement {measurement_no} already recorded for lot {lot_id} "
                        "with different wavelength or instrument"
                    )
                return json.loads(stored["response_json"])
            if not self.db.execute("SELECT 1 FROM chip_lots WHERE lot_id=?", (lot_id,)).fetchone():
                raise KeyError(lot_id)
            measurement_id = uuid.uuid4().hex
            result = {"measurement_id": measurement_id, "lot_id": lot_id,
                      "measurement_no": measurement_no}
            try:
                with transaction(self.db):
                    self.db.execute(
                        "INSERT INTO measurements(measurement_id,lot_id,measurement_no,"
                        "wavelength_nm,response,noise,instrument,operator,measured_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?)",
                        (measurement_id, lot_id, measurement_no, wavelength_nm, response,
                         noise, instrument, actor.user_id, utcnow()),
                    )
                    self.db.execute(
                        "INSERT INTO measurement_requests(lot_id,measurement_no,"
                        "request_sha256,response_json,created_at) VALUES(?,?,?,?,?)",
                        (lot_id, measurement_no, request_digest,
                         canonical_json(result), utcnow()),
                    )
                    event(self.db, lot_id, "measurement", actor.user_id,
                          {"measurement_id": measurement_id, "measurement_no": measurement_no,
                           "wavelength_nm": wavelength_nm, "instrument": instrument})
            except Exception as exc:
                # 并发提交或进程重启后首次重放：另一个事务可能已经落库。
                stored = self._stored_request(lot_id, measurement_no)
                if stored is not None:
                    if stored["request_sha256"] != request_digest:
                        raise Conflict(
                            f"measurement {measurement_no} already recorded for lot {lot_id} "
                            "with different wavelength or instrument"
                        ) from exc
                    return json.loads(stored["response_json"])
                raise
        return result

    def analyze(self, token: str, lot_id: str) -> dict:
        self.auth.require(token, "analyze")
        with self._lock:
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
        with self._lock, transaction(self.db):
            self.db.execute("INSERT OR REPLACE INTO approvals VALUES(?,?,?,?,?)", (lot_id, actor.user_id, decision, reason, utcnow()))
            status = {"release": "released", "hold": "hold", "reject": "rejected"}[decision]
            self.db.execute("UPDATE chip_lots SET status=?,updated_at=? WHERE lot_id=?", (status, utcnow(), lot_id))
            event(self.db, lot_id, "approval", actor.user_id, {"decision": decision, "reason": reason})
        return self.get_lot(token, lot_id)

    def audit(self, token: str, lot_id: str) -> list[dict]:
        self.auth.require(token, "read")
        with self._lock:
            return [dict(r) for r in self.db.execute("SELECT * FROM lot_events WHERE lot_id=? ORDER BY event_id", (lot_id,)).fetchall()]
