"""测量写入幂等性、冲突检测、并发与重启一致性的测试。"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from photon_fab.api import Handler
from photon_fab.errors import Conflict
from photon_fab.service import PhotonService
from photon_fab.storage import connect


class MeasurementIdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = PhotonService()
        self.service.bootstrap_admin()
        self.token = self.service.auth.login("admin", "photon-admin")
        self.service.create_lot(self.token, "LOT-1", "PD-array", "R1", 20)

    def _count(self) -> int:
        return self.service.db.execute("SELECT count(*) FROM measurements WHERE lot_id='LOT-1'").fetchone()[0]

    def _add(self, measurement_no="M-1", **overrides):
        params = {"wavelength_nm": 450.0, "response": 0.71, "noise": 0.01, "instrument": "spec-1"}
        params.update(overrides)
        return self.service.add_measurement(self.token, "LOT-1", measurement_no=measurement_no, **params)

    def test_replay_returns_original_record_without_new_audit(self) -> None:
        first = self._add()
        replay = self._add()
        self.assertFalse(first["replayed"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["measurement_id"], replay["measurement_id"])
        self.assertEqual(self._count(), 1)
        events = [e["event_type"] for e in self.service.audit(self.token, "LOT-1")]
        self.assertEqual(events, ["created", "measurement"])

    def test_same_number_different_wavelength_conflicts(self) -> None:
        self._add(wavelength_nm=450.0)
        with self.assertRaises(Conflict):
            self._add(wavelength_nm=520.0)
        self.assertEqual(self._count(), 1)

    def test_same_number_different_instrument_conflicts(self) -> None:
        self._add(instrument="spec-1")
        with self.assertRaises(Conflict):
            self._add(instrument="spec-2")
        self.assertEqual(self._count(), 1)

    def test_same_number_different_payload_conflicts(self) -> None:
        self._add(response=0.71)
        with self.assertRaises(Conflict):
            self._add(response=0.99)
        self.assertEqual(self._count(), 1)

    def test_different_numbers_same_wavelength_conflict(self) -> None:
        self._add(measurement_no="M-1")
        with self.assertRaises(Conflict):
            self._add(measurement_no="M-2")
        self.assertEqual(self._count(), 1)

    def test_three_wavelengths_with_replays_keep_analysis_stable(self) -> None:
        for index, (w, r) in enumerate(((450, .71), (520, .93), (650, .84)), start=1):
            self._add(measurement_no=f"M-{index}", wavelength_nm=w, response=r)
        before = self.service.analyze(self.token, "LOT-1")
        # 网络重试：全部重放一遍。
        for index, (w, r) in enumerate(((450, .71), (520, .93), (650, .84)), start=1):
            replayed = self._add(measurement_no=f"M-{index}", wavelength_nm=w, response=r)
            self.assertTrue(replayed["replayed"])
        after = self.service.analyze(self.token, "LOT-1")
        self.assertEqual(after, before)
        self.assertEqual(after["spectrum"]["count"], 3)
        self.assertEqual(after["spectrum"]["peak_wavelength_nm"], 520.0)

    def test_concurrent_submissions_with_same_number_insert_once(self) -> None:
        results: list[dict] = []
        errors: list[Exception] = []

        def submit() -> None:
            try:
                results.append(self._add())
            except Exception as exc:  # noqa: BLE001 - 测试需要收集所有线程结果
                errors.append(exc)

        threads = [threading.Thread(target=submit) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 8)
        self.assertEqual({r["measurement_id"] for r in results}, {results[0]["measurement_id"]})
        self.assertEqual(self._count(), 1)
        events = self.service.audit(self.token, "LOT-1")
        self.assertEqual(sum(1 for e in events if e["event_type"] == "measurement"), 1)

    def test_concurrent_submissions_with_different_numbers_same_business_key(self) -> None:
        outcomes: list[str] = []
        lock = threading.Lock()

        def submit(number: str) -> None:
            try:
                result = self._add(measurement_no=number)
                with lock:
                    outcomes.append("ok" if result["replayed"] else "created")
            except Conflict:
                with lock:
                    outcomes.append("conflict")

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(submit, [f"M-{i}" for i in range(8)]))
        self.assertEqual(self._count(), 1)
        self.assertEqual(outcomes.count("created"), 1)
        self.assertEqual(sorted(outcomes).count("conflict") + outcomes.count("ok"), 7)


class RestartPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "photon.sqlite3")
        self.service = PhotonService(self.path)
        self.service.bootstrap_admin()
        token = self.service.auth.login("admin", "photon-admin")
        self.service.create_lot(token, "LOT-X", "PD", "R1", 10)
        self.service.add_measurement(token, "LOT-X", 450, .7, .01, "spec-1", "MEAS-1")
        self.service.add_measurement(token, "LOT-X", 520, .9, .01, "spec-1", "MEAS-2")
        self.service.add_measurement(token, "LOT-X", 650, .8, .01, "spec-1", "MEAS-3")
        self.token = token

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_replay_and_conflict_survive_restart(self) -> None:
        before = self.service.analyze(self.token, "LOT-X")
        del self.service
        restarted = PhotonService(self.path)
        token = restarted.auth.login("admin", "photon-admin")
        replay = restarted.add_measurement(token, "LOT-X", 520, .9, .01, "spec-1", "MEAS-2")
        self.assertTrue(replay["replayed"])
        count = restarted.db.execute("SELECT count(*) FROM measurements").fetchone()[0]
        self.assertEqual(count, 3)
        events = restarted.audit(token, "LOT-X")
        self.assertEqual(sum(1 for e in events if e["event_type"] == "measurement"), 3)
        with self.assertRaises(Conflict):
            restarted.add_measurement(token, "LOT-X", 530, .9, .01, "spec-1", "MEAS-2")
        with self.assertRaises(Conflict):
            restarted.add_measurement(token, "LOT-X", 520, .9, .01, "spec-9", "MEAS-2")
        self.assertEqual(restarted.analyze(token, "LOT-X"), before)


class LegacyMigrationTests(unittest.TestCase):
    def test_pre_fix_database_with_duplicates_is_deduped(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = str(Path(tmp.name) / "legacy.sqlite3")
        db = sqlite3.connect(path)
        db.executescript(
            """
            CREATE TABLE chip_lots(lot_id TEXT PRIMARY KEY, product TEXT, process_rev TEXT,
             wafer_count INTEGER, status TEXT, owner TEXT, created_at TEXT, updated_at TEXT);
            CREATE TABLE measurements(measurement_id TEXT PRIMARY KEY, lot_id TEXT,
             wavelength_nm REAL, response REAL, noise REAL, instrument TEXT,
             operator TEXT, measured_at TEXT, UNIQUE(lot_id,measurement_id));
            CREATE TABLE lot_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT, lot_id TEXT,
             event_type TEXT, actor TEXT, payload TEXT, created_at TEXT);
            CREATE TABLE approvals(lot_id TEXT, reviewer TEXT, decision TEXT, reason TEXT,
             created_at TEXT, PRIMARY KEY(lot_id,reviewer));
            """
        )
        db.execute("INSERT INTO chip_lots VALUES('L1','p','r',1,'engineering','a','t','t')")
        # 修复前：同一批次/仪器/波长因网络重试被记录两次。
        db.execute("INSERT INTO measurements VALUES('id-1','L1',520,.9,.01,'spec-1','a','t')")
        db.execute("INSERT INTO measurements VALUES('id-2','L1',520,.9,.01,'spec-1','a','t')")
        db.execute("INSERT INTO lot_events(lot_id,event_type,actor,payload,created_at) VALUES('L1','measurement','a','{\"measurement_id\": \"id-1\"}','t')")
        db.execute("INSERT INTO lot_events(lot_id,event_type,actor,payload,created_at) VALUES('L1','measurement','a','{\"measurement_id\": \"id-2\"}','t')")
        db.commit()
        db.close()

        service = PhotonService(path)
        rows = service.db.execute(
            "SELECT measurement_id FROM measurements ORDER BY measurement_id"
        ).fetchall()
        self.assertEqual([r[0] for r in rows], ["id-1"])
        events = service.db.execute(
            "SELECT json_extract(payload,'$.measurement_id') AS mid FROM lot_events WHERE event_type='measurement'"
        ).fetchall()
        self.assertEqual([r["mid"] for r in events], ["id-1"])


class HttpApiTests(unittest.TestCase):
    def _request(self, method: str, path: str, body: dict | None = None, token: str | None = None):
        payload = json.dumps(body).encode() if body is not None else b""
        handler = Handler.__new__(Handler)
        handler.path = path
        handler.headers = {"Content-Length": str(len(payload))}
        if token:
            handler.headers["Authorization"] = f"Bearer {token}"
        handler.rfile = type("R", (), {"read": lambda self, n: payload})()
        captured: dict = {}

        def capture_json(status, response_body):
            captured["status"] = status
            captured["body"] = response_body

        handler._json = capture_json  # type: ignore[method-assign]
        (handler.do_POST if method == "POST" else handler.do_GET)()
        return captured["status"], captured["body"]

    def setUp(self) -> None:
        Handler.service = PhotonService()
        Handler.service.bootstrap_admin()
        _, body = self._request("POST", "/login", {"user_id": "admin", "password": "photon-admin"})
        self.token = body["token"]
        self._request("POST", "/lots", {"lot_id": "LOT-H", "product": "p", "process_rev": "r", "wafer_count": 5}, self.token)

    def test_create_then_replay_status_codes(self) -> None:
        payload = {"wavelength_nm": 450, "response": .7, "noise": .01, "instrument": "spec-1", "measurement_no": "H-1"}
        status1, body1 = self._request("POST", "/lots/LOT-H/measurements", payload, self.token)
        status2, body2 = self._request("POST", "/lots/LOT-H/measurements", payload, self.token)
        self.assertEqual((status1, status2), (201, 200))
        self.assertEqual(body1["measurement_id"], body2["measurement_id"])
        self.assertTrue(body2["replayed"])

    def test_conflict_returns_409(self) -> None:
        base = {"wavelength_nm": 450, "response": .7, "noise": .01, "instrument": "spec-1", "measurement_no": "H-1"}
        self._request("POST", "/lots/LOT-H/measurements", base, self.token)
        changed = dict(base, wavelength_nm=520)
        status, body = self._request("POST", "/lots/LOT-H/measurements", changed, self.token)
        self.assertEqual(status, 409)
        self.assertIn("冲突", body["error"])


if __name__ == "__main__":
    unittest.main()
