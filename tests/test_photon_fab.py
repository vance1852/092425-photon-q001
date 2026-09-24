"""photon_fab 测量写入幂等、冲突、并发与重启行为测试。"""

from __future__ import annotations

import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from photon_fab.api import Handler
from photon_fab.errors import Conflict
from photon_fab.service import PhotonService


class MeasurementIdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = PhotonService(":memory:")
        self.service.bootstrap_admin()
        self.token = self.service.auth.login("admin", "photon-admin")
        self.service.create_lot(self.token, "LOT-1", "CMOS image sensor", "P3.2", 10)
        self.points = ((450, .71), (520, .93), (650, .84))
        for seq, (wavelength, response) in enumerate(self.points, start=1):
            self.service.add_measurement(
                self.token, "LOT-1", wavelength, response, .01,
                "spectrometer-1", measurement_no=f"MEAS-{seq:04d}",
            )

    def count(self) -> int:
        return self.service.db.execute(
            "SELECT count(*) FROM measurements WHERE lot_id='LOT-1'"
        ).fetchone()[0]

    def measurement_events(self) -> int:
        return self.service.db.execute(
            "SELECT count(*) FROM lot_events WHERE lot_id='LOT-1' AND event_type='measurement'"
        ).fetchone()[0]

    def test_replay_returns_original_record_without_new_row_or_event(self) -> None:
        first = self.service.add_measurement(
            self.token, "LOT-1", 450, .71, .01, "spectrometer-1",
            measurement_no="MEAS-0001",
        )
        second = self.service.add_measurement(
            self.token, "LOT-1", 450, .71, .01, "spectrometer-1",
            measurement_no="MEAS-0001",
        )
        self.assertEqual(first, second)
        self.assertEqual(self.count(), 3)
        self.assertEqual(self.measurement_events(), 3)

    def test_replay_survives_process_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "photon.sqlite3")
            first = PhotonService(path)
            first.bootstrap_admin()
            token = first.auth.login("admin", "photon-admin")
            first.create_lot(token, "LOT-R", "CMOS image sensor", "P3.2", 10)
            original = first.add_measurement(
                token, "LOT-R", 450, .71, .01, "spectrometer-1",
                measurement_no="MEAS-0001",
            )
            first.db.close()

            # 模拟进程重启：重新打开同一个持久化数据库。
            restarted = PhotonService(path)
            restarted.bootstrap_admin()
            token2 = restarted.auth.login("admin", "photon-admin")
            replayed = restarted.add_measurement(
                token2, "LOT-R", 450, .71, .01, "spectrometer-1",
                measurement_no="MEAS-0001",
            )
            self.assertEqual(replayed, original)
            self.assertEqual(restarted.db.execute(
                "SELECT count(*) FROM measurements").fetchone()[0], 1)
            self.assertEqual(restarted.db.execute(
                "SELECT count(*) FROM lot_events WHERE event_type='measurement'"
            ).fetchone()[0], 1)
            with self.assertRaises(Conflict):
                restarted.add_measurement(
                    token2, "LOT-R", 460, .71, .01, "spectrometer-1",
                    measurement_no="MEAS-0001",
                )
            with self.assertRaises(Conflict):
                restarted.add_measurement(
                    token2, "LOT-R", 450, .71, .01, "spectrometer-2",
                    measurement_no="MEAS-0001",
                )
            restarted.db.close()

    def test_different_wavelength_or_instrument_conflicts(self) -> None:
        with self.assertRaises(Conflict):
            self.service.add_measurement(
                self.token, "LOT-1", 460, .71, .01, "spectrometer-1",
                measurement_no="MEAS-0001",
            )
        with self.assertRaises(Conflict):
            self.service.add_measurement(
                self.token, "LOT-1", 450, .71, .01, "spectrometer-2",
                measurement_no="MEAS-0001",
            )
        with self.assertRaises(Conflict):
            self.service.add_measurement(
                self.token, "LOT-1", 450, .99, .01, "spectrometer-1",
                measurement_no="MEAS-0001",
            )
        self.assertEqual(self.count(), 3)
        self.assertEqual(self.measurement_events(), 3)

    def test_concurrent_same_measurement_submits_once(self) -> None:
        barrier = threading.Barrier(8)
        results: list[dict] = []
        errors: list[Exception] = []

        def submit() -> None:
            barrier.wait()
            try:
                results.append(self.service.add_measurement(
                    self.token, "LOT-1", 700, .66, .02,
                    "spectrometer-1", measurement_no="MEAS-0009",
                ))
            except Exception as exc:  # noqa: BLE001 - 记录线程内异常
                errors.append(exc)

        threads = [threading.Thread(target=submit) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 8)
        self.assertEqual({r["measurement_id"] for r in results}, {results[0]["measurement_id"]})
        self.assertEqual(self.count(), 4)
        self.assertEqual(self.measurement_events(), 4)

    def test_concurrent_conflicting_payloads_never_duplicate(self) -> None:
        barrier = threading.Barrier(6)
        errors: list[Exception] = []

        def submit(wavelength: float) -> None:
            barrier.wait()
            try:
                self.service.add_measurement(
                    self.token, "LOT-1", wavelength, .71, .01,
                    "spectrometer-1", measurement_no="MEAS-0010",
                )
            except Conflict as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=submit, args=(800.0 if i % 2 else 801.0,))
            for i in range(6)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(self.count(), 4)
        self.assertEqual(self.measurement_events(), 4)
        self.assertEqual(len(errors), 3)

    def test_analysis_is_stable_after_replays(self) -> None:
        before = self.service.analyze(self.token, "LOT-1")
        for seq, (wavelength, response) in enumerate(self.points, start=1):
            for _ in range(3):
                self.service.add_measurement(
                    self.token, "LOT-1", wavelength, response, .01,
                    "spectrometer-1", measurement_no=f"MEAS-{seq:04d}",
                )
        after = self.service.analyze(self.token, "LOT-1")
        self.assertEqual(before["spectrum"], after["spectrum"])
        self.assertEqual(before["yield"], after["yield"])
        self.assertEqual(before["response_ci"], after["response_ci"])
        self.assertEqual(after["spectrum"]["peak_wavelength_nm"], 520)

    def test_measurement_no_is_required(self) -> None:
        with self.assertRaises(ValueError):
            self.service.add_measurement(
                self.token, "LOT-1", 900, .5, .01, "spectrometer-1",
            )

    def test_audit_order_is_append_only_with_event_id(self) -> None:
        events = self.service.audit(self.token, "LOT-1")
        self.assertEqual([e["event_type"] for e in events],
                         ["created", "measurement", "measurement", "measurement"])
        ids = [e["event_id"] for e in events]
        self.assertEqual(ids, sorted(ids))
        self.assertEqual(len(ids), len(set(ids)))


class CrossProcessReplayTests(unittest.TestCase):
    """两个独立服务实例（各自独立连接）共享同一文件库，等价于两个进程。"""

    def test_concurrent_submit_and_replay_across_connections(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "photon.sqlite3")
            first = PhotonService(path)
            first.bootstrap_admin()
            token = first.auth.login("admin", "photon-admin")
            first.create_lot(token, "LOT-P", "sensor", "P1", 4)
            first.db.close()

            worker_a = PhotonService(path)
            worker_b = PhotonService(path)
            ta = worker_a.auth.login("admin", "photon-admin")
            tb = worker_b.auth.login("admin", "photon-admin")
            barrier = threading.Barrier(2)
            outcomes: dict[str, dict] = {}

            def submit(worker, tok, name) -> None:
                barrier.wait()
                outcomes[name] = worker.add_measurement(
                    tok, "LOT-P", 530, .88, .01,
                    "spectrometer-1", measurement_no="MEAS-X1",
                )

            t1 = threading.Thread(target=submit, args=(worker_a, ta, "a"))
            t2 = threading.Thread(target=submit, args=(worker_b, tb, "b"))
            t1.start(); t2.start(); t1.join(); t2.join()

            self.assertEqual(outcomes["a"], outcomes["b"])
            self.assertEqual(worker_a.db.execute(
                "SELECT count(*) FROM measurements WHERE lot_id='LOT-P'"
            ).fetchone()[0], 1)
            self.assertEqual(worker_a.db.execute(
                "SELECT count(*) FROM lot_events WHERE lot_id='LOT-P' AND event_type='measurement'"
            ).fetchone()[0], 1)

            with self.assertRaises(Conflict):
                worker_b.add_measurement(
                    tb, "LOT-P", 600, .88, .01,
                    "spectrometer-1", measurement_no="MEAS-X1",
                )
            with self.assertRaises(Conflict):
                worker_b.add_measurement(
                    tb, "LOT-P", 530, .88, .01,
                    "spectrometer-9", measurement_no="MEAS-X1",
                )
            worker_a.db.close()
            worker_b.db.close()


class MeasurementHttpApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        path = str(Path(self.tmp.name) / "photon.sqlite3")
        Handler.service = PhotonService(path)
        Handler.service.bootstrap_admin()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        token = self.post("/login", {"user_id": "admin", "password": "photon-admin"})[1]["token"]
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self.post("/lots", {"lot_id": "LOT-9", "product": "sensor",
                            "process_rev": "P1", "wafer_count": 4})

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        Handler.service.db.close()
        self.tmp.cleanup()

    def post(self, path: str, body: dict) -> tuple[int, dict]:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        payload = json.dumps(body).encode()
        headers = {"Content-Type": "application/json"}
        if hasattr(self, "headers"):
            headers.update(self.headers)
        conn.request("POST", path, payload, headers)
        response = conn.getresponse()
        data = json.loads(response.read())
        conn.close()
        return response.status, data

    def measurement_body(self, wavelength: float = 450, instrument: str = "spectrometer-1") -> dict:
        return {"measurement_no": "M-HTTP-1", "wavelength_nm": wavelength,
                "response": .72, "noise": .01, "instrument": instrument}

    def test_http_replay_is_idempotent_and_conflict_is_409(self) -> None:
        status1, body1 = self.post("/lots/LOT-9/measurements", self.measurement_body())
        self.assertEqual(status1, 201)
        status2, body2 = self.post("/lots/LOT-9/measurements", self.measurement_body())
        self.assertEqual(status2, 201)
        self.assertEqual(body1, body2)
        status3, body3 = self.post("/lots/LOT-9/measurements",
                                   self.measurement_body(wavelength=555))
        self.assertEqual(status3, 409)
        self.assertIn("different wavelength or instrument", body3["error"])
        count = Handler.service.db.execute(
            "SELECT count(*) FROM measurements WHERE lot_id='LOT-9'"
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_http_concurrent_replays_create_one_record(self) -> None:
        responses: list[tuple[int, dict]] = []
        lock = threading.Lock()
        barrier = threading.Barrier(6)

        def post() -> None:
            barrier.wait()
            with lock:
                responses.append(self.post("/lots/LOT-9/measurements", self.measurement_body(470)))

        threads = [threading.Thread(target=post) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual({status for status, _ in responses}, {201})
        self.assertEqual(len({body["measurement_id"] for _, body in responses}), 1)
        count = Handler.service.db.execute(
            "SELECT count(*) FROM measurements WHERE lot_id='LOT-9'"
        ).fetchone()[0]
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
