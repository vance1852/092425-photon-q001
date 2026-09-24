"""容器和快照检查使用的冒烟验收命令。"""

from __future__ import annotations

import argparse
import json

from .service import PhotonService


def run() -> dict:
    service = PhotonService()
    service.bootstrap_admin()
    token = service.auth.login("admin", "photon-admin")
    service.create_lot(token, "LOT-DEMO", "CMOS image sensor", "P3.2", 10)
    for index, (wavelength, response) in enumerate(((450, .71), (520, .93), (650, .84)), start=1):
        service.add_measurement(token, "LOT-DEMO", wavelength, response, .01, "spectrometer-1", f"MEAS-{index:04d}")
    # 模拟网络重试：同一测量编号重放必须返回原记录且不新增审计事件。
    replay = service.add_measurement(token, "LOT-DEMO", 520, .93, .01, "spectrometer-1", "MEAS-0002")
    result = service.analyze(token, "LOT-DEMO")
    service.approve(token, "LOT-DEMO", "hold", "awaiting quality review")
    events = service.audit(token, "LOT-DEMO")
    assert replay["replayed"] is True
    assert result["spectrum"]["count"] == 3
    return {"status": "ok", "lot": result["lot_id"], "peak": result["spectrum"]["peak_wavelength_nm"], "events": len(events)}


def main() -> None:
    argparse.ArgumentParser().parse_args()
    print(json.dumps(run(), ensure_ascii=False))


if __name__ == "__main__":
    main()
