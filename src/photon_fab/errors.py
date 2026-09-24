"""photon_fab 服务层可观察错误。"""

from __future__ import annotations


class Conflict(ValueError):
    """同一业务编号已存在，但本次提交内容与首次记录不一致（HTTP 409）。"""
