"""光子工厂领域错误类型。"""

from __future__ import annotations


class Conflict(Exception):
    """业务请求与既有记录冲突（同一测量编号对应不同内容，或业务键已存在）。"""
