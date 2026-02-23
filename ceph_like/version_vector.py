"""
版本向量 (Vector Clock) 模块
用于解决分布式环境下的并发写入冲突
"""

import json
import time
from collections import defaultdict
from typing import Dict, Any, Optional, Tuple


class VectorClock:
    """版本向量：每个节点维护自己的逻辑时钟"""

    def __init__(self, clocks: Optional[Dict[str, int]] = None, node_id: str = ""):
        """
        初始化版本向量
        :param clocks: {node_id: counter} 字典
        :param node_id: 当前节点ID
        """
        self._clocks = clocks if clocks else {}
        self._node_id = node_id

    def increment(self, node_id: str = None) -> "VectorClock":
        """增加当前节点的版本号"""
        nid = node_id or self._node_id
        if nid not in self._clocks:
            self._clocks[nid] = 0
        self._clocks[nid] += 1
        return self

    def merge(self, other: "VectorClock") -> "VectorClock":
        """合并另一个版本向量（取最大值）"""
        result_clocks = dict(self._clocks)
        for node_id, counter in other._clocks.items():
            result_clocks[node_id] = max(result_clocks.get(node_id, 0), counter)
        return VectorClock(result_clocks, self._node_id)

    def happens_before(self, other: "VectorClock") -> bool:
        """判断当前版本是否在另一个版本之前（可比较）"""
        dominated = False  # 是否严格小于
        for node_id, counter in other._clocks.items():
            self_counter = self._clocks.get(node_id, 0)
            if self_counter > counter:
                return False
            if self_counter < counter:
                dominated = True
        return dominated

    def is_concurrent(self, other: "VectorClock") -> bool:
        """判断两个版本是否并发（不可比较）"""
        return not self.happens_before(other) and not other.happens_before(self)

    def compare(self, other: "VectorClock") -> Tuple[str, "VectorClock"]:
        """
        比较两个版本向量，返回:
        - "before": self 在 other 之前
        - "after": self 在 other 之后
        - "concurrent": 并发冲突
        - "equal": 相等
        """
        if self._clocks == other._clocks:
            return "equal", self

        if self.happens_before(other):
            return "before", other.merge(self)

        if other.happens_before(self):
            return "after", self.merge(other)

        # 并发：取最新版本
        return "concurrent", self.merge(other)

    def to_dict(self) -> Dict[str, int]:
        """转换为字典"""
        return dict(self._clocks)

    def to_json(self) -> str:
        """序列化为JSON"""
        return json.dumps(self._clocks)

    @classmethod
    def from_json(cls, json_str: str) -> "VectorClock":
        """从JSON反序列化"""
        clocks = json.loads(json_str)
        return cls(clocks)

    def __repr__(self):
        return f"VectorClock({self._clocks})"

    def __str__(self):
        return json.dumps(self._clocks, sort_keys=True)


class VersionedValue:
    """带版本的数据"""

    def __init__(self, value: Any, vector_clock: VectorClock, timestamp: float = None):
        self.value = value
        self.vector_clock = vector_clock
        self.timestamp = timestamp or time.time()

    def to_dict(self) -> dict:
        return {
            "value": self.value,
            "vector_clock": self.vector_clock.to_dict(),
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "VersionedValue":
        vc = VectorClock(data.get("vector_clock", {}))
        return cls(data["value"], vc, data.get("timestamp"))

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "VersionedValue":
        return cls.from_dict(json.loads(json_str))


class ConflictResolver:
    """冲突解决器"""

    @staticmethod
    def resolve_by_timestamp(values: list[VersionedValue]) -> VersionedValue:
        """按时间戳选择最新的"""
        if not values:
            raise ValueError("No values to resolve")
        return max(values, key=lambda v: v.timestamp)

    @staticmethod
    def resolve_by_version(values: list[VersionedValue]) -> VersionedValue:
        """按版本向量选择最新的（合并后）"""
        if not values:
            raise ValueError("No values to resolve")

        # 合并所有版本向量
        merged_vc = values[0].vector_clock
        for v in values[1:]:
            merged_vc = merged_vc.merge(v.vector_clock)

        # 返回版本最新的
        return max(values, key=lambda v: sum(v.vector_clock.to_dict().values()))

    @staticmethod
    def resolve_by_custom(values: list[VersionedValue], key: str) -> VersionedValue:
        """自定义策略：按值中的某个key比较"""
        if not values:
            raise ValueError("No values to resolve")

        # 找到包含指定key的最新值
        valid = [v for v in values if isinstance(v.value, dict) and key in v.value]
        if valid:
            return max(valid, key=lambda v: v.value.get(key, 0))
        return values[0]


if __name__ == "__main__":
    # 测试版本向量
    vc1 = VectorClock({"node-a": 1, "node-b": 1})
    vc2 = VectorClock({"node-a": 2, "node-b": 1})
    vc3 = VectorClock({"node-a": 1, "node-b": 2})
    vc4 = VectorClock({"node-a": 1, "node-b": 1})

    print(f"vc1: {vc1}")
    print(f"vc2: {vc2}")
    print(f"vc3: {vc3}")
    print(f"vc1 < vc2: {vc1.happens_before(vc2)}")
    print(f"vc2 > vc1: {vc2.happens_before(vc1)}")
    print(f"vc1 与 vc3 并发: {vc1.is_concurrent(vc3)}")
    print(f"vc1 == vc4: {vc1._clocks == vc4._clocks}")

    # 测试冲突解决
    vv1 = VersionedValue(
        {"data": "value1"}, VectorClock({"node-a": 1, "node-b": 1}), 1000
    )
    vv2 = VersionedValue(
        {"data": "value2"}, VectorClock({"node-a": 2, "node-b": 1}), 1001
    )

    resolved = ConflictResolver.resolve_by_timestamp([vv1, vv2])
    print(f"Resolved value: {resolved.value}")
