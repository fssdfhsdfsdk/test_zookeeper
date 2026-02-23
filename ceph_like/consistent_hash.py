"""
一致性哈希环模块
用于数据分片和副本分配
"""

import bisect
import hashlib
from typing import Dict, List, Optional, Any, Set, Tuple


class ConsistentHashRing:
    """一致性哈希环实现"""

    def __init__(self, virtual_nodes: int = 150):
        """
        初始化一致性哈希环
        :param virtual_nodes: 虚拟节点数量，越多分布越均匀
        """
        self.virtual_nodes = virtual_nodes
        self.ring: List[int] = []  # 排序后的哈希值列表
        self.ring_keys: List[Tuple[int, str]] = []  # (hash, node_id) 排序后
        self.nodes: Dict[str, Dict[str, Any]] = {}  # {node_id: node_info}
        self.node_virtuals: Dict[str, List[int]] = {}  # {node_id: [hash1, hash2, ...]}

    def _hash(self, key: str) -> int:
        """计算哈希值"""
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

    def _hash_range(self, key: str, start: int, end: int) -> int:
        """计算范围哈希（用于查找落在范围内的副本）"""
        return int(
            hashlib.sha256(f"{key}:{start}:{end}".encode("utf-8")).hexdigest(), 16
        ) % (2**32)

    def add_node(self, node_info: Dict[str, Any]) -> None:
        """
        添加节点到环
        :param node_info: {id, host, port, ...}
        """
        node_id = node_info["id"]
        if node_id in self.nodes:
            self.remove_node(node_id)

        self.nodes[node_id] = node_info
        self.node_virtuals[node_id] = []

        # 添加虚拟节点
        for i in range(self.virtual_nodes):
            v_key = f"{node_id}:vn-{i}"
            h = self._hash(v_key)
            self.ring.append(h)
            self.node_virtuals[node_id].append(h)

        self.ring.sort()
        self._rebuild_ring_keys()

    def remove_node(self, node_id: str) -> None:
        """从环中移除节点"""
        if node_id not in self.nodes:
            return

        # 移除虚拟节点
        for h in self.node_virtuals.get(node_id, []):
            self.ring.remove(h)

        del self.nodes[node_id]
        del self.node_virtuals[node_id]
        self._rebuild_ring_keys()

    def _rebuild_ring_keys(self) -> None:
        """重建ring_keys索引"""
        self.ring_keys = []
        for node_id in self.nodes:
            for h in self.node_virtuals.get(node_id, []):
                self.ring_keys.append((h, node_id))
        self.ring_keys.sort(key=lambda x: x[0])

    def get_node(self, key: str) -> Optional[Dict[str, Any]]:
        """
        获取key对应的节点
        :param key: 键
        :return: 节点信息字典
        """
        if not self.ring:
            return None

        h = self._hash(key)

        # 二分查找顺时针第一个节点
        idx = bisect.bisect(self.ring, h)
        if idx >= len(self.ring):
            idx = 0

        # 找到对应的真实节点
        target_hash = self.ring[idx]
        node_id = self._find_node_by_hash(target_hash)

        return self.nodes.get(node_id)

    def _find_node_by_hash(self, target_hash: int) -> Optional[str]:
        """根据哈希值找到对应的真实节点ID"""
        for node_id, hashes in self.node_virtuals.items():
            if target_hash in hashes:
                return node_id
        return None

    def get_replicas(self, key: str, n: int = 3) -> List[Dict[str, Any]]:
        """
        获取key对应的n个副本节点（顺时针）
        :param key: 键
        :param n: 副本数量
        :return: 节点信息列表
        """
        if not self.ring or n <= 0:
            return []

        replicas = []
        h = self._hash(key)

        # 找到起始位置
        idx = bisect.bisect(self.ring, h)
        if idx >= len(self.ring):
            idx = 0

        visited = set()
        while len(replicas) < n and len(visited) < len(self.nodes):
            target_hash = self.ring[idx]
            node_id = self._find_node_by_hash(target_hash)

            if node_id and node_id not in visited:
                visited.add(node_id)
                replicas.append(self.nodes[node_id])

            idx = (idx + 1) % len(self.ring)

        return replicas

    def get_range_for_node(self, node_id: str) -> Tuple[int, int]:
        """
        获取节点负责的哈希范围 [start, end)
        :return: (start_hash, end_hash)
        """
        if node_id not in self.node_virtuals:
            return (0, 0)

        virtuals = sorted(self.node_virtuals[node_id])

        # 找到这个节点负责的范围
        # 从最后一个虚拟节点到第一个虚拟节点，顺时针
        start_hash = virtuals[-1] + 1 if virtuals[-1] < 2**128 - 1 else 0
        # 到第一个虚拟节点之前
        end_hash = virtuals[0]

        # 实际上我们应该找这个节点控制的范围
        # 由于虚拟节点是均匀分布的，节点负责的范围是:
        # 从前一个节点的最后一个虚拟节点+1，到这个节点的第一个虚拟节点
        all_nodes_sorted = sorted(self.nodes.keys())
        node_idx = all_nodes_sorted.index(node_id)

        if node_idx == 0:
            # 最后一个节点的虚拟节点的最大值+1
            prev_node = all_nodes_sorted[-1]
        else:
            prev_node = all_nodes_sorted[node_idx - 1]

        prev_virtuals = sorted(self.node_virtuals.get(prev_node, []))
        start = (prev_virtuals[-1] + 1) % (2**128)

        first_virtual = virtuals[0]

        return (start, first_virtual)

    def get_node_for_hash(self, hash_value: int) -> Optional[Dict[str, Any]]:
        """根据哈希值直接获取节点"""
        if not self.ring:
            return None

        idx = bisect.bisect(self.ring, hash_value)
        if idx >= len(self.ring):
            idx = 0

        target_hash = self.ring[idx]
        node_id = self._find_node_by_hash(target_hash)

        return self.nodes.get(node_id)

    def get_all_nodes(self) -> List[Dict[str, Any]]:
        """获取所有节点"""
        return list(self.nodes.values())

    def __len__(self) -> int:
        return len(self.nodes)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self.nodes


class HashRingManager:
    """哈希环管理器，支持动态更新和变化通知"""

    def __init__(self, virtual_nodes: int = 150):
        self.ring = ConsistentHashRing(virtual_nodes)
        self._callbacks: List[callable] = []

    def add_node(self, node_info: Dict[str, Any]) -> bool:
        """添加节点"""
        if node_info["id"] in self.ring:
            return False

        old_nodes = set(self.ring.nodes.keys())
        self.ring.add_node(node_info)
        new_nodes = set(self.ring.nodes.keys())

        added = new_nodes - old_nodes
        if added:
            self._notify_change("add", added)

        return True

    def remove_node(self, node_id: str) -> bool:
        """移除节点"""
        if node_id not in self.ring:
            return False

        old_nodes = set(self.ring.nodes.keys())
        self.ring.remove_node(node_id)
        new_nodes = set(self.ring.nodes.keys())

        removed = old_nodes - new_nodes
        if removed:
            self._notify_change("remove", removed)

        return True

    def register_change_callback(self, callback: callable) -> None:
        """注册变化回调"""
        self._callbacks.append(callback)

    def _notify_change(self, change_type: str, node_ids: Set[str]) -> None:
        """通知变化"""
        for callback in self._callbacks:
            try:
                callback(change_type, node_ids)
            except Exception as e:
                print(f"Callback error: {e}")


if __name__ == "__main__":
    # 测试一致性哈希环
    ring = ConsistentHashRing(virtual_nodes=10)

    # 添加节点
    for i in range(3):
        ring.add_node(
            {
                "id": f"osd-{i}",
                "host": f"127.0.0.1",
                "port": 9100 + i,
                "status": "online",
            }
        )

    print(f"节点数: {len(ring.nodes)}")

    # 测试副本分配
    for key in ["key1", "key2", "key3", "key4", "key5"]:
        replicas = ring.get_replicas(key, 3)
        print(f"{key} -> {[n['id'] for n in replicas]}")

    # 测试节点移除
    print("\n移除 osd-0 后:")
    ring.remove_node("osd-0")
    for key in ["key1", "key2", "key3"]:
        replicas = ring.get_replicas(key, 3)
        print(f"{key} -> {[n['id'] for n in replicas]}")
