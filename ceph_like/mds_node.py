"""
MDS (Metadata Server) 元数据服务器
负责元数据管理、块设备分配、Leader选举（主备模式）
"""

import os
import sys
import json
import time
import threading
import logging
import signal
import hashlib
from typing import Dict, Any, List, Optional

from zk_manager import ZKManager, NodeType
from consistent_hash import ConsistentHashRing

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [MDS-%(name)s] - %(message)s"
)
logger = logging.getLogger("MDS")


class MDSNode:
    """MDS 元数据服务器"""

    def __init__(
        self,
        mds_id: str,
        host: str = "127.0.0.1",
        port: int = 9110,
        zk_hosts: str = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183",
    ):
        self.mds_id = mds_id
        self.host = host
        self.port = port
        self.zk_hosts = zk_hosts

        # ZK 管理器
        self.zk = ZKManager(hosts=zk_hosts)

        # 状态
        self.is_leader = False
        self.running = False

        # OSD 缓存
        self.osd_ring = ConsistentHashRing()
        self.active_osds: Dict[str, Dict[str, Any]] = {}

        # 设备管理
        self.devices: Dict[str, Dict] = {}  # {device_id: device_info}
        self.device_blocks: Dict[str, List[str]] = {}  # {device_id: [block_id,...]}

        # 块分配
        self.block_counter = 0
        self.block_lock = threading.Lock()

        # 信号处理
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def start(self):
        """启动 MDS"""
        logger.info(f"🚀 启动 MDS: {self.mds_id}")

        # 连接 ZK
        if not self.zk.start():
            logger.error("❌ ZK 连接失败")
            return

        self.running = True

        # 注册到 ZK
        self._register()

        # 监听 OSD 变化
        self._watch_osds()

        # 启动 Leader 选举
        self._start_leader_election()

        # 启动业务处理
        self._start_business()

        logger.info(f"✅ MDS 启动完成: {self.mds_id}")

    def _register(self):
        """注册到 ZK"""
        mds_info = {
            "id": self.mds_id,
            "host": self.host,
            "port": self.port,
            "status": "online",
        }
        self.zk.register_mds(mds_info)
        logger.info(f"✅ 已注册到 ZK")

    def _watch_osds(self):
        """监听 OSD 变化"""

        def on_osds_change(osds: List[Dict]):
            # 更新哈希环
            old_osds = set(self.active_osds.keys())
            new_osds = {osd["id"]: osd for osd in osds}

            with self.block_lock:
                self.active_osds = new_osds

                # 更新哈希环
                self.osd_ring = ConsistentHashRing()
                for osd in osds:
                    if osd.get("status") == "online":
                        self.osd_ring.add_node(osd)

            added = set(new_osds.keys()) - old_osds
            removed = old_osds - set(new_osds.keys())

            if added:
                logger.warning(f"🟢 OSD 加入: {added}")
            if removed:
                logger.error(f"🔴 OSD 下线: {removed}")

            if self.is_leader:
                logger.info(f"📊 可用 OSD: {list(self.active_osds.keys())}")

        self.zk.watch_osds(on_osds_change)

    def _start_leader_election(self):
        """启动 Leader 选举"""

        def election_loop():
            while self.running:
                try:
                    if self.zk.elect_leader(self.mds_id):
                        self._become_leader()
                    else:
                        self._become_follower()
                        # 监听 Leader 变化
                        self.zk.watch_leader(self._on_leader_change)

                except Exception as e:
                    logger.error(f"选举异常: {e}")

                # 等待一段时间再重试
                time.sleep(2)

        t = threading.Thread(target=election_loop, daemon=True)
        t.start()

    def _on_leader_change(self, leader_id: Optional[str]):
        """Leader 变化回调"""
        if leader_id is None:
            logger.warning("⚠️ Leader 节点消失，重新选举")
            self.is_leader = False

    def _become_leader(self):
        """成为 Leader"""
        if not self.is_leader:
            self.is_leader = True
            logger.critical(f"👑 {self.mds_id} 成为 ACTIVE MDS!")

            # 加载设备元数据
            self._load_devices()

    def _become_follower(self):
        """成为 Follower"""
        if self.is_leader:
            self.is_leader = False
            logger.warning(f"📉 {self.mds_id} 降级为 STANDBY")

    def _load_devices(self):
        """从 ZK 加载设备信息"""
        try:
            blocks = self.zk.get_all_blocks()
            for block in blocks:
                device_id = block.get("device_id")
                if device_id:
                    if device_id not in self.device_blocks:
                        self.device_blocks[device_id] = []
                    block_id = block.get("block_id")
                    if block_id and block_id not in self.device_blocks[device_id]:
                        self.device_blocks[device_id].append(block_id)

            logger.info(f"💾 已加载 {len(self.device_blocks)} 个设备")
        except Exception as e:
            logger.error(f"加载设备失败: {e}")

    def _start_business(self):
        """启动业务处理"""

        def business_loop():
            while self.running:
                if self.is_leader:
                    # Leader 职责
                    self._sync_metadata()
                time.sleep(10)

        t = threading.Thread(target=business_loop, daemon=True)
        t.start()

    def _sync_metadata(self):
        """同步元数据到 ZK（Leader 定期执行）"""
        # 可以在此实现定期元数据同步
        pass

    # ========== 设备管理 API ==========

    def create_device(
        self, client_id: str, device_id: str, size_gb: int, block_size: int = 4
    ) -> bool:
        """
        创建设备
        :param client_id: 客户端ID
        :param device_id: 设备ID
        :param size_gb: 大小(GB)
        :param block_size: 块大小(MB)
        """
        if not self.is_leader:
            logger.error("❌ 只有 Leader 才能创建设备")
            return False

        try:
            # 计算块数量
            total_blocks = (size_gb * 1024) // block_size

            # 分配块
            blocks = []
            for i in range(total_blocks):
                block_id = f"{device_id}-block-{i}"

                # 使用一致性哈希选择主 OSD
                replicas = self.osd_ring.get_replicas(block_id, 3)
                if len(replicas) < 3:
                    logger.error("❌ OSD 数量不足")
                    return False

                osd_primary = replicas[0]["id"]
                osd_replicas = [replicas[1]["id"], replicas[2]["id"]]

                # 创建块元数据
                block_meta = {
                    "block_id": block_id,
                    "device_id": device_id,
                    "index": i,
                    "primary_osd": osd_primary,
                    "replica_osds": osd_replicas,
                    "status": "allocated",
                }

                # 写入 ZK
                if not self.zk.create_block(block_id, block_meta):
                    logger.error(f"❌ 创建块失败: {block_id}")
                    continue

                blocks.append(block_id)

            # 创建设备元数据
            device_meta = {
                "device_id": device_id,
                "client_id": client_id,
                "size_gb": size_gb,
                "block_size": block_size,
                "total_blocks": total_blocks,
                "blocks": blocks,
                "status": "active",
                "created_at": time.time(),
            }

            # 写入 ZK
            if not self.zk.create_device(client_id, device_id, device_meta):
                logger.error(f"❌ 创建设备失败: {device_id}")
                return False

            # 更新内存
            self.devices[device_id] = device_meta
            self.device_blocks[device_id] = blocks

            logger.info(
                f"✅ 设备创建: {device_id}, {total_blocks} blocks, "
                f"primary={osd_primary}"
            )
            return True

        except Exception as e:
            logger.error(f"创建设备异常: {e}")
            return False

    def get_device(self, device_id: str) -> Optional[Dict]:
        """获取设备信息"""
        if device_id in self.devices:
            return self.devices[device_id]

        # 从 ZK 加载
        device = self.zk.get_device(device_id)
        if device:
            self.devices[device_id] = device
        return device

    def get_client_devices(self, client_id: str) -> List[Dict]:
        """获取客户端设备列表"""
        devices = self.zk.get_client_devices(client_id)
        for d in devices:
            self.devices[d["device_id"]] = d
        return devices

    def get_osd_topology(self) -> Dict[str, Any]:
        """获取 OSD 拓扑"""
        with self.block_lock:
            return {
                "total_osds": len(self.active_osds),
                "osds": list(self.active_osds.values()),
                "ring": {node["id"]: node for node in self.osd_ring.get_all_nodes()},
            }

    def get_cluster_status(self) -> Dict[str, Any]:
        """获取集群状态"""
        return {
            "mds_id": self.mds_id,
            "is_leader": self.is_leader,
            "active_osds": len(self.active_osds),
            "devices": len(self.devices),
            "total_blocks": sum(len(blocks) for blocks in self.device_blocks.values()),
        }

    def _shutdown(self, signum, frame):
        """关闭 MDS"""
        logger.warning("收到关闭信号...")
        self.running = False

        self.zk.stop()

        logger.info("MDS 已关闭")
        os._exit(0)

    def stop(self):
        """停止 MDS"""
        self._shutdown(None, None)


class MDSCluster:
    """MDS 集群管理（启动多个 MDS 实例）"""

    def __init__(self, zk_hosts: str = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"):
        self.zk_hosts = zk_hosts
        self.nodes: Dict[str, MDSNode] = {}

    def start_node(self, mds_id: str, port: int = 9110):
        """启动一个 MDS 节点"""
        node = MDSNode(mds_id=mds_id, port=port, zk_hosts=self.zk_hosts)
        threading.Thread(target=node.start, daemon=True).start()
        self.nodes[mds_id] = node
        return node

    def wait_for_leader(self, timeout: int = 30) -> Optional[str]:
        """等待 Leader 选举完成"""
        zk = ZKManager(hosts=self.zk_hosts)
        zk.start()

        start = time.time()
        while time.time() - start < timeout:
            leader = zk.get_leader()
            if leader:
                zk.stop()
                return leader
            time.sleep(1)

        zk.stop()
        return None

    def stop_all(self):
        """停止所有节点"""
        for node in self.nodes.values():
            node.stop()


if __name__ == "__main__":
    mds_id = sys.argv[1] if len(sys.argv) > 1 else "mds-1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9110

    mds = MDSNode(mds_id=mds_id, port=port)
    mds.start()

    while mds.running:
        time.sleep(1)
