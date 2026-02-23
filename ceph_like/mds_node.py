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
        self.devices: Dict[str, Dict] = {}
        self.device_blocks: Dict[str, List[str]] = {}

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
            old_osds = set(self.active_osds.keys())
            new_osds = {osd["id"]: osd for osd in osds}

            with self.block_lock:
                self.active_osds = new_osds
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
                        self.zk.watch_leader(self._on_leader_change)
                    else:
                        self._become_follower()
                        self.zk.watch_leader(self._on_leader_change)

                except Exception as e:
                    logger.error(f"选举异常: {e}")

                time.sleep(2)

        # 立即尝试一次选举
        try:
            if self.zk.elect_leader(self.mds_id):
                self._become_leader()
                logger.info("✅ 立即成为 Leader")
            else:
                self._become_follower()
                self.zk.watch_leader(self._on_leader_change)
                logger.info("⏳ 等待 Leader 选举...")
        except Exception as e:
            logger.error(f"初始选举异常: {e}")

        # 启动后台选举循环
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
                    self._sync_metadata()
                time.sleep(10)

        t = threading.Thread(target=business_loop, daemon=True)
        t.start()

    def _sync_metadata(self):
        """同步元数据到 ZK"""
        pass

    def create_device(
        self, client_id: str, device_id: str, size_gb: int, block_size: int = 4
    ) -> bool:
        """创建设备"""
        if not self.is_leader:
            logger.error("❌ 只有 Leader 才能创建设备")
            return False

        try:
            total_blocks = (size_gb * 1024) // block_size
            blocks = []

            for i in range(total_blocks):
                block_id = f"{device_id}-block-{i}"
                replicas = self.osd_ring.get_replicas(block_id, 3)

                if len(replicas) < 3:
                    logger.error("❌ OSD 数量不足")
                    return False

                osd_primary = replicas[0]["id"]
                osd_replicas = [replicas[1]["id"], replicas[2]["id"]]

                block_meta = {
                    "block_id": block_id,
                    "device_id": device_id,
                    "index": i,
                    "primary_osd": osd_primary,
                    "replica_osds": osd_replicas,
                    "status": "allocated",
                }

                if self.zk.create_block(block_id, block_meta):
                    blocks.append(block_id)

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

            if self.zk.create_device(client_id, device_id, device_meta):
                self.devices[device_id] = device_meta
                self.device_blocks[device_id] = blocks
                logger.info(f"✅ 设备创建: {device_id}, {total_blocks} blocks")
                return True
            return False

        except Exception as e:
            logger.error(f"创建设备异常: {e}")
            return False

    def get_cluster_status(self) -> Dict[str, Any]:
        return {
            "mds_id": self.mds_id,
            "is_leader": self.is_leader,
            "active_osds": len(self.active_osds),
        }

    def _shutdown(self, signum, frame):
        logger.warning("收到关闭信号...")
        self.running = False
        self.zk.stop()
        logger.info("MDS 已关闭")
        os._exit(0)

    def stop(self):
        self._shutdown(None, None)


if __name__ == "__main__":
    mds_id = sys.argv[1] if len(sys.argv) > 1 else "mds-1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9110

    mds = MDSNode(mds_id=mds_id, port=port)
    mds.start()

    while mds.running:
        time.sleep(1)
