"""
数据迁移（Rebalancing）模块
当新 OSD 加入时，负责将数据从旧 OSD 迁移到新 OSD
"""

import os
import json
import time
import threading
import logging
import socket
from typing import Dict, List, Any, Optional, Set, Tuple
from collections import defaultdict

from zk_manager import ZKManager, NodeType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [REBALANCER] - %(message)s",
)
logger = logging.getLogger("Rebalancer")


class DataRebalancer:
    """数据迁移器"""

    def __init__(
        self,
        zk_hosts: str = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183",
        interval: int = 30,
        batch_size: int = 100,
    ):
        self.zk_hosts = zk_hosts
        self.interval = interval
        self.batch_size = batch_size

        self.zk = ZKManager(hosts=zk_hosts)
        self.running = False

        # OSD 状态
        self.osds: Dict[str, Dict[str, Any]] = {}
        self.osd_ring = {}

        # 迁移锁
        self.migration_lock = threading.Lock()

        # 迁移统计
        self.migration_stats = {
            "total_migrated": 0,
            "migrations": [],
        }

    def start(self):
        """启动迁移器"""
        logger.info("🚀 启动数据迁移器")

        if not self.zk.start():
            logger.error("❌ ZK 连接失败")
            return

        # 监听 OSD 变化
        self._watch_osds()

        # 启动迁移循环
        self.running = True
        t = threading.Thread(target=self._migration_loop, daemon=True)
        t.start()

        logger.info("✅ 数据迁移器已启动")

    def stop(self):
        """停止迁移器"""
        self.running = False
        self.zk.stop()
        logger.info("数据迁移器已停止")

    def _watch_osds(self):
        """监听 OSD 变化"""

        def on_osds_change(osds: List[Dict]):
            old_osds = set(self.osds.keys())
            new_osds = {osd["id"]: osd for osd in osds if osd.get("status") == "online"}

            added = set(new_osds.keys()) - old_osds
            removed = old_osds - set(new_osds.keys())

            with self.migration_lock:
                self.osds = new_osds

            if added:
                logger.warning(f"🟢 检测到新 OSD 加入: {added}")
                # 触发立即迁移
                self.trigger_rebalance(added)

            if removed:
                logger.error(f"🔴 检测到 OSD 下线: {removed}")

        self.zk.watch_osds(on_osds_change)

    def _migration_loop(self):
        """迁移循环"""
        while self.running:
            try:
                time.sleep(self.interval)

                # 检查是否需要迁移
                if len(self.osds) >= 2:
                    self._do_rebalance()

            except Exception as e:
                logger.error(f"迁移循环异常: {e}")

    def trigger_rebalance(self, new_osds: Set[str] = None):
        """触发重新平衡"""
        threading.Thread(target=self._do_rebalance, daemon=True).start()

    def _do_rebalance(self):
        """执行重新平衡"""
        with self.migration_lock:
            if len(self.osds) < 2:
                logger.info("OSD 数量不足，跳过迁移")
                return

            try:
                # 获取所有块信息
                blocks = self.zk.get_all_blocks()
                logger.info(f"📊 开始迁移检查，共 {len(blocks)} 个块")

                # 按主 OSD 分组
                osd_blocks = defaultdict(list)
                for block in blocks:
                    primary = block.get("primary_osd")
                    if primary:
                        osd_blocks[primary].append(block)

                # 对每个 OSD 进行迁移检查
                for osd_id, osd_blocks_list in osd_blocks.items():
                    if osd_id not in self.osds:
                        continue

                    # 计算应该迁移多少数据到新节点
                    total_blocks = len(blocks)
                    if total_blocks == 0:
                        continue

                    # 获取当前的 OSD 列表（按加入时间排序）
                    osd_list = sorted(self.osds.keys())
                    osd_idx = osd_list.index(osd_id) if osd_id in osd_list else -1

                    if osd_idx < 0:
                        continue

                    # 找到新加入的 OSD（在当前 OSD 之后的）
                    if osd_idx + 1 < len(osd_list):
                        new_osd = osd_list[osd_idx + 1]

                        # 迁移部分数据到新 OSD
                        blocks_to_migrate = osd_blocks_list[: self.batch_size]

                        for block in blocks_to_migrate:
                            self._migrate_block(block, osd_id, new_osd)

                logger.info(f"✅ 迁移检查完成")

            except Exception as e:
                logger.error(f"迁移失败: {e}")

    def _migrate_block(
        self, block: Dict[str, Any], source_osd: str, target_osd: str
    ) -> bool:
        """
        迁移单个块
        """
        block_id = block.get("block_id")
        if not block_id:
            return False

        try:
            # 1. 从源 OSD 读取数据
            source_info = self.osds.get(source_osd)
            if not source_info:
                return False

            # 获取所有版本的数据
            cmd = f"GET_VERSIONS {block_id}"
            data = self._send_to_osd(source_osd, source_info, cmd)

            if not data or data == "NULL":
                return True  # 数据不存在，跳过

            # 2. 发送到目标 OSD
            target_info = self.osds.get(target_osd)
            if not target_info:
                return False

            # 发送迁移数据
            migrate_cmd = f"MIGRATE_IN {block_id} {data} {source_osd}"
            resp = self._send_to_osd(target_osd, target_info, migrate_cmd)

            if resp == "OK":
                # 3. 更新 ZK 元数据
                new_replicas = block.get("replica_osds", [])
                if target_osd not in new_replicas:
                    new_replicas.append(target_osd)

                self.zk.update_block(
                    block_id,
                    {
                        **block,
                        "replica_osds": new_replicas,
                        "migrated_from": source_osd,
                    },
                )

                self.migration_stats["total_migrated"] += 1
                logger.info(f"🔄 迁移完成: {block_id} {source_osd} -> {target_osd}")
                return True

        except Exception as e:
            logger.error(f"迁移块失败 {block_id}: {e}")

        return False

    def _send_to_osd(
        self, osd_id: str, osd_info: Dict[str, Any], command: str
    ) -> Optional[str]:
        """发送命令到 OSD"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((osd_info["host"], osd_info["port"]))
            s.sendall(command.encode())
            resp = s.recv(65536).decode()
            s.close()
            return resp
        except Exception as e:
            logger.debug(f"OSD {osd_id} 通信失败: {e}")
            return None

    def get_stats(self) -> Dict[str, Any]:
        """获取迁移统计"""
        return {
            **self.migration_stats,
            "active_osds": len(self.osds),
            "osd_list": list(self.osds.keys()),
        }


class RebalancerService:
    """迁移服务（可作为独立进程运行）"""

    def __init__(self):
        self.rebalancer = None

    def run(self, zk_hosts: str = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"):
        """运行迁移服务"""
        self.rebalancer = DataRebalancer(zk_hosts=zk_hosts)
        self.rebalancer.start()

        # 保持运行
        import time

        while True:
            try:
                time.sleep(60)
                stats = self.rebalancer.get_stats()
                logger.info(f"迁移统计: {stats}")
            except KeyboardInterrupt:
                break

        if self.rebalancer:
            self.rebalancer.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="数据迁移服务")
    parser.add_argument(
        "--zk-hosts",
        default="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183",
    )
    parser.add_argument("--interval", type=int, default=30, help="迁移间隔(秒)")
    parser.add_argument("--batch-size", type=int, default=100, help="每批迁移数量")
    args = parser.parse_args()

    rebalancer = DataRebalancer(
        zk_hosts=args.zk_hosts,
        interval=args.interval,
        batch_size=args.batch_size,
    )

    rebalancer.start()

    import time

    try:
        while True:
            time.sleep(60)
            stats = rebalancer.get_stats()
            logger.info(f"迁移统计: {stats}")
    except KeyboardInterrupt:
        pass

    rebalancer.stop()
