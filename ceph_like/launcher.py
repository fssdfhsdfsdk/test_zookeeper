"""
集群启动脚本
一键启动完整的分布式存储集群
"""

import os
import sys
import time
import subprocess
import signal
import threading
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [LAUNCHER] - %(message)s")
logger = logging.getLogger("Launcher")


class ClusterLauncher:
    """集群启动器"""

    def __init__(self):
        self.processes: Dict[str, subprocess.Popen] = {}
        self.zk_hosts = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

    def start_zk(self):
        """启动 ZK 集群（需要预先配置好）"""
        logger.info("检查 ZK 集群...")

    def start_osd(self, count: int = 3):
        """启动 OSD 节点"""
        logger.info(f"启动 {count} 个 OSD 节点...")

        for i in range(count):
            osd_id = f"osd-{i}"
            port = 9100 + i

            cmd = [
                sys.executable,
                "-m",
                "ceph_like.osd_node",
                osd_id,
                str(port),
            ]

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=os.path.dirname(os.path.abspath(__file__)),
            )

            self.processes[osd_id] = proc
            logger.info(f"  启动 OSD: {osd_id} @ port {port}")

        time.sleep(1)

    def start_mds(self, count: int = 2):
        """启动 MDS 节点"""
        logger.info(f"启动 {count} 个 MDS 节点...")

        for i in range(count):
            mds_id = f"mds-{i}"
            port = 9110 + i

            cmd = [
                sys.executable,
                "-m",
                "ceph_like.mds_node",
                mds_id,
                str(port),
            ]

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=os.path.dirname(os.path.abspath(__file__)),
            )

            self.processes[mds_id] = proc
            logger.info(f"  启动 MDS: {mds_id} @ port {port}")

        time.sleep(1)

    def start_rebalancer(self):
        """启动数据迁移服务"""
        logger.info("启动数据迁移服务...")

        cmd = [
            sys.executable,
            "-m",
            "ceph_like.rebalancer",
        ]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )

        self.processes["rebalancer"] = proc
        logger.info("  启动 Rebalancer")

    def start_client(self, client_id: str = "client-1"):
        """启动客户端"""
        logger.info("启动客户端...")

        cmd = [
            sys.executable,
            "-m",
            "ceph_like.storage_client",
            "--client-id",
            client_id,
        ]

        proc = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )

        self.processes[client_id] = proc

    def start_all(self, osd_count: int = 3, mds_count: int = 2):
        """启动完整集群"""
        logger.info("=" * 50)
        logger.info("启动分布式存储集群")
        logger.info("=" * 50)

        # 启动 OSD
        self.start_osd(osd_count)

        # 启动 MDS
        self.start_mds(mds_count)

        # 启动 Rebalancer
        self.start_rebalancer()

        logger.info("=" * 50)
        logger.info("集群启动完成!")
        logger.info(f"ZK: {self.zk_hosts}")
        logger.info(f"OSD: 3 节点 (9100-9102)")
        logger.info(f"MDS: 2 节点 (9110-9111)")
        logger.info("=" * 50)

    def stop_all(self):
        """停止所有进程"""
        logger.info("停止集群...")

        for name, proc in self.processes.items():
            try:
                proc.terminate()
                proc.wait(timeout=5)
                logger.info(f"  停止 {name}")
            except subprocess.TimeoutExpired:
                proc.kill()
            except Exception as e:
                logger.error(f"  停止 {name} 失败: {e}")

        self.processes.clear()
        logger.info("集群已停止")

    def monitor(self):
        """监控进程状态"""
        while True:
            time.sleep(10)

            for name, proc in list(self.processes.items()):
                if proc.poll() is not None:
                    logger.warning(f"进程退出: {name}")
                    del self.processes[name]


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="分布式存储集群启动器")
    parser.add_argument("--osd", type=int, default=3, help="OSD 节点数")
    parser.add_argument("--mds", type=int, default=2, help="MDS 节点数")
    parser.add_argument("--client", action="store_true", help="启动客户端")
    parser.add_argument("--stop", action="store_true", help="停止集群")
    args = parser.parse_args()

    launcher = ClusterLauncher()

    if args.stop:
        launcher.stop_all()
    else:
        try:
            launcher.start_all(args.osd, args.mds)

            if args.client:
                time.sleep(2)
                launcher.start_client()

            # 监控
            launcher.monitor()

        except KeyboardInterrupt:
            logger.info("收到退出信号...")
        finally:
            launcher.stop_all()


if __name__ == "__main__":
    main()
