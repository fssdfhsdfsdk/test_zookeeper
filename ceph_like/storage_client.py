"""
存储客户端
支持：
1. 通过 MDS 获取 OSD 拓扑
2. 一致性哈希分配数据
3. 多副本写入（顺时针N个节点）
4. 读取最新版本
5. 设备（Volume）管理
"""

import os
import sys
import json
import time
import socket
import threading
import logging
from typing import Dict, Any, List, Optional

from zk_manager import ZKManager, NodeType
from consistent_hash import ConsistentHashRing
from version_vector import VectorClock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [CLIENT-%(name)s] - %(message)s",
)
logger = logging.getLogger("StorageClient")


class StorageClient:
    """分布式存储客户端"""

    def __init__(
        self,
        client_id: str = "client-1",
        zk_hosts: str = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183",
        replication_count: int = 3,
        max_retry: int = 3,
        timeout: int = 5,
    ):
        self.client_id = client_id
        self.zk_hosts = zk_hosts
        self.replication_count = replication_count
        self.max_retry = max_retry
        self.timeout = timeout

        # ZK 管理器
        self.zk = ZKManager(hosts=zk_hosts)

        # 一致性哈希环
        self.hash_ring = ConsistentHashRing()

        # 本地缓存
        self.devices: Dict[str, Dict] = {}
        self.block_osd_map: Dict[str, Dict] = {}

        # 版本向量
        self.version_clocks: Dict[str, VectorClock] = {}

        # 锁
        self.lock = threading.RLock()

        # 运行状态
        self.running = False

    def connect(self) -> bool:
        """连接集群"""
        if not self.zk.start():
            logger.error("❌ ZK 连接失败")
            return False

        self.running = True

        # 监听 OSD 变化
        self._watch_osds()

        # 加载设备信息
        self._load_devices()

        # 启动定期刷新线程（防止watch丢失）
        self._start_refresh_loop()

        logger.info("✅ 客户端连接成功")
        return True

    def _watch_osds(self):
        """监听 OSD 变化"""

        def on_osds_change(osds: List[Dict]):
            # 获取新的在线 OSD
            new_online_osds = {
                osd["id"] for osd in osds if osd.get("status") == "online"
            }

            with self.lock:
                old_online_osds = {n["id"] for n in self.hash_ring.get_all_nodes()}

                # 重建哈希环
                self.hash_ring = ConsistentHashRing()
                for osd in osds:
                    if osd.get("status") == "online":
                        self.hash_ring.add_node(osd)

                # 计算变化
                added = new_online_osds - old_online_osds
                removed = old_online_osds - new_online_osds

            if added:
                logger.warning(f"🟢 OSD 加入: {added}")
            if removed:
                logger.error(f"🔴 OSD 下线: {removed}")

            logger.info(f"📊 OSD 拓扑更新: {len(self.hash_ring)} 节点")

        self.zk.watch_osds(on_osds_change)

    def _start_refresh_loop(self):
        """启动定期刷新线程，防止watch丢失"""

        def refresh_loop():
            while self.running:
                time.sleep(10)  # 每10秒刷新一次
                try:
                    self._refresh_osds()
                except Exception as e:
                    logger.debug(f"刷新 OSD 状态失败: {e}")

        t = threading.Thread(target=refresh_loop, daemon=True)
        t.start()

    def _refresh_osds(self):
        """刷新 OSD 状态"""
        osds = self.zk.get_all_osds()
        new_online_osds = {osd["id"] for osd in osds if osd.get("status") == "online"}

        with self.lock:
            old_online_osds = {n["id"] for n in self.hash_ring.get_all_nodes()}

            # 如果发现不一致，强制更新
            if new_online_osds != old_online_osds:
                logger.warning(
                    f"🔄 检测到 OSD 状态不一致，强制刷新: 旧={old_online_osds}, 新={new_online_osds}"
                )

                self.hash_ring = ConsistentHashRing()
                for osd in osds:
                    if osd.get("status") == "online":
                        self.hash_ring.add_node(osd)

                added = new_online_osds - old_online_osds
                removed = old_online_osds - new_online_osds

                if added:
                    logger.warning(f"🟢 OSD 加入: {added}")
                if removed:
                    logger.error(f"🔴 OSD 下线: {removed}")

                logger.info(f"📊 OSD 拓扑刷新: {len(self.hash_ring)} 节点")

    def _load_devices(self):
        """加载设备信息"""
        try:
            devices = self.zk.get_client_devices(self.client_id)
            for device in devices:
                self.devices[device["device_id"]] = device
                blocks = device.get("blocks", [])
                for block_id in blocks:
                    block_meta = self.zk.get_block(block_id)
                    if block_meta:
                        self.block_osd_map[block_id] = {
                            "primary": block_meta.get("primary_osd"),
                            "replicas": block_meta.get("replica_osds", []),
                        }
            logger.info(f"💾 已加载 {len(self.devices)} 个设备")
        except Exception as e:
            logger.error(f"加载设备失败: {e}")

    # ========== 设备管理 ==========

    def _wait_for_leader(self, timeout: int = 30) -> bool:
        """等待 Leader MDS 选举完成"""
        start = time.time()
        while time.time() - start < timeout:
            leader = self.zk.get_leader()
            if leader:
                logger.info(f"✅ 找到 Leader MDS: {leader}")
                return True
            time.sleep(0.5)
        return False

    def create_device(
        self, device_id: str, size_gb: int = 10, block_size: int = 4
    ) -> bool:
        """创建设备"""
        # 等待 Leader MDS
        leader = self.zk.get_leader()
        if not leader:
            logger.warning("⚠️ 等待 Leader MDS 选举...")
            if not self._wait_for_leader():
                logger.error("❌ 无法找到 Leader MDS")
                return False

        try:
            total_blocks = (size_gb * 1024) // block_size
            blocks = []

            for i in range(total_blocks):
                block_id = f"{device_id}-block-{i}"
                replicas = self.hash_ring.get_replicas(block_id, self.replication_count)

                if len(replicas) < self.replication_count:
                    logger.error("❌ OSD 数量不足")
                    return False

                osd_primary = replicas[0]
                osd_replicas = replicas[1:]

                block_meta = {
                    "block_id": block_id,
                    "device_id": device_id,
                    "index": i,
                    "primary_osd": osd_primary["id"],
                    "replica_osds": [r["id"] for r in osd_replicas],
                    "status": "allocated",
                }

                if self.zk.create_block(block_id, block_meta):
                    blocks.append(block_id)
                    self.block_osd_map[block_id] = {
                        "primary": osd_primary["id"],
                        "replicas": [r["id"] for r in osd_replicas],
                    }

            device_meta = {
                "device_id": device_id,
                "client_id": self.client_id,
                "size_gb": size_gb,
                "block_size": block_size,
                "total_blocks": total_blocks,
                "blocks": blocks,
                "status": "active",
                "created_at": time.time(),
            }

            result = self.zk.create_device(self.client_id, device_id, device_meta)
            if result:
                self.devices[device_id] = device_meta
                logger.info(f"✅ 设备创建成功: {device_id}, {total_blocks} blocks")
                return True
            return False

        except Exception as e:
            logger.error(f"创建设备失败: {e}")
            return False

    def list_devices(self) -> List[Dict]:
        return list(self.devices.values())

    def get_device_info(self, device_id: str) -> Optional[Dict]:
        return self.devices.get(device_id)

    # ========== 数据读写 ==========

    def _send_to_osd(
        self, osd_id: str, command: str, timeout: int = None
    ) -> Optional[str]:
        osd = None
        for node in self.hash_ring.get_all_nodes():
            if node["id"] == osd_id:
                osd = node
                break

        if not osd:
            return None

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout or self.timeout)
            s.connect((osd["host"], osd["port"]))
            s.sendall(command.encode())
            resp = s.recv(65536).decode()
            s.close()
            return resp
        except Exception as e:
            logger.debug(f"OSD {osd_id} 请求失败: {e}")
            return None

    def _get_or_increment_clock(self, key: str) -> VectorClock:
        if key not in self.version_clocks:
            self.version_clocks[key] = VectorClock({}, self.client_id)
        self.version_clocks[key].increment(self.client_id)
        return self.version_clocks[key]

    def write(self, key: str, value: str, device_id: str = None) -> bool:
        # 每次写入前刷新 OSD 状态
        self._refresh_osds()

        with self.lock:
            replicas = self.hash_ring.get_replicas(key, self.replication_count)
            if not replicas:
                logger.error("❌ 无可用 OSD")
                return False

            vector_clock = self._get_or_increment_clock(key)
            vc_json = vector_clock.to_json()

            success = False
            for osd in replicas:
                cmd = f"PUT {key} {value} {vc_json}"
                resp = self._send_to_osd(osd["id"], cmd)

                if resp == "OK":
                    success = True
                    logger.info(f"✅ 写入成功: {key} -> {osd['id']}")
                else:
                    logger.warning(f"⚠️ 副本写入失败: {osd['id']}: {resp}")

            return success

    def read(self, key: str) -> Optional[str]:
        # 每次读取前刷新 OSD 状态
        self._refresh_osds()

        with self.lock:
            primary = self.hash_ring.get_node(key)
            if not primary:
                logger.error("❌ 无可用 OSD")
                return None

            cmd = f"GET {key}"
            resp = self._send_to_osd(primary["id"], cmd)

            if resp and resp != "NULL":
                logger.info(
                    f"✅ 读取成功: {key} = {resp[:50]}... (来自 {primary['id']})"
                )
                return resp

            replicas = self.hash_ring.get_replicas(key, self.replication_count)
            for osd in replicas:
                if osd["id"] == primary["id"]:
                    continue
                resp = self._send_to_osd(osd["id"], cmd)
                if resp and resp != "NULL":
                    logger.info(f"✅ 从副本读取成功: {key} (来自 {osd['id']})")
                    return resp

            logger.warning(f"⚠️ 读取失败: {key}")
            return None

    def delete(self, key: str) -> bool:
        with self.lock:
            replicas = self.hash_ring.get_replicas(key, self.replication_count)
            success = False

            for osd in replicas:
                cmd = f"DELETE {key}"
                resp = self._send_to_osd(osd["id"], cmd)
                if resp == "OK":
                    success = True
                    logger.info(f"🗑️ 删除成功: {key} @ {osd['id']}")

            if key in self.version_clocks:
                del self.version_clocks[key]

            return success

    # ========== 设备块读写 ==========

    def write_to_device(self, device_id: str, offset: int, data: str) -> bool:
        device = self.devices.get(device_id)
        if not device:
            logger.error(f"❌ 设备不存在: {device_id}")
            return False

        blocks = device.get("blocks", [])
        block_size = device.get("block_size", 4)
        block_idx = offset // block_size

        if block_idx >= len(blocks):
            logger.error(f"❌ 偏移超出范围: {offset}")
            return False

        key = f"{device_id}:{block_idx}"
        return self.write(key, data, device_id)

    def read_from_device(
        self, device_id: str, offset: int, size: int = None
    ) -> Optional[str]:
        device = self.devices.get(device_id)
        if not device:
            logger.error(f"❌ 设备不存在: {device_id}")
            return None

        blocks = device.get("blocks", [])
        block_size = device.get("block_size", 4)
        block_idx = offset // block_size

        if block_idx >= len(blocks):
            logger.error(f"❌ 偏移超出范围: {offset}")
            return None

        key = f"{device_id}:{block_idx}"
        return self.read(key)

    # ========== 集群状态 ==========

    def get_cluster_status(self) -> Dict[str, Any]:
        # 读取前刷新状态
        self._refresh_osds()

        return {
            "client_id": self.client_id,
            "connected": self.zk.is_connected(),
            "osd_count": len(self.hash_ring),
            "device_count": len(self.devices),
            "leader_mds": self.zk.get_leader(),
        }

    def get_topology(self) -> Dict[str, Any]:
        return {
            "osds": self.hash_ring.get_all_nodes(),
            "devices": list(self.devices.values()),
        }

    def disconnect(self):
        self.running = False
        self.zk.stop()
        logger.info("客户端已断开")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="分布式存储客户端")
    parser.add_argument("--client-id", default="client-1", help="客户端ID")
    parser.add_argument(
        "--zk-hosts", default="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
    )
    parser.add_argument("--replication", type=int, default=3, help="副本数")
    args = parser.parse_args()

    client = StorageClient(
        client_id=args.client_id,
        zk_hosts=args.zk_hosts,
        replication_count=args.replication,
    )

    if not client.connect():
        print("连接失败")
        return

    print(f"\n欢迎使用分布式存储客户端!")
    print(f"客户端ID: {args.client_id}")
    print(f"输入 help 查看命令\n")

    while True:
        try:
            cmd = input("> ").strip()
            if not cmd:
                continue

            parts = cmd.split()
            action = parts[0].lower()

            if action in ("exit", "quit"):
                break

            elif action == "help":
                print("""
命令帮助:
  device create <id> <size_gb>   - 创建设备
  device list                      - 列出设备
  device info <id>                 - 设备详情
  
  write <key> <value>             - 写入数据
  read <key>                       - 读取数据
  delete <key>                     - 删除数据
  
  volume write <dev> <offset> <data> - 写入卷
  volume read <dev> <offset>        - 读取卷
  
  status                           - 集群状态
  topology                         - 拓扑信息
  refresh                          - 强制刷新OSD状态
  help                             - 显示帮助
                """)

            elif action == "device" and len(parts) >= 2:
                if parts[1] == "create" and len(parts) >= 4:
                    dev_id = parts[2]
                    size = int(parts[3])
                    client.create_device(dev_id, size)
                elif parts[1] == "list":
                    devices = client.list_devices()
                    for d in devices:
                        print(
                            f"  {d['device_id']}: {d['size_gb']}GB, {d['total_blocks']} blocks"
                        )
                elif parts[1] == "info" and len(parts) >= 3:
                    info = client.get_device_info(parts[2])
                    if info:
                        print(json.dumps(info, indent=2))
                    else:
                        print("设备不存在")

            elif action == "write" and len(parts) >= 3:
                key = parts[1]
                value = " ".join(parts[2:])
                client.write(key, value)

            elif action == "read" and len(parts) >= 2:
                key = parts[1]
                result = client.read(key)
                if result:
                    print(result)

            elif action == "delete" and len(parts) >= 2:
                key = parts[1]
                client.delete(key)

            elif action == "volume" and len(parts) >= 3:
                if parts[1] == "write" and len(parts) >= 5:
                    dev = parts[2]
                    offset = int(parts[3])
                    data = " ".join(parts[4:])
                    client.write_to_device(dev, offset, data)
                elif parts[1] == "read" and len(parts) >= 4:
                    dev = parts[2]
                    offset = int(parts[3])
                    result = client.read_from_device(dev, offset)
                    if result:
                        print(result)

            elif action == "status":
                status = client.get_cluster_status()
                print(json.dumps(status, indent=2))

            elif action == "refresh":
                client._refresh_osds()
                print("✅ OSD 状态已刷新")

            elif action == "topology":
                topo = client.get_topology()
                print(json.dumps(topo, indent=2))

            else:
                print("未知命令，输入 help 查看帮助")

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"错误: {e}")

    client.disconnect()


if __name__ == "__main__":
    main()
