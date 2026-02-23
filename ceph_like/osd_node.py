"""
OSD (Object Storage Daemon) 存储节点
负责实际的数据存储，支持副本写入、版本向量、心跳检测
"""

import os
import json
import time
import socket
import threading
import signal
import hashlib
import logging
import shutil
from typing import Dict, Any, Optional, List
from collections import defaultdict

from zk_manager import ZKManager, NodeType
from version_vector import VectorClock, VersionedValue, ConflictResolver

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [OSD-%(name)s] - %(message)s"
)
logger = logging.getLogger("OSD")


class OSDNode:
    """OSD 存储节点"""

    def __init__(
        self,
        osd_id: str,
        host: str = "127.0.0.1",
        port: int = 9100,
        data_dir: str = "./osd_data",
        zk_hosts: str = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183",
        replication_count: int = 3,
    ):
        self.osd_id = osd_id
        self.host = host
        self.port = port
        self.data_dir = os.path.join(data_dir, osd_id)
        self.replication_count = replication_count

        # 状态
        self.status = "online"
        self.capacity = 100 * 1024 * 1024 * 1024  # 100GB
        self.used = 0

        # ZK 管理器
        self.zk = ZKManager(hosts=zk_hosts)

        # 数据存储: {key: [VersionedValue,...]} - 保留多个版本
        self.data_store: Dict[str, List[VersionedValue]] = {}
        self.lock = threading.RLock()

        # 副本数据（从其他OSD同步来的）
        self.replica_store: Dict[str, List[VersionedValue]] = {}

        # 运行状态
        self.running = False
        self.server_socket = None

        # 信号处理
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def start(self):
        """启动 OSD 节点"""
        logger.info(f"🚀 启动 OSD: {self.osd_id}")

        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)

        # 连接 ZK
        if not self.zk.start():
            logger.error("❌ ZK 连接失败")
            return

        # 注册到 ZK
        self._register()

        # 加载本地数据
        self._load_data()

        # 启动 TCP 服务器
        self._start_server()

        # 启动心跳线程
        self._start_heartbeat()

        logger.info(f"✅ OSD 启动完成: {self.osd_id} @ {self.host}:{self.port}")

    def _register(self):
        """注册到 ZK"""
        osd_info = {
            "id": self.osd_id,
            "host": self.host,
            "port": self.port,
            "status": self.status,
            "capacity": self.capacity,
            "used": self.used,
            "weight": 1.0,
        }
        self.zk.register_osd(osd_info)
        logger.info(f"✅ 已注册到 ZK")

    def _load_data(self):
        """从磁盘加载数据"""
        data_file = os.path.join(self.data_dir, "data.json")
        if os.path.exists(data_file):
            try:
                with open(data_file, "r") as f:
                    data = json.load(f)
                    for key, versions in data.items():
                        self.data_store[key] = [
                            VersionedValue.from_dict(v) for v in versions
                        ]
                logger.info(f"💾 已加载 {len(self.data_store)} 个 key")
            except Exception as e:
                logger.error(f"加载数据失败: {e}")

    def _save_data(self):
        """保存数据到磁盘"""
        data_file = os.path.join(self.data_dir, "data.json")
        try:
            data = {}
            for key, versions in self.data_store.items():
                data[key] = [v.to_dict() for v in versions]
            with open(data_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"保存数据失败: {e}")

    def _start_server(self):
        """启动 TCP 服务器"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True

        # 启动接受连接的线程
        t = threading.Thread(target=self._accept_loop, daemon=True)
        t.start()

    def _accept_loop(self):
        """接受连接循环"""
        logger.info(f"📡 监听: {self.host}:{self.port}")
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                conn, addr = self.server_socket.accept()
                t = threading.Thread(
                    target=self._handle_connection, args=(conn, addr), daemon=True
                )
                t.start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    logger.error(f"接受连接错误: {e}")

    def _handle_connection(self, conn: socket.socket, addr):
        """处理客户端连接"""
        try:
            conn.settimeout(5.0)
            data = conn.recv(8192).decode()
            if not data:
                return

            parts = data.strip().split(" ", 2)
            cmd = parts[0].upper()

            response = ""

            if cmd == "PUT" and len(parts) >= 3:
                # PUT key value [vector_clock_json]
                key = parts[1]
                value = parts[2]
                vector_clock_json = parts[3] if len(parts) > 3 else "{}"

                result = self.put(key, value, vector_clock_json)
                response = "OK" if result else "FAIL"

            elif cmd == "GET" and len(parts) >= 2:
                # GET key
                key = parts[1]
                value = self.get(key)
                response = value if value is not None else "NULL"

            elif cmd == "GET_VERSIONS" and len(parts) >= 2:
                # GET_VERSIONS key
                key = parts[1]
                versions = self.get_versions(key)
                response = json.dumps(
                    [
                        {"value": v.value, "clock": v.vector_clock.to_dict()}
                        for v in versions
                    ]
                )

            elif cmd == "REPLICATE" and len(parts) >= 3:
                # REPLICATE key value vector_clock_json
                key = parts[1]
                value = parts[2]
                vector_clock_json = parts[3] if len(parts) > 3 else "{}"
                self.put_replica(key, value, vector_clock_json)
                response = "OK"

            elif cmd == "DELETE" and len(parts) >= 2:
                key = parts[1]
                self.delete(key)
                response = "OK"

            elif cmd == "STATUS":
                response = json.dumps(
                    {
                        "id": self.osd_id,
                        "status": self.status,
                        "keys": len(self.data_store),
                        "capacity": self.capacity,
                        "used": self.used,
                    }
                )

            elif cmd == "MIGRATE_IN":
                # 从其他OSD接收迁移数据
                # 格式: MIGRATE_IN key value vector_clock_json source_osd
                if len(parts) >= 5:
                    key = parts[1]
                    value = parts[2]
                    vector_clock_json = parts[3]
                    source_osd = parts[4]
                    self.put(key, value, vector_clock_json)
                    response = "OK"

            else:
                response = "ERROR: Unknown command"

            conn.sendall(response.encode())

        except socket.timeout:
            conn.sendall(b"ERROR: Timeout")
        except Exception as e:
            logger.error(f"处理请求错误: {e}")
            try:
                conn.sendall(f"ERROR: {e}".encode())
            except:
                pass
        finally:
            conn.close()

    def put(self, key: str, value: str, vector_clock_json: str = "{}") -> bool:
        """写入数据（带版本向量）"""
        with self.lock:
            try:
                # 解析或创建版本向量
                if vector_clock_json and vector_clock_json != "{}":
                    other_vc = VectorClock.from_json(vector_clock_json)
                    # 增加当前节点的版本
                    vector_clock = other_vc.increment(self.osd_id)
                else:
                    vector_clock = VectorClock({}, self.osd_id).increment()

                # 创建带版本的值
                versioned_value = VersionedValue(value, vector_clock)

                if key not in self.data_store:
                    self.data_store[key] = []

                # 检查是否需要解决冲突
                existing_versions = self.data_store[key]
                if existing_versions:
                    # 检查新版本是否与现有版本并发
                    if versioned_value.vector_clock.is_concurrent(
                        existing_versions[-1].vector_clock
                    ):
                        # 解决冲突：使用时间戳
                        resolved = ConflictResolver.resolve_by_timestamp(
                            existing_versions + [versioned_value]
                        )
                        self.data_store[key] = [resolved]
                        logger.info(f"🔀 冲突解决: {key} -> {resolved.value[:50]}")
                    else:
                        # 顺序写入，追加版本
                        self.data_store[key].append(versioned_value)
                else:
                    self.data_store[key].append(versioned_value)

                # 限制版本数量（保留最近10个版本）
                if len(self.data_store[key]) > 10:
                    self.data_store[key] = self.data_store[key][-10:]

                # 更新使用空间
                self.used += len(value)
                self._save_data()

                logger.info(f"📝 写入: {key} (版本: {vector_clock})")
                return True

            except Exception as e:
                logger.error(f"写入失败: {e}")
                return False

    def put_replica(self, key: str, value: str, vector_clock_json: str):
        """写入副本数据"""
        with self.lock:
            try:
                if vector_clock_json and vector_clock_json != "{}":
                    other_vc = VectorClock.from_json(vector_clock_json)
                    vector_clock = other_vc.increment(f"{self.osd_id}-replica")
                else:
                    vector_clock = VectorClock({}, self.osd_id).increment()

                versioned_value = VersionedValue(value, vector_clock)

                if key not in self.replica_store:
                    self.replica_store[key] = []

                self.replica_store[key].append(versioned_value)

                # 限制副本版本数量
                if len(self.replica_store[key]) > 5:
                    self.replica_store[key] = self.replica_store[key][-5:]

                logger.info(f"📥 收到副本: {key}")
                return True

            except Exception as e:
                logger.error(f"写入副本失败: {e}")
                return False

    def get(self, key: str) -> Optional[str]:
        """读取最新数据"""
        with self.lock:
            # 先查主数据
            if key in self.data_store and self.data_store[key]:
                versions = self.data_store[key]
                latest = ConflictResolver.resolve_by_timestamp(versions)
                return latest.value

            # 再查副本
            if key in self.replica_store and self.replica_store[key]:
                versions = self.replica_store[key]
                latest = ConflictResolver.resolve_by_timestamp(versions)
                return latest.value

            return None

    def get_versions(self, key: str) -> List[VersionedValue]:
        """获取所有版本"""
        with self.lock:
            result = []
            if key in self.data_store:
                result.extend(self.data_store[key])
            if key in self.replica_store:
                result.extend(self.replica_store[key])
            return result

    def delete(self, key: str) -> bool:
        """删除数据"""
        with self.lock:
            if key in self.data_store:
                del self.data_store[key]
                self._save_data()
                logger.info(f"🗑️ 删除: {key}")
                return True
            return False

    def get_all_keys(self) -> List[str]:
        """获取所有 key"""
        with self.lock:
            return list(self.data_store.keys())

    def get_hash_range(self) -> tuple:
        """获取当前节点负责的哈希范围（简单实现）"""
        # 使用节点ID的哈希值作为范围
        h = int(hashlib.md5(self.osd_id.encode()).hexdigest(), 16)
        start = h % (2**32)
        end = (start + 2**32 // 3) % (2**32)
        return (start, end)

    def _start_heartbeat(self):
        """启动心跳线程"""

        def heartbeat_loop():
            while self.running:
                try:
                    status = {
                        "status": self.status,
                        "used": self.used,
                        "capacity": self.capacity,
                        "keys": len(self.data_store),
                        "load": 0.5,
                    }
                    self.zk.send_heartbeat(NodeType.OSD, self.osd_id, status)
                except Exception as e:
                    logger.error(f"心跳失败: {e}")

                time.sleep(5)

        t = threading.Thread(target=heartbeat_loop, daemon=True)
        t.start()

    def _shutdown(self, signum, frame):
        """关闭 OSD"""
        logger.warning("收到关闭信号...")
        self.running = False

        # 保存数据
        self._save_data()

        # 关闭服务器
        if self.server_socket:
            self.server_socket.close()

        # 关闭 ZK
        self.zk.stop()

        logger.info("OSD 已关闭")
        os._exit(0)

    def stop(self):
        """停止 OSD"""
        self._shutdown(None, None)


if __name__ == "__main__":
    import sys

    osd_id = sys.argv[1] if len(sys.argv) > 1 else "osd-1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9100

    osd = OSDNode(osd_id=osd_id, port=port)
    osd.start()

    # 保持运行
    while osd.running:
        time.sleep(1)
