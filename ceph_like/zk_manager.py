"""
ZooKeeper 集群管理模块
封装ZK操作，提供服务注册，心跳、选举等功能
"""

import json
import logging
import time
import threading
from typing import Dict, List, Any, Optional, Callable
from enum import Enum

try:
    from kazoo.client import KazooClient
    from kazoo.client import KazooState
    from kazoo.recipe.lock import Lock
    from kazoo.exceptions import (
        NodeExistsException,
        NoNodeException,
        BadVersionException,
        ConnectionLoss,
    )

    try:
        from kazoo.exceptions import SessionExpired
    except (ImportError, AttributeError):
        SessionExpired = Exception
except (ImportError, AttributeError) as e:
    print(f"Warning: kazoo import error: {e}")
    KazooClient = None
    KazooState = None
    Lock = None
    NodeExistsException = Exception
    NoNodeException = Exception
    BadVersionException = Exception
    ConnectionLoss = Exception
    SessionExpired = Exception


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [ZKManager] - %(message)s"
)
logger = logging.getLogger("ZKManager")


class NodeType(Enum):
    OSD = "osd"
    MDS = "mds"
    CLIENT = "client"


class ZKManager:
    """ZooKeeper 集群管理器"""

    def __init__(
        self,
        hosts: str = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183",
        root_path: str = "/ceph_like",
        timeout: int = 10,
    ):
        self.hosts = hosts
        self.root_path = root_path
        self.timeout = timeout
        self.zk: Optional[KazooClient] = None
        self._running = False
        self._lock = threading.Lock()
        self._session_id = None

        # 路径常量
        self.OSD_PATH = f"{root_path}/osd"
        self.MDS_PATH = f"{root_path}/mds"
        self.BLOCKS_PATH = f"{root_path}/blocks"
        self.DEVICES_PATH = f"{root_path}/devices"
        self.LEADER_PATH = f"{root_path}/leader"
        self.LOCKS_PATH = f"{root_path}/locks"
        self.HEARTBEAT_PATH = f"{root_path}/heartbeat"

    def start(self) -> bool:
        """连接到ZK集群"""
        if KazooClient is None:
            logger.error("KazooClient not available")
            return False

        try:
            self.zk = KazooClient(hosts=self.hosts, timeout=self.timeout)
            self.zk.start()
            self._running = True
            self._session_id = self.zk.client_id

            # 确保必要的路径存在
            self.zk.ensure_path(self.OSD_PATH)
            self.zk.ensure_path(self.MDS_PATH)
            self.zk.ensure_path(self.BLOCKS_PATH)
            self.zk.ensure_path(self.DEVICES_PATH)
            self.zk.ensure_path(self.LEADER_PATH)
            self.zk.ensure_path(self.LOCKS_PATH)
            self.zk.ensure_path(self.HEARTBEAT_PATH)

            # 注册状态监听
            self.zk.add_listener(self._on_state_change)

            logger.info(f"✅ 连接到 ZK 集群: {self.hosts}")
            return True
        except Exception as e:
            logger.error(f"❌ ZK 连接失败: {e}")
            return False

    def stop(self):
        """断开ZK连接"""
        self._running = False
        if self.zk:
            try:
                self.zk.stop()
                logger.info("ZK 连接已关闭")
            except Exception as e:
                logger.error(f"关闭 ZK 连接失败: {e}")

    def _on_state_change(self, state: str):
        try:
            if state == KazooState.LOST:
                logger.error("❌ Session 丢失")
            elif state == KazooState.SUSPENDED:
                logger.warning("⚠️ 连接中断")
            elif state == KazooState.CONNECTED:
                logger.info("✅ ZK 连接已恢复")
        except:
            pass

    def is_connected(self) -> bool:
        try:
            return self.zk and self.zk.state == KazooState.CONNECTED
        except:
            return False

    # ========== 服务注册 ==========

    def register_osd(self, osd_info: Dict[str, Any]) -> bool:
        try:
            path = f"{self.OSD_PATH}/{osd_info['id']}"
            data = json.dumps(osd_info).encode()
            self.zk.create(path, data, ephemeral=True, sequence=False, makepath=True)
            logger.info(f"✅ OSD 注册成功: {osd_info['id']}")
            return True
        except Exception as e:
            logger.error(f"❌ OSD 注册失败: {e}")
            return False

    def register_mds(self, mds_info: Dict[str, Any]) -> bool:
        try:
            path = f"{self.MDS_PATH}/{mds_info['id']}"
            data = json.dumps(mds_info).encode()
            self.zk.create(path, data, ephemeral=True, makepath=True)
            logger.info(f"✅ MDS 注册成功: {mds_info['id']}")
            return True
        except Exception as e:
            logger.error(f"❌ MDS 注册失败: {e}")
            return False

    # ========== 服务发现 ==========

    def get_all_osds(self) -> List[Dict[str, Any]]:
        try:
            children = self.zk.get_children(self.OSD_PATH)
            osds = []
            for child in children:
                try:
                    data, _ = self.zk.get(f"{self.OSD_PATH}/{child}")
                    info = json.loads(data.decode())
                    osds.append(info)
                except:
                    continue
            return osds
        except Exception as e:
            logger.error(f"获取 OSD 列表失败: {e}")
            return []

    def get_all_mds(self) -> List[Dict[str, Any]]:
        try:
            children = self.zk.get_children(self.MDS_PATH)
            mds_list = []
            for child in children:
                try:
                    data, _ = self.zk.get(f"{self.MDS_PATH}/{child}")
                    info = json.loads(data.decode())
                    mds_list.append(info)
                except:
                    continue
            return mds_list
        except Exception as e:
            logger.error(f"获取 MDS 列表失败: {e}")
            return []

    def watch_osds(self, callback: Callable[[List[Dict]], None]):
        def watcher(event):
            osds = self.get_all_osds()
            callback(osds)

        try:
            self.zk.get_children(self.OSD_PATH, watch=watcher)
            osds = self.get_all_osds()
            callback(osds)
        except Exception as e:
            logger.error(f"监听 OSD 变化失败: {e}")

    # ========== Leader 选举 ==========

    def elect_leader(self, node_id: str) -> bool:
        """尝试竞选 Leader"""
        try:
            # 先检查 Leader 节点状态
            try:
                data, stat = self.zk.get(self.LEADER_PATH)
                logger.info(
                    f"Leader 节点已存在: {data.decode()}, version: {stat.version}"
                )
                return False
            except NoNodeException:
                logger.info("Leader 节点不存在，准备创建...")

            # 尝试创建 Leader 节点
            self.zk.create(self.LEADER_PATH, node_id.encode(), ephemeral=True)
            logger.info(f"👑 {node_id} 创建 Leader 节点成功!")
            return True

        except NodeExistsException:
            logger.info(f"Leader 节点已存在，{node_id} 竞选失败")
            return False
        except Exception as e:
            logger.error(f"选举过程异常: {e}")
            return False

    def watch_leader(self, callback: Callable[[Optional[str]], None]):
        """监听 Leader 变化"""

        def watcher(event):
            logger.info(f"Leader 节点变化事件: {event}")
            try:
                data, _ = self.zk.get(self.LEADER_PATH)
                leader_id = data.decode() if data else None
            except NoNodeException:
                leader_id = None
            callback(leader_id)

        try:
            # 先获取当前状态
            try:
                data, stat = self.zk.get(self.LEADER_PATH)
                logger.info(f"当前 Leader: {data.decode()}, 设置监听...")
            except NoNodeException:
                logger.info("当前无 Leader，设置监听...")

            self.zk.exists(self.LEADER_PATH, watch=watcher)
        except Exception as e:
            logger.error(f"监听 Leader 失败: {e}")

    def get_leader(self) -> Optional[str]:
        """获取当前 Leader"""
        try:
            data, _ = self.zk.get(self.LEADER_PATH)
            return data.decode() if data else None
        except NoNodeException:
            return None
        except Exception as e:
            logger.error(f"获取 Leader 失败: {e}")
            return None

    # ========== 元数据操作 ==========

    def create_block(self, block_id: str, metadata: Dict[str, Any]) -> bool:
        try:
            path = f"{self.BLOCKS_PATH}/{block_id}"
            data = json.dumps(metadata).encode()
            self.zk.create(path, data, makepath=True)
            return True
        except NodeExistsException:
            return False
        except Exception as e:
            logger.error(f"创建 Block 失败: {e}")
            return False

    def update_block(self, block_id: str, metadata: Dict[str, Any]) -> bool:
        try:
            path = f"{self.BLOCKS_PATH}/{block_id}"
            data = json.dumps(metadata).encode()
            self.zk.set(path, data)
            return True
        except Exception as e:
            logger.error(f"更新 Block 失败: {e}")
            return False

    def get_block(self, block_id: str) -> Optional[Dict]:
        try:
            path = f"{self.BLOCKS_PATH}/{block_id}"
            data, _ = self.zk.get(path)
            return json.loads(data.decode())
        except NoNodeException:
            return None
        except Exception as e:
            logger.error(f"获取 Block 失败: {e}")
            return None

    def get_all_blocks(self) -> List[Dict]:
        try:
            children = self.zk.get_children(self.BLOCKS_PATH)
            blocks = []
            for child in children:
                try:
                    data, _ = self.zk.get(f"{self.BLOCKS_PATH}/{child}")
                    info = json.loads(data.decode())
                    blocks.append(info)
                except:
                    continue
            return blocks
        except Exception as e:
            logger.error(f"获取 Blocks 失败: {e}")
            return []

    # ========== 设备管理 ==========

    def create_device(
        self, client_id: str, device_id: str, config: Dict[str, Any]
    ) -> bool:
        try:
            path = f"{self.DEVICES_PATH}/{device_id}"
            metadata = {
                "client_id": client_id,
                "device_id": device_id,
                "config": config,
                "created_at": time.time(),
            }
            data = json.dumps(metadata).encode()
            self.zk.create(path, data, makepath=True)
            logger.info(f"✅ 设备创建: {device_id}")
            return True
        except NodeExistsException:
            logger.warning(f"设备已存在: {device_id}")
            return False
        except Exception as e:
            logger.error(f"创建设备失败: {e}")
            return False

    def get_device(self, device_id: str) -> Optional[Dict]:
        try:
            path = f"{self.DEVICES_PATH}/{device_id}"
            data, _ = self.zk.get(path)
            return json.loads(data.decode())
        except NoNodeException:
            return None
        except Exception as e:
            logger.error(f"获取设备失败: {e}")
            return None

    def get_client_devices(self, client_id: str) -> List[Dict]:
        try:
            children = self.zk.get_children(self.DEVICES_PATH)
            devices = []
            for child in children:
                try:
                    data, _ = self.zk.get(f"{self.DEVICES_PATH}/{child}")
                    info = json.loads(data.decode())
                    if info.get("client_id") == client_id:
                        devices.append(info)
                except:
                    continue
            return devices
        except Exception as e:
            logger.error(f"获取设备列表失败: {e}")
            return []

    # ========== 心跳 ==========

    def send_heartbeat(self, node_type: NodeType, node_id: str, status: Dict[str, Any]):
        try:
            path = f"{self.HEARTBEAT_PATH}/{node_type.value}/{node_id}"
            status["timestamp"] = time.time()
            data = json.dumps(status).encode()
            self.zk.set(path, data)
        except NoNodeException:
            try:
                path = f"{self.HEARTBEAT_PATH}/{node_type.value}/{node_id}"
                status["timestamp"] = time.time()
                data = json.dumps(status).encode()
                self.zk.create(path, data, ephemeral=True, makepath=True)
            except:
                pass
        except Exception as e:
            logger.error(f"发送心跳失败: {e}")


if __name__ == "__main__":
    zk = ZKManager()
    if zk.start():
        zk.register_osd({"id": "osd-test", "host": "127.0.0.1", "port": 9100})
        osds = zk.get_all_osds()
        print(f"OSDs: {osds}")
        zk.stop()
