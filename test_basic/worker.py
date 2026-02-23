import os
import sys
import time
import json
import logging
import signal
import threading
from datetime import datetime
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

# --- 配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(name)s] - %(levelname)s - %(message)s')
logger = logging.getLogger("ClusterWorker")

# 从命令行获取端口，默认 8001
PORT = sys.argv[1] if len(sys.argv) > 1 else "8001"
HOSTNAME = os.uname()[1]
WORKER_ID = f"worker_{PORT}"
WORKER_PATH = f"/app/workers/{WORKER_ID}"
ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

class ClusterAwareWorker:
    def __init__(self, port, zk_hosts):
        self.port = port
        self.zk_hosts = zk_hosts
        self.zk = None
        self.running = True
        self.peers = {}  # 存储集群拓扑：{worker_id: info_dict}
        self.lock = threading.Lock() # 线程锁，保护 peers 字典
        self.my_path = f"/app/workers/worker_{port}"
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logger.warning("收到退出信号，正在关闭...")
        self.running = False

    def connect(self):
        """连接 ZK 集群"""
        self.zk = KazooClient(hosts=self.zk_hosts)
        self.zk.start()
        logger.info("✅ 已连接到 ZooKeeper 集群")
        self.zk.ensure_path("/app/workers")

    def register_self(self):
        """注册自身信息到 ZK (临时节点)"""
        # 节点数据包含更多信息，方便同伴识别
        node_data = json.dumps({
            "id": WORKER_ID,
            "host": HOSTNAME,
            "port": self.port,
            "start_time": datetime.now().isoformat(),
            "pid": os.getpid()
        }).encode('utf-8')
        
        try:
            self.zk.create(self.my_path, node_data, ephemeral=True)
            logger.info(f"🆔 自身注册成功：{WORKER_ID}")
        except NodeExistsError:
            logger.error(f"❌ 节点已存在，可能是端口冲突或上次未正常退出：{WORKER_ID}")
            # 生产环境这里应该尝试删除旧节点或退出
            self.running = False

    def update_cluster_view(self, children):
        """
        核心逻辑：当 ZK 节点列表变化时，更新本地视图
        此函数由 ZK 监听线程回调，注意线程安全
        """
        current_peers = {}
        my_info = None

        # 1. 获取所有节点的详细数据
        for child in children:
            path = f"/app/workers/{child}"
            try:
                data, stat = self.zk.get(path)
                info = json.loads(data.decode('utf-8'))
                current_peers[child] = info
                
                # 识别自己
                if child == WORKER_ID:
                    my_info = info
            except Exception as e:
                logger.warning(f"读取节点 {child} 信息失败：{e}")

        # 2. 线程安全更新内存中的拓扑表
        with self.lock:
            old_peers_count = len(self.peers)
            self.peers = current_peers
        
        # 3. 感知变化逻辑 (Join / Kick)
        # 注意：由于 children 是全量列表，我们需要对比上一刻的状态才能知道具体是谁变了
        # 为简化教程，这里直接打印当前全量视图。生产环境建议保存 self.previous_peers 进行 diff
        
        if my_info:
            # 计算同伴数量 (总数 - 自己)
            peer_count = len(self.peers) - 1
            logger.info(f"🌐 集群视图更新 | 总节点：{len(self.peers)} | 同伴数：{peer_count}")
            
            # 打印同伴列表
            peer_ids = [pid for pid in self.peers.keys() if pid != WORKER_ID]
            if peer_ids:
                logger.info(f"   👉 在线同伴：{', '.join(peer_ids)}")
            else:
                logger.info(f"   👉 当前无其他同伴 (单机模式)")
        else:
            logger.error("⚠️ 无法在集群列表中找到自己，可能节点已失效！")

    def start_watch(self):
        """启动监听"""
        # ChildrenWatch 会在注册时立即触发一次回调（返回当前列表），之后每次列表变化都会触发
        self.zk.ChildrenWatch("/app/workers", self.update_cluster_view)
        logger.info("👁️  已开启集群成员监听")

    def run(self):
        """主运行循环"""
        self.connect()
        self.register_self()
        if not self.running: return

        self.start_watch()
        
        logger.info("🚀 服务运行中，正在感知集群变化...")
        
        try:
            while self.running:
                # 模拟业务逻辑
                # 在这里，你可以使用 self.peers 来构建连接池、分发任务等
                time.sleep(5)
                
                # 演示：定期打印拓扑摘要
                with self.lock:
                    count = len(self.peers)
                # logger.debug(f"心跳检查... 当前感知节点数：{count}")
        except Exception as e:
            logger.error(f"运行异常：{e}")
        finally:
            if self.zk:
                self.zk.stop()
                self.zk.close()
            logger.info("🛑 服务已停止")

if __name__ == "__main__":
    worker = ClusterAwareWorker(PORT, ZK_HOSTS)
    worker.run()