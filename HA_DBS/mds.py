import os, sys, time, json, logging, signal, threading
from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock
from kazoo.exceptions import NodeExistsError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [MDS-%(name)s] - %(levelname)s - %(message)s')
logger = logging.getLogger("MDS")

MDS_ID = sys.argv[1] if len(sys.argv) > 1 else "mds-1"
ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

# ZK 路径规划
LEADER_PATH = "/storage/leader"       # 选举 Leader
BLOCKS_PATH = "/storage/blocks"       # 元数据：BlockID -> OSDID
LOCKS_PATH = "/storage/locks"         # 分布式锁路径
NODES_PATH = "/storage/nodes"         # OSD 注册路径

class MetadataServer:
    def __init__(self, mds_id):
        self.mds_id = mds_id
        self.zk = KazooClient(hosts=ZK_HOSTS)
        self.is_leader = False
        self.running = True
        self.active_osds = {} # 内存中缓存可用的 OSD
        self.lock = threading.Lock()
        
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum, frame):
        logger.warning("MDS 收到退出信号...")
        self.running = False

    def start(self):
        self.zk.start()
        logger.info("连接到 ZK 集群")
        self.zk.ensure_path(BLOCKS_PATH)
        self.zk.ensure_path(LOCKS_PATH)
        
        # 1. 启动 OSD 监听 (感知存储节点变化)
        self.zk.ChildrenWatch(NODES_PATH, self.watch_osds)
        logger.info("👁️  开始监听 OSD 节点...")

        # 2. 尝试竞选 Leader
        self.run_leader_election()

    def watch_osds(self, children):
        """感知 OSD 加入/踢出"""
        new_osds = {}
        for child in children:
            try:
                data, _ = self.zk.get(f"{NODES_PATH}/{child}")
                info = json.loads(data.decode())
                new_osds[child] = info
            except:
                continue
        
        with self.lock:
            # 计算变化
            added = set(new_osds.keys()) - set(self.active_osds.keys())
            removed = set(self.active_osds.keys()) - set(new_osds.keys())
            self.active_osds = new_osds

        if added:
            logger.warning(f"🟢 [存储扩容] 新 OSD 加入：{added}")
        if removed:
            logger.error(f"🔴 [存储故障] OSD 下线：{removed} (触发数据迁移逻辑...)")
        
        # 只有 Leader 需要关心 OSD 列表用于分配
        if self.is_leader:
            logger.info(f"📊 当前可用存储池：{list(self.active_osds.keys())}")

    def run_leader_election(self):
        """
        工业级 Leader 选举逻辑：
        尝试创建 /storage/leader (Ephemeral)。
        成功 = 我是 Leader。
        失败 = 我是 Follower，监听该节点，等它消失再抢。
        """
        while self.running:
            try:
                # 尝试创建临时节点
                self.zk.create(LEADER_PATH, self.mds_id.encode(), ephemeral=True)
                self.become_leader()
            except NodeExistsError:
                self.become_follower()
            
            if not self.running: break
            
            # 如果是 Follower，监听 Leader 节点
            if not self.is_leader:
                try:
                    # watch 存在性，一旦 Leader 节点消失 (会话断开)，watch 触发
                    self.zk.exists(LEADER_PATH, watch=self.on_leader_change)
                    # 阻塞等待，避免空转 CPU
                    time.sleep(2) 
                except Exception as e:
                    logger.error(f"监听 Leader 异常：{e}")
                    time.sleep(1)

    def on_leader_change(self, event):
        """当 Leader 节点发生变化（通常是删除）时触发"""
        logger.warning("⚠️ 检测到 Leader 变更，重新竞选...")
        self.is_leader = False # 重置状态，循环会重新尝试 create

    def become_leader(self):
        self.is_leader = True
        logger.critical(f"👑 [选举成功] {self.mds_id} 成为 ACTIVE MDS!")
        # 这里可以加载元数据到内存等初始化操作

    def become_follower(self):
        if self.is_leader:
            logger.warning(f"📉 [选举失败] {self.mds_id} 降级为 STANDBY")
        self.is_leader = False

    def allocate_block(self, client_id):
        """
        核心业务：分配存储块
        必须满足：1. 我是 Leader 2. 拿到分布式锁
        """
        if not self.is_leader:
            return None, "Not Leader"
        
        # 使用 ZK 分布式锁，确保同一时刻只有一个线程在分配 Block ID
        lock = Lock(self.zk, f"{LOCKS_PATH}/block_allocation")
        try:
            with lock:
                # 1. 生成新 Block ID (模拟)
                blocks = self.zk.get_children(BLOCKS_PATH)
                new_block_id = f"block-{len(blocks) + 1}"
                
                # 2. 选择一个 OSD (简单轮询)
                with self.lock:
                    if not self.active_osds:
                        return None, "No Storage Available"
                    osd_id = list(self.active_osds.keys())[0]
                
                # 3. 写入元数据 (Block -> OSD 映射)
                mapping = json.dumps({"osd": osd_id, "client": client_id, "time": time.time()})
                self.zk.create(f"{BLOCKS_PATH}/{new_block_id}", mapping.encode())
                
                logger.info(f"💾 [分配成功] Block:{new_block_id} -> OSD:{osd_id}")
                return new_block_id, osd_id
        except Exception as e:
            logger.error(f"分配失败：{e}")
            return None, str(e)

    def run_simulation(self):
        """模拟接收客户端请求"""
        while self.running:
            if self.is_leader:
                # 模拟每 5 秒接收一个写入请求
                time.sleep(5)
                self.allocate_block("client-A")
            else:
                time.sleep(1)

if __name__ == "__main__":
    mds = MetadataServer(MDS_ID)
    # 选举线程
    t_elect = threading.Thread(target=mds.run_leader_election, daemon=True)
    # 业务线程
    t_work = threading.Thread(target=mds.run_simulation, daemon=True)
    
    mds.start()
    t_elect.start()
    t_work.start()
    
    while mds.running:
        time.sleep(1)
    mds.zk.stop()