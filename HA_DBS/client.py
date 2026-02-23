import hashlib, bisect, socket, json, logging, time, threading
from kazoo.client import KazooClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [CLIENT] - %(message)s')
logger = logging.getLogger("Client")

ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
NODES_PATH = "/storage/nodes"
VIRTUAL_NODES = 10  # 虚拟节点数，保证分布均匀

class ConsistentHashRing:
    def __init__(self, nodes=None):
        self.ring = []
        self.ring_keys = []
        self.nodes = {} # {osd_id: {host, port}}
        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node_info):
        node_id = node_info['id']
        self.nodes[node_id] = node_info
        # 添加虚拟节点
        for i in range(VIRTUAL_NODES):
            v_key = f"{node_id}-virtual-{i}"
            h = self._hash(v_key)
            self.ring.append(h)
            self.ring_keys.append((h, node_id))
        self.ring_keys.sort(key=lambda x: x[0])

    def remove_node(self, node_id):
        if node_id not in self.nodes: return
        del self.nodes[node_id]
        # 重建环 (简单实现，生产环境可用更高效的数据结构)
        self.ring = []
        self.ring_keys = []
        for nid, info in self.nodes.items():
            for i in range(VIRTUAL_NODES):
                v_key = f"{nid}-virtual-{i}"
                h = self._hash(v_key)
                self.ring.append(h)
                self.ring_keys.append((h, nid))
        self.ring_keys.sort(key=lambda x: x[0])

    def get_node(self, key):
        if not self.ring_keys: return None
        h = self._hash(key)
        # 二分查找顺时针第一个节点
        idx = bisect.bisect(self.ring, h)
        if idx == len(self.ring):
            idx = 0
        # 返回真实节点信息
        node_id = self.ring_keys[idx][1]
        return self.nodes.get(node_id)

class StorageClient:
    def __init__(self):
        self.zk = KazooClient(hosts=ZK_HOSTS)
        self.hash_ring = ConsistentHashRing()
        self.lock = threading.Lock()
        
    def connect(self):
        self.zk.start()
        # 监听 OSD 列表变化
        self.zk.ChildrenWatch(NODES_PATH, self.update_ring)
        logger.info("🔗 客户端已连接 ZK，开始监听 OSD 变化...")

    def update_ring(self, children):
        """当 ZK 中 OSD 节点变化时，重建哈希环"""
        new_nodes = []
        for child in children:
            try:
                data, _ = self.zk.get(f"{NODES_PATH}/{child}")
                info = json.loads(data.decode())
                new_nodes.append(info)
            except:
                continue
        
        with self.lock:
            # 简单的全量重建逻辑
            current_ids = set(self.hash_ring.nodes.keys())
            new_ids = set(n['id'] for n in new_nodes)
            
            # 移除下线的
            for removed in current_ids - new_ids:
                self.hash_ring.remove_node(removed)
                logger.warning(f"🔴 OSD 下线，哈希环已更新：{removed}")
            
            # 添加新加入的
            for node in new_nodes:
                if node['id'] not in current_ids:
                    self.hash_ring.add_node(node)
                    logger.info(f"🟢 OSD 上线，哈希环已更新：{node['id']}")
        
        logger.info(f"🔄 当前哈希环节点数：{len(self.hash_ring.nodes)}")

    def _send_command(self, host, port, command):
        """发送 TCP 命令到 OSD"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((host, port))
            s.sendall(command.encode())
            resp = s.recv(1024).decode()
            s.close()
            return resp
        except Exception as e:
            raise Exception(f"连接失败 {host}:{port} - {e}")

    def put(self, key, value):
        """写入数据 (带故障转移)"""
        with self.lock:
            # 1. 计算目标节点
            target = self.hash_ring.get_node(key)
            if not target:
                logger.error("❌ 无可用存储节点")
                return False
            
            # 2. 尝试写入，如果失败，尝试环上的下一个节点 (副本逻辑)
            # 这里简化为尝试所有节点直到成功
            nodes_list = list(self.hash_ring.nodes.values())
            if not nodes_list: return False
            
            # 找到目标在列表中的索引，以便故障时找下一个
            try:
                start_idx = [n['id'] for n in nodes_list].index(target['id'])
            except:
                start_idx = 0
                
            for i in range(len(nodes_list)):
                idx = (start_idx + i) % len(nodes_list)
                node = nodes_list[idx]
                try:
                    resp = self._send_command(node['host'], node['port'], f"PUT {key} {value}")
                    if resp == "OK":
                        logger.info(f"✅ 写入成功：{key} -> {node['id']}")
                        return True
                    else:
                        logger.warning(f"⚠️ 写入失败 {node['id']}: {resp}")
                except Exception as e:
                    logger.error(f"⚠️ 节点 {node['id']} 不可达，尝试下一个...")
                    continue
            return False

    def get(self, key):
        """读取数据"""
        with self.lock:
            target = self.hash_ring.get_node(key)
            if not target: return None
            try:
                resp = self._send_command(target['host'], target['port'], f"GET {key}")
                if resp != "NULL":
                    logger.info(f"✅ 读取成功：{key} = {resp} (来自 {target['id']})")
                    return resp
                else:
                    logger.warning(f"⚠️ 数据不存在：{key}")
                    return None
            except Exception as e:
                logger.error(f"❌ 读取失败：{e}")
                return None

if __name__ == "__main__":
    client = StorageClient()
    client.connect()
    
    # 等待环初始化
    time.sleep(2)
    
    # 模拟业务
    import sys
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "write":
            key = sys.argv[2]
            val = sys.argv[3]
            client.put(key, val)
        elif cmd == "read":
            key = sys.argv[2]
            client.get(key)
    else:
        # 交互模式
        logger.info("进入交互模式 (输入: put key value / get key / exit)")
        while True:
            try:
                inp = input("> ").strip()
                if not inp: continue
                parts = inp.split()
                if parts[0] == 'exit': break
                elif parts[0] == 'put' and len(parts) >= 3:
                    client.put(parts[1], parts[2])
                elif parts[0] == 'get' and len(parts) >= 2:
                    client.get(parts[1])
            except KeyboardInterrupt:
                break
    client.zk.stop()