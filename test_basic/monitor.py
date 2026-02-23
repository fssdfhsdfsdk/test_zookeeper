import logging
from kazoo.client import KazooClient
from kazoo.protocol.states import KeeperState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Monitor")

ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
WATCH_PATH = "/app/workers"

# 记录当前已知的活跃节点
active_nodes = set()

def watch_children(children):
    global active_nodes
    current_nodes = set(children)
    
    # 计算差异
    joined_nodes = current_nodes - active_nodes
    kicked_nodes = active_nodes - current_nodes
    
    # 更新状态
    active_nodes = current_nodes

    # 处理加入
    for node in joined_nodes:
        logger.warning(f"🟢 [节点加入] 检测到新服务：{node}")
        # 这里可以触发负载均衡更新、发送钉钉通知等
        # update_load_balancer(node, action='add')

    # 处理踢出 (下线)
    for node in kicked_nodes:
        logger.error(f"🔴 [节点踢出] 服务不可用，已移除：{node}")
        # 这里触发故障转移、告警
        # update_load_balancer(node, action='remove')
        # send_alert(f"Service {node} is down!")

    if not joined_nodes and not kicked_nodes and children:
        logger.info(f"🔵 [心跳正常] 当前活跃节点数：{len(children)}")
    elif not children:
        logger.warning("⚠️ [警告] 集群中无活跃节点！")

def main():
    logger.info("启动 Monitor 监控服务...")
    zk = KazooClient(hosts=ZK_HOSTS)
    
    # 监听连接状态
    @zk.add_listener
    def watch_connection(state):
        if state == KeeperState.CONNECTED:
            logger.info("Monitor 已连接到 ZK 集群")
        elif state == KeeperState.EXPIRED:
            logger.error("Monitor 与 ZK 会话过期，需要重连")

    zk.start()
    zk.ensure_path(WATCH_PATH)

    # 注册监听器
    # ChildrenWatch 会在子节点列表变化时自动触发 watch_children 函数
    zk.ChildrenWatch(WATCH_PATH, watch_children)

    logger.info("Monitor 正在监听中... (Ctrl+C 停止)")
    
    try:
        # 阻塞主线程
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Monitor 停止")
    finally:
        zk.stop()

if __name__ == "__main__":
    import time
    main()