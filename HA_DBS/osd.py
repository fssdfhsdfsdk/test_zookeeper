import os, sys, time, json, logging, signal, socket, threading
from kazoo.client import KazooClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [OSD-%(name)s] - %(message)s')
logger = logging.getLogger("OSD")

OSD_ID = sys.argv[1] if len(sys.argv) > 1 else "osd-1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 9001
ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
ZK_PATH = f"/storage/nodes/{OSD_ID}"
DISK_FILE = f"disk_{OSD_ID}.db"

zk = None
running = True
disk_lock = threading.Lock()
# 内存数据库 (启动时从文件加载)
data_store = {}

def load_disk():
    global data_store
    if os.path.exists(DISK_FILE):
        try:
            with open(DISK_FILE, 'r') as f:
                data_store = json.load(f)
            logger.info(f"💾 磁盘数据已加载：{len(data_store)} 条记录")
        except:
            data_store = {}
    else:
        data_store = {}

def save_disk():
    # 模拟落盘延迟
    time.sleep(0.01) 
    with open(DISK_FILE, 'w') as f:
        json.dump(data_store, f)

def handle_client(conn, addr):
    try:
        data = conn.recv(1024).decode()
        if not data: return
        
        parts = data.strip().split(' ', 2)
        cmd = parts[0].upper()
        
        response = "OK"
        
        if cmd == "PUT" and len(parts) == 3:
            key, value = parts[1], parts[2]
            with disk_lock:
                data_store[key] = value
                save_disk()
            logger.info(f"📝 写入：{key}={value}")
        elif cmd == "GET" and len(parts) == 2:
            key = parts[1]
            with disk_lock:
                value = data_store.get(key, "NULL")
            response = value
            logger.info(f"📖 读取：{key} -> {value}")
        else:
            response = "ERROR COMMAND"
            
        conn.sendall(response.encode())
    except Exception as e:
        logger.error(f"处理请求错误：{e}")
    finally:
        conn.close()

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('127.0.0.1', PORT))
    server.listen(5)
    logger.info(f"🚀 OSD 服务启动，监听 127.0.0.1:{PORT}")
    
    while running:
        try:
            server.settimeout(1.0)
            conn, addr = server.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr))
            t.daemon = True
            t.start()
        except socket.timeout:
            continue
        except:
            break
    server.close()

def register_zk():
    global zk
    zk = KazooClient(hosts=ZK_HOSTS)
    zk.start()
    zk.ensure_path("/storage/nodes")
    
    info = json.dumps({
        "id": OSD_ID,
        "host": "127.0.0.1",
        "port": PORT,
        "status": "online"
    }).encode()
    
    try:
        zk.create(ZK_PATH, info, ephemeral=True)
        logger.info(f"✅ 已注册到 ZK: {ZK_PATH}")
    except Exception as e:
        logger.error(f"ZK 注册失败：{e}")

def shutdown(signum, frame):
    global running
    logger.warning("收到停止信号...")
    running = False

if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    load_disk()
    register_zk()
    start_server()
    
    if zk: zk.stop()
    logger.info("OSD 已关闭")