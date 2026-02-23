
# 【问题1】导入报错

```
我在云环境运行：➜  ceph_like git:(master) ✗ python osd_node.py osd-1 9100
Warning: kazoo not installed. Run: pip install kazoo: cannot import name 'SessionExpired' from 'kazoo.exceptions' (/root/.pyenv/versions/3.11.1/lib/python3.11/site-packages/kazoo/exceptions.py)
2026-02-23 17:47:21,288 - [ZKManager] - 🚀 启动 OSD: osd-1
2026-02-23 17:47:21,288 - [ZKManager] - KazooClient not available
2026-02-23 17:47:21,288 - [ZKManager] - ❌ ZK 连接失败
```

# 【问题2】MDS未选主


```
➜  ceph_like git:(master) ✗ python mds_node.py mds-2 9111
2026-02-23 17:53:46,797 - [ZKManager] - 🚀 启动 MDS: mds-2
2026-02-23 17:53:46,799 - [ZKManager] - Connecting to 127.0.0.1(127.0.0.1):2183, use_ssl: False
2026-02-23 17:53:46,808 - [ZKManager] - Zookeeper connection established, state: CONNECTED
2026-02-23 17:53:46,812 - [ZKManager] - ✅ 连接到 ZK 集群: 127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183
2026-02-23 17:53:46,826 - [ZKManager] - ✅ MDS 注册成功: mds-2
2026-02-23 17:53:46,826 - [ZKManager] - ✅ 已注册到 ZK
2026-02-23 17:53:46,828 - [ZKManager] - 🟢 OSD 加入: {'osd-3', 'osd-1', 'osd-2'}
2026-02-23 17:53:46,829 - [ZKManager] - ✅ MDS 启动完成: mds-2


➜  ceph_like git:(master) ✗ python storage_client.py --client-id client-1
2026-02-23 17:54:27,839 - [ZKManager] - Connecting to 127.0.0.1(127.0.0.1):2181, use_ssl: False
2026-02-23 17:54:27,854 - [ZKManager] - Zookeeper connection established, state: CONNECTED
2026-02-23 17:54:27,858 - [ZKManager] - ✅ 连接到 ZK 集群: 127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183
2026-02-23 17:54:27,861 - [ZKManager] - 📊 OSD 拓扑更新: 3 节点
2026-02-23 17:54:27,861 - [ZKManager] - 💾 已加载 0 个设备
2026-02-23 17:54:27,861 - [ZKManager] - ✅ 客户端连接成功

欢迎使用分布式存储客户端!
客户端ID: client-1
输入 help 查看命令

> device create mydevice 10 
2026-02-23 17:54:45,702 - [ZKManager] - ❌ 无法找到 Leader MDS
> 

```
