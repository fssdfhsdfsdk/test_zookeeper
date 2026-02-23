


```
➜  test_basic git:(master) python worker.py 8001
2026-02-23 16:21:02,442 - [kazoo.client] - INFO - Connecting to 127.0.0.1(127.0.0.1):2182, use_ssl: False
2026-02-23 16:21:02,474 - [kazoo.client] - INFO - Zookeeper connection established, state: CONNECTED
2026-02-23 16:21:02,474 - [ClusterWorker] - INFO - ✅ 已连接到 ZooKeeper 集群
2026-02-23 16:21:02,525 - [ClusterWorker] - INFO - 🆔 自身注册成功：worker_8001
2026-02-23 16:21:02,528 - [ClusterWorker] - INFO - 🌐 集群视图更新 | 总节点：1 | 同伴数：0
2026-02-23 16:21:02,528 - [ClusterWorker] - INFO -    👉 当前无其他同伴 (单机模式)
2026-02-23 16:21:02,528 - [ClusterWorker] - INFO - 👁️  已开启集群成员监听
2026-02-23 16:21:02,528 - [ClusterWorker] - INFO - 🚀 服务运行中，正在感知集群变化...
2026-02-23 16:21:23,160 - [ClusterWorker] - INFO - 🌐 集群视图更新 | 总节点：2 | 同伴数：1
2026-02-23 16:21:23,160 - [ClusterWorker] - INFO -    👉 在线同伴：worker_8002
2026-02-23 16:22:01,748 - [ClusterWorker] - INFO - 🌐 集群视图更新 | 总节点：3 | 同伴数：2
2026-02-23 16:22:01,748 - [ClusterWorker] - INFO -    👉 在线同伴：worker_8002, worker_8003
2026-02-23 16:22:21,769 - [ClusterWorker] - INFO - 🌐 集群视图更新 | 总节点：2 | 同伴数：1
2026-02-23 16:22:21,770 - [ClusterWorker] - INFO -    👉 在线同伴：worker_8002
```