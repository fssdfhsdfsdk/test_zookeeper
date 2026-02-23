

```
➜  ceph_like git:(master) ✗ python storage_client.py --client-id client-1
2026-02-23 18:24:53,854 - [ZKManager] - Connecting to 127.0.0.1(127.0.0.1):2182, use_ssl: False
2026-02-23 18:24:53,863 - [ZKManager] - Zookeeper connection established, state: CONNECTED
2026-02-23 18:24:53,867 - [ZKManager] - ✅ 连接到 ZK 集群: 127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183
2026-02-23 18:24:53,870 - [ZKManager] - 📊 OSD 拓扑更新: 3 节点
2026-02-23 18:24:53,871 - [ZKManager] - 💾 已加载 1 个设备
2026-02-23 18:24:53,871 - [ZKManager] - ✅ 客户端连接成功

欢迎使用分布式存储客户端!
客户端ID: client-1
输入 help 查看命令

> device create mydevice2 1
2026-02-23 18:25:14,289 - [ZKManager] - ✅ 设备创建: mydevice2
2026-02-23 18:25:14,289 - [ZKManager] - ✅ 设备创建成功: mydevice2, 256 blocks
> volume write mydevice 0 "data2"
2026-02-23 18:26:54,197 - [ZKManager] - ❌ 偏移超出范围: 0
> volume write mydevice2 0 "data"
2026-02-23 18:27:12,726 - [ZKManager] - ✅ 写入成功: mydevice2:0 -> osd-3
2026-02-23 18:27:12,727 - [ZKManager] - ✅ 写入成功: mydevice2:0 -> osd-1
2026-02-23 18:27:12,728 - [ZKManager] - ✅ 写入成功: mydevice2:0 -> osd-2
> volume read mydevice2 0  
2026-02-23 18:27:18,899 - [ZKManager] - ✅ 读取成功: mydevice2:0 = "data" {"client-1": 1}... (来自 osd-3)
"data" {"client-1": 1}
> write abc efd
2026-02-23 18:27:48,459 - [ZKManager] - ✅ 写入成功: abc -> osd-2
2026-02-23 18:27:48,460 - [ZKManager] - ✅ 写入成功: abc -> osd-1
2026-02-23 18:27:48,461 - [ZKManager] - ✅ 写入成功: abc -> osd-3
> read abc
2026-02-23 18:27:55,718 - [ZKManager] - ✅ 读取成功: abc = efd {"client-1": 1}... (来自 osd-2)
efd {"client-1": 1}
> 2026-02-23 18:28:08,716 - [ZKManager] - 📊 OSD 拓扑更新: 2 节点

> volume read mydevice2 0 
2026-02-23 18:28:32,166 - [ZKManager] - ✅ 读取成功: mydevice2:0 = "data" {"client-1": 1}... (来自 osd-1)
"data" {"client-1": 1}
> delete abc
2026-02-23 18:28:43,787 - [ZKManager] - 🗑️ 删除成功: abc @ osd-2
2026-02-23 18:28:43,787 - [ZKManager] - 🗑️ 删除成功: abc @ osd-1
> read abc
2026-02-23 18:29:05,126 - [ZKManager] - ⚠️ 读取失败: abc
> volume read mydevice2 0 
2026-02-23 18:29:21,958 - [ZKManager] - ✅ 读取成功: mydevice2:0 = "data" {"client-1": 1}... (来自 osd-1)
"data" {"client-1": 1}
> volume read mydevice2 0 
2026-02-23 18:29:46,006 - [ZKManager] - ✅ 从副本读取成功: mydevice2:0 (来自 osd-2)
"data" {"client-1": 1}
> volume read mydevice2 0 
2026-02-23 18:30:10,322 - [ZKManager] - ⚠️ 读取失败: mydevice2:0
> 
```