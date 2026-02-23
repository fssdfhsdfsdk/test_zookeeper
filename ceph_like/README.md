# 分布式块存储系统 (Ceph-like)

一个完整的工业级分布式存储系统教程，基于 ZooKeeper 实现。

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                         ZooKeeper 集群                      │
│                      (3 节点仲裁模式)                        │
└─────────────────────────────────────────────────────────────┘
                    ↑                    ↑
                    │                    │
        ┌───────────┴────────────┐      │
        │                        │      │
   ┌────▼─────┐            ┌────▼─────┐
   │   MDS-1  │            │   MDS-2  │
   │ (Leader) │◄──────────►│ (Standby)│
   └───────────┘    选举     └───────────┘
```

```
┌─────────────────────────────────────────────────────────────┐
│                         Client                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│  │  Device-1  │  │  Device-2   │  │  Device-N   │       │
│  └─────────────┘  └─────────────┘  └─────────────┘       │
│         │                │                │                 │
│         └────────────────┴────────────────┘                 │
│                          │                                  │
│              一致性 Hash 分配                                │
└──────────────────────────┬──────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────▼────┐       ┌────▼────┐       ┌────▼────┐
   │  OSD-1  │       │  OSD-2  │       │  OSD-3  │
   │ (Primary)│◄─────►│ (Replica)│◄─────►│ (Replica)│
   └─────────┘ 副本   └─────────┘ 副本   └─────────┘
```

## 核心特性

### 1. 数据副本 (Replication)
- 写入时顺时针复制 N 个节点（如 3 个）
- 读取时读最新版本
- 故障自动转移

### 2. 版本向量 (Vector Clock)
- 解决并发写入冲突
- 支持时间戳和向量时钟两种冲突解决策略

### 3. 一致性哈希环
- 虚拟节点保证数据均匀分布
- 动态节点加入/移除
- 变化感知通知

### 4. 后台数据迁移 (Rebalancing)
- 新 OSD 加入时自动迁移
- 定期检查并平衡数据分布

### 5. MDS 主备选举
- 基于 ZooKeeper 的 Leader 选举
- 主节点故障自动切换

### 6. 心跳检测
- OSD 定期向 ZK 发送心跳
- 节点故障自动感知

## 组件说明

| 组件 | 描述 | 端口 |
|------|------|------|
| ZK | ZooKeeper 集群 | 2181-2183 |
| OSD | 存储节点 | 9100+ |
| MDS | 元数据服务器 | 9110+ |
| Client | 客户端 | - |

## 快速开始

### 1. 安装依赖

```bash
pip install kazoo
```

### 2. 启动 ZooKeeper 集群

确保 ZooKeeper 集群已启动，监听端口 2181-2183。

### 3. 启动集群（两种方式）

**方式一：直接运行（推荐）**

```bash
cd ceph_like

# 终端1: 启动 OSD 节点
python osd_node.py osd-1 9100

# 终端2: 启动更多 OSD
python osd_node.py osd-2 9101
python osd_node.py osd-3 9102

# 终端3: 启动 MDS 节点
python mds_node.py mds-1 9110
python mds_node.py mds-2 9111

# 终端4: 启动迁移服务
python rebalancer.py

# 终端5: 启动客户端
python storage_client.py --client-id client-1
```

**方式二：安装后运行**

```bash
cd ceph_like
pip install -e .

# 然后可以从任意目录运行
python -m ceph_like.osd_node osd-1 9100
python -m ceph_like.mds_node mds-1 9110
python -m ceph_like.storage_client --client-id client-1
```

## 使用指南

### 客户端命令

```bash
# 创建设备
device create mydevice 10    # 创建 10GB 设备

# 列出设备
device list

# 写入数据
write key1 value1

# 读取数据
read key1

# 删除数据
delete key1

# 卷操作
volume write mydevice 0 "data"   # 写入卷
volume read mydevice 0           # 读取卷

# 集群状态
status
topology

# 帮助
help
```

## 工作原理

### 写入流程

1. Client 计算 key 的哈希值
2. 通过一致性哈希找到 N 个副本节点
3. 并行写入所有副本（携带版本向量）
4. OSD 接收数据，检查版本冲突
5. 如有冲突，按时间戳解决

### 读取流程

1. Client 计算 key 的哈希值
2. 找到主节点
3. 读取最新版本
4. 如主节点失败，尝试副本

### 设备创建流程

1. Client 向 MDS 发起请求
2. MDS (Leader) 分配块
3. 使用一致性哈希为每个块选择主 OSD 和副本 OSD
4. 块元数据写入 ZooKeeper

## 扩展

### 添加新 OSD

启动新的 OSD 节点后，系统会自动：
1. 感知新节点加入
2. 更新一致性哈希环
3. Rebalancer 开始迁移数据

### 节点故障

当 OSD 节点故障时：
1. 心跳超时，ZK 标记节点离线
2. Client 感知变化，更新哈希环
3. 写入自动路由到健康节点

## 文件结构

```
ceph_like/
├── config.ini          # 配置文件
├── setup.py           # 安装配置
├── zk_manager.py      # ZK 管理模块
├── osd_node.py        # OSD 存储节点
├── mds_node.py        # MDS 元数据服务器
├── storage_client.py  # 客户端
├── consistent_hash.py # 一致性哈希
├── version_vector.py # 版本向量
├── rebalancer.py     # 数据迁移
└── launcher.py       # 启动器
```

## 注意事项

1. 确保 ZooKeeper 集群正常运行
2. 首次使用前创建设备
3. 多副本需要足够的 OSD 节点
4. 定期检查数据迁移状态
