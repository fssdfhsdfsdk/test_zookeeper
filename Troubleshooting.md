

常见问题排查 (Troubleshooting)

-   **问题**：节点挂了，但其他节点很久才感知到。
        -   **原因**：ZK 会话超时。默认可能是 10s-30s。
        -   **解决**：初始化 `KazooClient` 时调整 `timeout` 参数，例如 `KazooClient(hosts=..., timeout=5)`。但注意网络抖动可能导致误判。
-   **问题**：`ChildrenWatch` 不触发。
        -   **原因**：`kazoo` 的 Watch 是持久的，但如果你手动使用了 `zk.get()` 并传了 `watch=True`，那是一次性的。本教程使用的 `ChildrenWatch` 是 `kazoo` 封装的持久监听，无需担心。
-   **问题**：数据乱码。
        -   **原因**：ZK 存储的是 bytes。
        -   **解决**：务必 `.encode('utf-8')` 写入，`.decode('utf-8')` 读取。