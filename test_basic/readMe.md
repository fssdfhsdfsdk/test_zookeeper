

【1】现代、真实、实战教程：zookeeper集群3节点 + 本地服务3节点，能支持心跳检测、加入节点、踢出不可用节点。python


```
pip install kazoo
docker-compose up -d
```

启动 3 个服务节点

```
python worker.py 8001
python worker.py 8002
python worker.py 8003
```