# SunsetKV

SunsetKV是一个稳定、高性能、轻量级的分布式KV存储系统。采用Raft协议作为分布式协议来保障系统的一致性与容灾
性。该系统具有以下特点
1. 主从结构：Raft协议采用主从（Leader-Follower）结构，其中一个节点被选举为主节点，负责协调整个集群的工作。其
   他节点则成为从节点，接受主节点的指令并执行任务。
2. 容错性强：Raft协议在节点挂掉或者网络分区等异常情况下，能够快速地重新选举出新的主节点，从而保证系统的稳定性
   和可用性。
3. 日志复制：为了保证多个节点之间的数据一致性，Raft协议采用日志复制机制，即每个节点都维护一份日志，并把操作逐
   个记录下来。当主节点执行完操作后，会将操作记录发送给从节点进行复制，从而保证所有节点的数据状态一致。
4. 状态转换简单：Raft协议的节点状态转换非常简单，只需要按照规则切换节点的状态即可。当节点被选举为主节点时，它
   就从“follower”状态转换成“leader”状态，而其他节点则从“follower”状态转换成“leader”状态。
5. 线性一致性：服务端对于所有客户端发送的指令的执行顺序都要符合线性时间的顺序（全局时间）。

raft.go: 实现节点的raft协议交互
server.go: 每个server绑定一个raft服务，负责处理数据的处理
client.go: 客户端