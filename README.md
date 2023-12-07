# 6.824 Raft

之前写过一次6.824的Raft，很多地方在coding的时候没有设计好，这里重新实现一次。Raft确实是一个比较复杂繁琐的流程，相对于数据结构中的各种算法简洁内聚的特点，Raft需要在一个随机、不稳定的环境中建立秩序。

实现Raft并通过所有的测试是很有难度的，需要花费大量的时间和精力，这里也简单总结一下我遇到的问题和一些心得。

当然，以下我所提到的技巧或许会给你一些帮助，让你通过几个Corner Case，但是所有的技巧都远不及你实现Raft的细心、耐心和决心。

## Leader Election

Leader Election的逻辑其实是最简单的，但这却是我们实现Raft的第一步，这里困难的地方在于做好前置工作，以及了解如何处理RPC流程。

前置工作：
- 理论：先搞懂理论再写代码，对于Leader Election为什么能够做到安全地选出一个Leader要有清楚的认知；
- 日志：一定要有把每一处日志都加上的耐心，最好对日志做好分等级，可以通过指定环境变量来控制打印日志的多少，这个控制不要Hard code到代码中，因为调试的时候会频繁切换日志等级，环境变量会更加方便；

处理RPC：
- 网络认知：6.824给我们提供了一个模拟的网络环境，这个网络有延时、乱序的特点，所以不要假设RPC会快速返回，也不要假设RPC请求到达的顺序，Client调用Server可以保证的是你的RPC请求一定会有返回，至于是1ms、1s、10s返回，对此不要有任何预期；
- 锁处理：RPC调用期间一定不能加锁，否则必然导致死锁，如何既能做到RPC期间不加锁，又能保证流程的并发安全是一个很重要的技巧，一般是通过Term来实现，即RPC前记录当前Term1，RPC之后看Raft的Term2是否与Term1相同，如果相同说明Raft状态没有转变，可以执行后续流程，之后所有涉及RPC的地方都是如此处理；

## Log Replication

Log Replication是整个Raft的核心，也是最复杂的地方，能不能实现这个是一个坎，如果可以正确实现Log Replication，那么很大概率可以实现后续的流程。

这一阶段没有捷径可走，肯定要踩很多坑才能最终实现，这里也有一些小建议：
- Leader Election：为了保证Raft的安全性，Leader Election有了补充条件，确保实现了这部分逻辑；
- Leader Append Entries：Leader发送Logs到各个Follower的过程中，Leader本身Logs也可能发生改变，与保留当前Term的思想一样，可以先把Leader的Logs复制一份，本次Append操作在这个Logs的基础上执行；
- Follower Append Entries：Leader的日志一定是最新的，Follower收到请求之后要通过Leader的Logs更新自己的Logs，有些时候需要Truncate日志，Truncate的时候要小心，不要把后面不该Truncate的日志去掉了，Leader的日志的确是最新的，但是这一次请求下发的日志可不一定是最新的（网络是无序的）；
- 测试失败的时候保留Raft日志：这个可能要在你失败的测试代码那里加上，完整的Raft日志可以帮助我们更好的Debug；

## Log Persistence

Log Persistence相对比较简单，如果你前面流程正确实现了，就只需要在适当的地方加上Persist调用即可。

## Log Compaction

Log Compaction个人认为其实并不复杂，但是有几个地方可能会困扰我们很久，这些都不是Raft中重点讲到的地方，却是正确实现Raft必不可少的。主要有以下总结：

- Logical Index：Log Compaction涉及到日志的裁剪，裁剪之后会导致Raft Log的Logical Index和Physical Index对不上，请确保实现支持Logical Index和Physical Index转换的逻辑，否则后续的流程将很难进行下去，个人建议先实现Logical Index，通过之前的测试后再进行Log Compaction代码编写；
- Snapshot：无论是Leader收到生成Snapshot的请求还是Follower收到InstallSnapshot的请求，都不能无脑替换，要判断是否比当前存在的Snapshot更新，Follow更新Snapshot之后要丢弃Snapshot范围内的日志；
- Apply：Snapshot生成之后要Apply一次，这个时候不要忘了Apply这个Snapshot之后的所有已经Commit的日志，这个往往很难想到，其实很好理解，Raft的日志都是按照顺序提交的，Apply Snapshot会打乱这个顺序，所以要将Snapshot后续Commit状态的日志再Apply一次，保证这个顺序；

## Raft KV(no snapshot)

至此，我们已经拥有了一个完善的Raft库，将基于此构建我们的KV服务。这一部分需要关注的点如下：

- Clerk：client的Get/PutAppend操作一定要在成功之后返回，确保操作成功，也就是说RPC可能失败，但是需要一直重试，直到成功；
- Sequence Number：每一个操作都要携带一个唯一的Seq，每一个Client的Seq都是单调递增的，这是为了后续可以处理重复操作，这里的主要手段是记录每个Client上一次操作的Seq，如果再次遇到这个Seq的Msg，则需要忽略，只有当NewSeq=LatestSeq+1时才能安全执行操作；