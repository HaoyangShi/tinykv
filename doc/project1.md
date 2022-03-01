# project1文档

主要实现两个任务：

1. 实现StandAlone_storage的基本接口
2. 在raw_api.go中完成增删改查的方法

### StandAlone_storage实现

在 /kv/storage/standalone_storage/standalone_storage.go 中仿照 /kv/storage/raft_storage/raft_server.go 实现各个函数，封装成一个standalone_storage

**StandAloneStorage定义**：在Raft_storage中，需要KVDB记录基本的键值数据，RaftDB记录协议所需的一些元信息。但是在单机模式下，只需存储数据即可，所以只需要一个KVDB。为了代码的系统性，仍使用Engines来封装KVDB，其中的RaftDB为空即可。

集群信息在StandAlone模式下主要是标志是否为为Raft集群的的参数“Raft”有用，其他Config参数都是为Raft协议服务，所以是否记录config都可以，无关紧要

**NewStandAloneStorage**：创建一个StandAloneStorage，主要就是在DBPath上创建KVDB的Badger。借鉴NewRaftStorage，定义KVDB存储路径，创建对应路径，调用CreateDB创建数据库，创建新的引擎。

**Start**:理论上只需要启动创建的KVDB。但是在CreateDB函数中，创建时就已经启动了，所以在此函数中什么都不需要做

**Stop**：调用Close函数关闭KVDB

**Reader**：首先分析Raft中Reader如何创建。在Raft中先向其他节点沟通，确认可以进行读和（获取分区？），最后在对应的分区上创建RegionReader。RegionReader是Raft场景下的StorageReader的实现，因为在TinyKV中对数据进行分区，所以它需要在Region上读所需要的数据。

对于StandAlone情景下，只有一个主机，所有数据都在当前主机上，所以不需要考虑和其他节点沟通和分区的情况，直接创建一个类似RegionReader返回即可。将类似RegionReader用于StandAlone场景下读的StorageReader定义为StandAloneReader，仿照RegionReader，将其所有与分区相关的内容删除即可

**Write**：在Raft的Write中，将要写入的内容组织为消息，发送给各个节点，在节点处理写入消息时在将内容更新到数据库。并且把当前节点也视作消息接收方，同样在收到消息的时候再写入数据库。

但是StandAlone场景下只需要直接写入数据库即可。通过project2的介绍文档提示，使用在Engine中封装好的WriteBatch批量写入数据库。

注：注意到有Engine中封装的WriteBatch，也可以直接Engine写入，两者的区别为WriteBatch是批量写入，多次输入，一次落盘；而直接写入是直接落盘。

### Raw_api增删改查实现

封装函数，调用Storage(StandAlone和Raft共通)中Reader和Write方法，实现对数据的增删改查

**RawGet**：创建Storage.Reader，调用GetCF方法获取结果

**RawPut**：调用Storage中Write方法。注意，所有的Key、Value都是以字节数组的方式存储，一个[]byte就是一个值，而不是一串值的数组。每一个RawPutReq中包括一个键值对，一个Modify中也记录一个键值对。

**RawDelete**：和RawPut类似，不过就是Modify的Data类型应该为Delete而已

**RawScan**：创建Storage.Reader，调用IterCF定义迭代器，对数据进行迭代。注意RawScanRes中用KVPair类型的列表记录键值对，其中KVPair相当于是Key-Value结构体，需要特别组织一下。

特别注意：响应回复需要一开始就定义出来，之后每种特殊处理都是在他的基础上进行改动，而不能每一种分别定义回复，会出现地址错误的问题

bug: 

1. 调用迭代器的最后应该关闭迭代器
2. Go语言中只有for循环语句，
3. 在用迭代器进行循环的时候，注意有迭代次数限制





