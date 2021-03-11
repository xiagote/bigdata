## spark shuffle read分析
shuffle分为write和read两个阶段。read阶段主要解决拉取上游map生成的分区数据，进行组织和计算，为后续的transformation提供输入数据。  

## spark shuffle read代码分析
shuffle read操作发生在转换shuffledRDD的compute方法种。
```scala
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    // ResultTask或者ShuffleMapTask在执行到shuffleRDD时，会调用compute方法
    // 来计算当前RDD的partition数据
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    // 获取ShuffleManager的reader去拉取ShuffleMapTask需要聚合的数据
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }
```
1、调用BlockStoreShuffleReader的read方法读取数据  
```scala
  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    // 构造ShuffleBlockFetcherIterator迭代器。负责从上游fetch blockid中的数据（writer阶段数据是合并到一个blockid文件中，所有数据是其中一段），然后根据数据创建InputStream，并返回blockid和创建的stream。显然如果上游有三个partition,每个partition的数据出局文件中有一段是当前的输入，那这个迭代器三次就结束了。
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      // MapOutputTracker在SparkEnv启动的时候实例化
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true))
    
    // 获取序列化实例
    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      // 使用自定义的序列化方式包装一下输入流，这样就能正常读出反序列化后的对象；
      // 然后调用asKeyValueIterator转换成NextIterator
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    // 这个迭代器使得任务能够被优雅的取消
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    // 如果reduce端需要聚合
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      // 如果map端已经聚合过了
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        // 对读取到的聚合结果进行再次聚合
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // 如果map端没有聚合，则针对未合并的<K,V>进行聚合
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // 如果需要对key进行排序，进行排序。
    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // 为了减少内存压力和避免GC开销，引入了外部排序器，当内存不足时会根据配置文件spark.shuffle.spill决定是否进行spill操作；在SortShuffleWriter的writer阶段也会调用insertAll进行排序
        // Create an ExternalSorter to sort the data.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        context.addTaskCompletionListener[Unit](_ => {
          sorter.stop()
        })
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        // 如果不需要排序，直接返回
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
```
2、ShuffleBlockFetcherIterator的初始化  
在实例化ShuffleBlockFetcherIterator时，会调用initialize()方法
```scala
  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener[Unit](_ => cleanup())

    // Split local and remote blocks.
    // 切分本地和远程的block
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    // 发送请求获取数据
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    // 拉取本地数据
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }
```
splitLocalRemoteBlocks解析block的位置
```scala
private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    // 远端请求从最多5个node去获取数据，每一个节点拉取的数据取决于spark.reducer.maxMbInFlight,
    // 即maxBytesInFlight参数。整个集群只允许每次在5台拉取5G的数据，那么每一个节点只允许拉取1G数
    // 据，这样就允许并行从5个节点获取，而不是从一个节点获取
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize
      + ", maxBlocksInFlightPerAddress: " + maxBlocksInFlightPerAddress)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    // 创建FetchRequest队列，用于存放拉取数据的请求，每一个请求可能包含多个block
    // 具体多少取决于总的请求block大小是否超过目标阈值
    val remoteRequests = new ArrayBuffer[FetchRequest]

    for ((address, blockInfos) <- blocksByAddress) {
      if (address.executorId == blockManager.blockManagerId.executorId) {
        blockInfos.find(_._2 <= 0) match {
          case Some((blockId, size)) if size < 0 =>
            throw new BlockException(blockId, "Negative block size " + size)
          case Some((blockId, size)) if size == 0 =>
            throw new BlockException(blockId, "Zero-sized blocks should be excluded.")
          case None => // do nothing.
        }
        localBlocks ++= blockInfos.map(_._1)
        numBlocksToFetch += localBlocks.size
      } else {
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          } else if (size == 0) {
            throw new BlockException(blockId, "Zero-sized blocks should be excluded.")
          } else {
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            numBlocksToFetch += 1
            curRequestSize += size
          }
          // 如果当前请求的大小已经超过了阈值
          if (curRequestSize >= targetRequestSize ||
              curBlocks.size >= maxBlocksInFlightPerAddress) {
            // 创建一个新的FetchRequest，放到请求队列
            // Add this FetchRequest
            remoteRequests += new FetchRequest(address, curBlocks)
            logDebug(s"Creating fetch request of $curRequestSize at $address "
              + s"with ${curBlocks.size} blocks")
            // 重置当前的block列表
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            // 重置当前请求数量
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks including ${localBlocks.size}" +
        s" local blocks and ${remoteBlocks.size} remote blocks")
    remoteRequests
  }
```