## 宽依赖与窄依赖  
Spark中RDD的高效执行与DAG图有莫大的关系，在DAG调度中需要对计算过程进行stage划分，划分的依据是RDD之间的依赖关系。RDD的依赖分为窄依赖（narrow dependency）和宽依赖（wide dependency）。
- 宽依赖  
父RDD中每个分区数据都只被子RDD的一个分区使用
- 窄依赖  
父RDD中每个分区数据会被子RDD的多个分区使用  
![rdd dependency](https://raw.githubusercontent.com/xiagote/bigdata/master/picture/rdd-dependency.jpg)  
宽依赖和窄依赖的区别在于RDD是否存在shuffle操作。spark常见的shuffle算子有：  
```scala
// 去重
def distinct()
def distinct(numPartitions: Int)

// 聚合
def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]
def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)]
def groupBy[K](f: T => K, p: Partitioner):RDD[(K, Iterable[V])]
def groupByKey(partitioner: Partitioner):RDD[(K, Iterable[V])]
def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner): RDD[(K, U)]
def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int): RDD[(K, U)]
def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C): RDD[(K, C)]
def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, numPartitions: Int): RDD[(K, C)]
def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, partitioner: Partitioner, mapSideCombine: Boolean = true, serializer: Serializer = null): RDD[(K, C)]

// 排序
def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length): RDD[(K, V)]
def sortBy[K](f: (T) => K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]

// 重分区
def coalesce(numPartitions: Int, shuffle: Boolean = false, partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null)

// 集合或表操作
def intersection(other: RDD[T]): RDD[T]
def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T]
def intersection(other: RDD[T], numPartitions: Int): RDD[T]
def subtract(other: RDD[T], numPartitions: Int): RDD[T]
def subtract(other: RDD[T], p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T]
def subtractByKey[W: ClassTag](other: RDD[(K, W)]): RDD[(K, V)]
def subtractByKey[W: ClassTag](other: RDD[(K, W)], numPartitions: Int): RDD[(K, V)]
def subtractByKey[W: ClassTag](other: RDD[(K, W)], p: Partitioner): RDD[(K, V)]
def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))]
def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))]
def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
```
## spark shuffle的演进历史
- Spark 0.8及以前 Hash Based Shuffle
- Spark 0.8.1为 Hasn Based Shuffle引入File Consolidation机制
- Spark 0.9引入ExternalAppendOnlyMap
- Spark 1.1引入Sort Based Shuffle，但默认仍为Hash Based Shuffle
- Spark 1.2默认的Shuffle方式改为Sort Based Shuffle
- Spark 1.4引入Tungsten-Sort Based Shuffle
- Spark 1.6 Tungsten-sort并入Sort Based Shuffle
- Spark 2.0 Hash Based Shuffle退出历史舞台

## spark shuffle原理(FIXME)

## 实现细节（FIXME）

## shuffle代码分析
<ol>
1、ShuffleManager  

在初始化SparkContext时，会创建spark的运行环境。在SparkEnv中初始化ShuffleManager。spark默认的shuffleManager时sort shuffle manager
```scala
// SparkContext.scala
// Create the Spark execution environment (cache, map output tracker, etc)
    _env = createSparkEnv(_conf, isLocal, listenerBus)

// SparkEnv.scala
// Let the user specify short names for shuffle managers
    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass =
      shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase(Locale.ROOT), shuffleMgrName)
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)
```
2、ShuffleDependency  
如果RDD操作中存在Shuffle操作，整个RDD将转换成ShuffleDependency。初始化ShuffleDependency时，初始化shuffleHandle，决定shuffle的RDD以哪种方式写入本地。
```scala
// ShuffleDependency初始时生成shuffleHandle
val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(shuffleId, _rdd.partitions.length, this)

  /**
   * Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // 如果满足BypassMergeSort，优先使用BypassMergeSortShuffleHandle
    if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      new BypassMergeSortShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // 如果支持序列化模式，使用SerializedShuffleHandle
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }
```
2.1、分析shouldBypassMergeSort，如果满足条件：<b>（1）不存在局部聚合；（2）shuffle的分区数小于设定的参数</b>
```scala
private[spark] object SortShuffleWriter {
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    // 不需要局部聚合
    if (dep.mapSideCombine) {
      false
    } else {
      val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
      dep的partitions数量小于默认数量
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
```
2.2、分析canUseSerializedShuffle，如果满足条件：<b>（1）支持序列化<font color="red">（待分析）</font>；（2）不需要局部聚合；（3）分区数小于16777216</b>
```scala
  /**
   * Helper method for determining whether a shuffle should use an optimized serialized shuffle
   * path or whether it should fall back to the original path that operates on deserialized objects.
   */
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val shufId = dependency.shuffleId
    // 获取分区数
    val numPartitions = dependency.partitioner.numPartitions
    // 是否支持序列化值的重新定位
    if (!dependency.serializer.supportsRelocationOfSerializedObjects) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because the serializer, " +
        s"${dependency.serializer.getClass.getName}, does not support object relocation")
      false
      // 是否需要进行局部聚合
    } else if (dependency.mapSideCombine) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because we need to do " +
        s"map-side aggregation")
      false
      // 分区数量是否大于16777216
    } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because it has more than " +
        s"$MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE partitions")
      false
    } else {
      log.debug(s"Can use serialized shuffle for shuffle $shufId")
      true
    }
  }
```
3、ShuffleMapTask  
Spark中的stage有两种，一种时ResultStage，一种时ShuffleMapStage。在提交任务时，如果是resultStage，执行的是ResultTask；如果是ShuffleMapStage，执行的是ShuffleMapTask。ShuffleMapTask中需要将rdd的内容写入到硬盘中，首先获取写文件的类，然后调用write写文件。
对于ShuffleMapTask，执行的代码如下：
```scala
  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      // dep.shuffleHandle就是步骤2中的handle，获取shuffle中write的方法
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }
```
4、ShuffleManager.getWriter  
```scala
  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    numMapsForShuffle.putIfAbsent(
      handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    val env = SparkEnv.get
    handle match {
      // 如果使用SerializedShuffleHandle则获取UnsafeShuffleWriter
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf)
      // 如果使用BypassMergeSortShuffleHandle则获取BypassMergeSortShuffleWriter
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new BypassMergeSortShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          bypassMergeSortHandle,
          mapId,
          context,
          env.conf)
      // 如果使用BaseShuffleHandle则获取SortShuffleWriter
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new SortShuffleWriter(shuffleBlockResolver, other, mapId, context)
    }
  }
```
5、BypassMergeSortShuffleWriter  
```java
@Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    if (!records.hasNext()) {
      partitionLengths = new long[numPartitions];
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, null);
      mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
      return;
    }
    final SerializerInstance serInstance = serializer.newInstance();
    final long openStartTime = System.nanoTime();
    // 构建一个对于task结果对应分区数量的write数组，即一个分区对应一个writer
    // 对于这种写入方式，会同时打开numPartition个writer，所以分区数量不宜过大，避免带来国中的内存开销
    // 现在默认writer的缓存大小时32k
    partitionWriters = new DiskBlockObjectWriter[numPartitions];
    // 创建FileSegment数组，一个分区的writer对应一组FileSegment
    partitionWriterSegments = new FileSegment[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      // 创建临时的shuffle block,返回一个(shuffle blockid, file)数组
      final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
        blockManager.diskBlockManager().createTempShuffleBlock();
      // 获取该分区对应的文件
      final File file = tempShuffleBlockIdPlusFile._2();
      // 获取该分区对应的blockid
      final BlockId blockId = tempShuffleBlockIdPlusFile._1();
      // 构造每一个分区的writer
      partitionWriters[i] =
        blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
    }
    // Creating the file to write to and creating a disk writer both involve interacting with
    // the disk, and can take a long time in aggregate when we open many files, so should be
    // included in the shuffle write time.
    writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

    // 如果有数据，获取数据，对key进行分区，然后将<key,value>写入该分区对应的文件
    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
      final K key = record._1();
      partitionWriters[partitioner.getPartition(key)].write(key, record._2());
    }

    // 遍历所有分区的writer列表，刷新数据到文件，构建FileSegment数组
    for (int i = 0; i < numPartitions; i++) {
      final DiskBlockObjectWriter writer = partitionWriters[i];
      // 把数据刷到磁盘，构建一个FileSegment
      partitionWriterSegments[i] = writer.commitAndGet();
      writer.close();
    }

    // 根据shuffleId和mapId，构建ShuffleDataBlockId，创建文件，格式为
    //shuffle_{shuffleId}_{mapId}_{reduceId}.data
    File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    // 创建临时文件
    File tmp = Utils.tempFileWith(output);
    try {
      // 合并前面生成的各个中间临时文件，并获取分区对应的数据大小，然后开始计算偏移量
      partitionLengths = writePartitionedFile(tmp);
      // 创建索引文件，将每一个分区的起始位置、结束位置和偏移量写入索引
      // 且将合并的data临时文件重命名，索引文件的临时文件重命名
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    // 封装并返回任何结果
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }
```
5.1、writePartitionedFile合并所有分区的文件，并获取每个分区文件的大小
```java
/**
   * Concatenate all of the per-partition files into a single combined file.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  private long[] writePartitionedFile(File outputFile) throws IOException {
    // Track location of the partition starts in the output file
    // 构建一个分区数量大小的数组
    final long[] lengths = new long[numPartitions];
    if (partitionWriters == null) {
      // We were passed an empty iterator
      return lengths;
    }

    // 创建合并文件的临时文件输出流
    final FileOutputStream out = new FileOutputStream(outputFile, true);
    final long writeStartTime = System.nanoTime();
    boolean threwException = true;
    try {
      // 合并分区文件，返回每一个分区文件长度
      for (int i = 0; i < numPartitions; i++) {
        // 获取该分区对应的FileSegment对应的文件
        final File file = partitionWriterSegments[i].file();
        // 如果文件存在
        if (file.exists()) {
          final FileInputStream in = new FileInputStream(file);
          boolean copyThrewException = true;
          try {
            // 把文件复制到临时文件中，并返回文件长度
            lengths[i] = Utils.copyStream(in, out, false, transferToEnabled);
            copyThrewException = false;
          } finally {
            Closeables.close(in, copyThrewException);
          }
          if (!file.delete()) {
            logger.error("Unable to delete file for partition {}", i);
          }
        }
      }
      threwException = false;
    } finally {
      Closeables.close(out, threwException);
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
    }
    partitionWriters = null;
    return lengths;
  }
```
5.2、writeIndexFileAndCommit创建文件索引
```scala
/**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File): Unit = {
    // 获取索引文件
    val indexFile = getIndexFile(shuffleId, mapId)
    // 临时的索引文件
    val indexTmp = Utils.tempFileWith(indexFile)
    try {
      // 获取数据文件
      val dataFile = getDataFile(shuffleId, mapId)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      synchronized {
        // 传递索引、数据文件以及分区数，校验准确性
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (dataTmp != null && dataTmp.exists()) {
            dataTmp.delete()
          }
        } else {
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
          Utils.tryWithSafeFinally {
            // We take in lengths of each block, need to convert it to offsets.
            // 将offset写入临时的索引文件
            var offset = 0L
            out.writeLong(offset)
            for (length <- lengths) {
              offset += length
              out.writeLong(offset)
            }
          } {
            out.close()
          }

          if (indexFile.exists()) {
            indexFile.delete()
          }
          if (dataFile.exists()) {
            dataFile.delete()
          }
          if (!indexTmp.renameTo(indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }
    } finally {
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
    }
  }
```
6、SortShuffleWriter
```scala
  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // map端是否需要在本地进行combine操作，如果需要，则需要传入aggregator和keyOrdering，创建ExternalSorter
    // aggregator用于指导combiner的操作，keyOrdering用于传递key的排序规则
    sorter = if (dep.mapSideCombine) {
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      // 如果不需要在本地进行combine操作，不需要aggregator和keyOrdering
      // 本地每个分区的数据不需要做聚合和排序
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    // 将写入数据全部放入外部排序器ExternalSorter，并且根据是否需要spill进行spill操作
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    // 创建data文件，文件格式为'shuffle_{shuffleId}_{mapId}_{reducerId}.data'
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    // 创建临时文件
    val tmp = Utils.tempFileWith(output)
    try {
      // 创建shuffle block id: shuffle_{shuffleId}_{mapId}_{reducerId}
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      // 创建index索引文件，写入每个分区的offset和length信息，并且重命名data临时文件和index临时文件
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      // 把部分信息封装到MapStatus返回
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }
```
6.1、insertAll
```scala
def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    // 判断aggregator是否为空，如果不为空，表示需要在本地进行combine
    if (shouldCombine) {
      // Combine values in-memory first using our AppendOnlyMap
      // 使用AppendOnlyMap优先在内存中进行combine
      // 获取aggregator的merge函数，用于merge新的值到聚合记录
      val mergeValue = aggregator.get.mergeValue
      // 获取aggregator的createCombine函数，用于创建聚合的初始值
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      // 创建update函数，如果有值进行mergeValue，如果没有则createCombiner
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        // 处理一个元素，就更新一次结果
        addElementsRead()
        kv = records.next()
        // 计算key的分区，然后开始merge
        map.changeValue((getPartition(kv._1), kv._1), update)
        // 如果溢出，需要将内存数据写入磁盘
        maybeSpillCollection(usingMap = true)
      }
    } else {
      // Stick values into our buffer
      while (records.hasNext) {
        // 处理一个元素，就更新一次结果
        addElementsRead()
        // 取出一个（key,value）
        val kv = records.next()
        // 往PartitionedPairBuffer添加数据
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        // 如果内存溢出，需要将数据写入磁盘
        maybeSpillCollection(usingMap = false)
      }
    }
  }
```
6.2、maybeSpillCollection
```scala
/**
   * Spill the current in-memory collection to disk if needed.
   *
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {
      // 首先估计一下该map的大小
      estimatedSize = map.estimateSize()
      // 然后会根据预估的map大小决定是否需要进行spill
      if (maybeSpill(map, estimatedSize)) {
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      // 不需要进行本地聚合时，使用PartitionedPairBuffer
      // 估算buffer的大小
      estimatedSize = buffer.estimateSize()
      // 根据大小确定是否进行spill
      if (maybeSpill(buffer, estimatedSize)) {
        buffer = new PartitionedPairBuffer[K, C]
      }
    }

    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

```
6.3、maybeSpill
```scala
  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *
   * @param collection collection to spill to disk
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false
    // 如果读取的数据时32的倍数，而且当前内存大于内存阈值（默认是5M）
    // 先尝试向MemoryManager申请（2 * currentMemory - myMemoryThreshold)大小的内存
    // 如果能够申请到，不进行spill操作，而是继续向Buffer中存储数据
    // 否则就会调用spill()方法将Buffer中数据输出到磁盘中
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted = acquireMemory(amountToRequest)
      // 更新现在内存阈值
      myMemoryThreshold += granted
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection
      // 再次判断当前内存是否大于阈值，如果还是大于阈值则继续spill
      shouldSpill = currentMemory >= myMemoryThreshold
    }
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    // Actually spill
    // 如果需要进行spill，则开始进行spill操作
    if (shouldSpill) {
      _spillCount += 1
      logSpillage(currentMemory)
      spill(collection)
      _elementsRead = 0
      _memoryBytesSpilled += currentMemory
      // 释放内存
      releaseMemory()
    }
    shouldSpill
  }
```
6.4、spill
```scala
  /**
   * Spill our in-memory collection to a sorted file that we can merge later.
   * We add this file into `spilledFiles` to find it later.
   *
   * @param collection whichever collection we're using (map or buffer)
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    // 返回一个根据指定的比较器排序的迭代器，这里其实是用hashCode进行比较
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    // 将内存的数据写道硬盘中的临时文件
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    // 更新写道硬盘的临时文件
    spills += spillFile
  
```
6.5、spillMemoryIteratorToDisk
```scala
/**
   * Spill contents of in-memory iterator to a temporary file on disk.
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator): SpilledFile = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    // 因为这些文件在shuffle期间可能被读取，应该用spark.shuffle.compress控制压缩，需要创建临时的shuffle block
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // These variables are reset after each flush
    var objectsWritten: Long = 0
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    // 创建针对临时文件的writer
    val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)

    // List of batch sizes (bytes) in the order they are written to disk
    // 批量写入磁盘列表
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is committed at the end of this process.
    def flush(): Unit = {
      val segment = writer.commitAndGet()
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      // 循环读取内存里的数据
      while (inMemoryIterator.hasNext) {
        val partitionId = inMemoryIterator.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        // 将内存里的数据写入文件
        inMemoryIterator.writeNext(writer)
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1

        // 将数据写入硬盘
        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      // 遍历完数据之后，将数据写入硬盘
      if (objectsWritten > 0) {
        flush()
      } else {
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (success) {
        writer.close()
      } else {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        writer.revertPartialWritesAndClose()
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    // 创建SpilledFile
    SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition)
  }
```
6.6、writePartitionedFile。完成insertAll之后，排序。
```scala
/**
   * Write all the data added into this ExternalSorter into a file in the disk store. This is
   * called by the SortShuffleWriter.
   *
   * @param blockId block ID to write to. The index file will be blockId.name + ".index".
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   */
  def writePartitionedFile(
      blockId: BlockId,
      outputFile: File): Array[Long] = {

    // Track location of each range in the output file
    val lengths = new Array[Long](numPartitions)
    val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
      context.taskMetrics().shuffleWriteMetrics)
    
    // 判断是否有进行了spill的文件
    // 如果spills是空的，表明所有的数据都保存在内存中
    if (spills.isEmpty) {
      // Case where we only have in-memory data
      // 如果指定了aggregator，就返回PartitionedAppendOnlyMap里的数据，否则返回PartitionedPairBuffer里的数据
      val collection = if (aggregator.isDefined) map else buffer
      // 返回一个对结果排序的迭代器
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext) {
        val partitionId = it.nextPartition()
        while (it.hasNext && it.nextPartition() == partitionId) {
          // 通过writer将内存数据写入文件
          it.writeNext(writer)
        }
        // 数据写入硬盘，并且创建FileSegment数组
        val segment = writer.commitAndGet()
        lengths(partitionId) = segment.length
      }
    } else {
      // 表示有数据写到硬盘中，需要进行归并排序(merge-sort)
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      // 每一个分区的数据都写入临时文件
      for ((id, elements) <- this.partitionedIterator) {
        if (elements.hasNext) {
          for (elem <- elements) {
            writer.write(elem._1, elem._2)
          }
          val segment = writer.commitAndGet()
          lengths(id) = segment.length
        }
      }
    }

    writer.close()
    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)

    lengths
  }
```
6.7、partitionedIterator
```scala
  /**
   * Return an iterator over all the data written to this object, grouped by partition and
   * aggregated by the requested aggregator. For each partition we then have an iterator over its
   * contents, and these are expected to be accessed in order (you can't "skip ahead" to one
   * partition without reading the previous one). Guaranteed to return a key-value pair for each
   * partition, in order of partition ID.
   *
   * For now, we just merge all the spilled files in once pass, but this can be modified to
   * support hierarchical merging.
   * Exposed for testing.
   */
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    // 是否需要本地combine
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
    if (spills.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      if (!ordering.isDefined) {
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        //数据只是按照partitionId排序，并不会对key进行排序
        groupByPartition(destructiveIterator(collection.partitionedDestructiveSortedIterator(None)))
      } else {
        // We do need to sort by both partition ID and key
        // 数据先按照partitionId排序，然后分区内部对key进行排序
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(Some(keyComparator))))
      }
    } else {
      // Merge spilled and in-memory data
      // 如果有数据写入硬盘，需要将硬盘里的数据和内存中的数据进行合并
      merge(spills, destructiveIterator(
        collection.partitionedDestructiveSortedIterator(comparator)))
    }
  }
```
</ol>