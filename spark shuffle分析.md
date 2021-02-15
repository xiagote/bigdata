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
6、
</ol>