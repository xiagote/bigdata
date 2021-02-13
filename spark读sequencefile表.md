# spark读SequenceFile表
&ensp;spark.sql读sequenceFile表时,在日志中会出现扫描的文件数，例子如下所示：
```
21/02/12 20:54:50 INFO FileInputFormat: Total input paths to process : 3
21/02/12 20:54:51 INFO FileInputFormat: Total input paths to process : 3
```
&ensp;比较好奇哪里整个执行流程，分析下运行流程。
## 构造执行环境
&ensp;在分析读表流程之前，构造表以及插入数据。这里构造了一个分区表，表的存储格式是SEQUENCEFILE。为了方便分析，采用单击模式运行。
```java
public void insertData(SparkSession spark) {
        String delete_sql = "drop table if exists Employee";
        String sql_text = "create table if not exists Employee(id int, name string) partitioned by(dt string) STORED AS SEQUENCEFILE";
        // spark.sql(delete_sql);
        spark.sql(sql_text);

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "a"),
                RowFactory.create(2, "b"),
                RowFactory.create(3, "c")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> dataFrame = spark.createDataFrame(data, schema);
        dataFrame.createOrReplaceTempView("tmpTable");
        spark.sql("insert into Employee partition(dt='20210210') select id, name from tmpTable");
    }
```
## 分析执行流程前奏
&ensp;为了提高分析的效率，可以使用explain方法分析读表的过程。
```java
public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "D:\\winutils-master\\hadoop-2.7.3\\");
        SparkSession spark = SparkSession.
                builder()
                .appName("read_table")
                .master("local[3]")
                .enableHiveSupport()
                .getOrCreate();

        // spark.sparkContext().setLogLevel("DEBUG");
        String sql = "select * from Employee ";
        spark.sql(sql).explain(true);
        // spark.sql(sql).show();
    }
```
&ensp;上述的执行结果如下，执行过程有Logical Plan、Analyzed Logical Plan、Optimized Logical Plan和Physical Plan。逻辑计划是HiveTableRelation，对应的物理计划是HiveTableScanExec。
```
== Parsed Logical Plan ==
'Project [*]
+- 'UnresolvedRelation `Employee`

== Analyzed Logical Plan ==
id: int, name: string, dt: string
Project [id#7, name#8, dt#9]
+- SubqueryAlias `default`.`employee`
   +- HiveTableRelation `default`.`employee`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [id#7, name#8], [dt#9]

== Optimized Logical Plan ==
HiveTableRelation `default`.`employee`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [id#7, name#8], [dt#9]

== Physical Plan ==
Scan hive default.employee [id#7, name#8, dt#9], HiveTableRelation `default`.`employee`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [id#7, name#8], [dt#9]

```
## 分析执行流程
&ensp;为了触发执行流程，调用action操作，这里使用的是show方法。直接把前面的explain变成show即可。在执行过程中，会调用HiveTableScanExec的doExecute生成初始的rdd。
```scala
protected override def doExecute(): RDD[InternalRow] = {
    // Using dummyCallSite, as getCallSite can turn out to be expensive with
    // multiple partitions.
    val rdd = if (!relation.isPartitioned) {
      Utils.withDummyCallSite(sqlContext.sparkContext) {
        hadoopReader.makeRDDForTable(hiveQlTable)
      }
    } else {
      Utils.withDummyCallSite(sqlContext.sparkContext) {
        hadoopReader.makeRDDForPartitionedTable(prunePartitions(rawPartitions))
      }
    }
    val numOutputRows = longMetric("numOutputRows")
    // Avoid to serialize MetastoreRelation because schema is lazy. (see SPARK-15649)
    val outputSchema = schema
    rdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(outputSchema)
      proj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }
```
<ol>
1、分析表的分区，提取分区。  
</br>
这里创建的表是分区表，所以doExecute内实际执行的是hadoopReader.makeRDDForPartiitonedTable()，因为没有where条件，所以是遍历所有的分区。sparkSession.sessionStat.catalog.listPartitions

```scala
// exposed for tests
  @transient lazy val rawPartitions = {
    val prunedPartitions =
      if (sparkSession.sessionState.conf.metastorePartitionPruning &&
          partitionPruningPred.size > 0) {
        // Retrieve the original attributes based on expression ID so that capitalization matches.
        // 如果where有过滤分区的字段，这里调用的是...
        val normalizedFilters = partitionPruningPred.map(_.transform {
          case a: AttributeReference => originalAttributes(a)
        })
        sparkSession.sessionState.catalog.listPartitionsByFilter(
          relation.tableMeta.identifier,
          normalizedFilters)
      } else {
        sparkSession.sessionState.catalog.listPartitions(relation.tableMeta.identifier)
      }
    prunedPartitions.map(HiveClientImpl.toHivePartition(_, hiveQlTable))
  }

```
2、创建RDDForPartitionedTable。  

确定分区，以及需要读取的数据之后，创建RDD。
```scala
  override def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[InternalRow] = {
    val partitionToDeserializer = partitions.map(part =>
      (part, part.getDeserializer.getClass.asInstanceOf[Class[Deserializer]])).toMap
    makeRDDForPartitionedTable(partitionToDeserializer, filterOpt = None)
  }
```
3、生成UnionRDD或者EmptyRDD。  

这里省略掉不关注的部分，只关注最后生成的RDD。可以看到代码中的生成结果是EmptyRDD或者UnionRDD。假设有数据，也就是生成UnionRDD。
```scala
/**
   * Create a HadoopRDD for every partition key specified in the query. Note that for on-disk Hive
   * tables, a data directory is created for each partition corresponding to keys specified using
   * 'PARTITION BY'.
   *
   * @param partitionToDeserializer Mapping from a Hive Partition metadata object to the SerDe
   *     class to use to deserialize input Writables from the corresponding partition.
   * @param filterOpt If defined, then the filter is used to reject files contained in the data
   *     subdirectory of each partition being read. If None, then all files are accepted.
   */
  def makeRDDForPartitionedTable(
      partitionToDeserializer: Map[HivePartition, Class[_ <: Deserializer]],
      filterOpt: Option[PathFilter]): RDD[InternalRow] = {
    // 省略不关注的部分

    // Even if we don't use any partitions, we still need an empty RDD
    if (hivePartitionRDDs.size == 0) {
      new EmptyRDD[InternalRow](sparkSession.sparkContext)
    } else {
      new UnionRDD(hivePartitionRDDs(0).context, hivePartitionRDDs)
    }
  }
```
4、创建HadoopRDD。  

生成UinonRDD或者EmptyRDD之前，有一个基础的的RDD，实际上生成的是hadoopRDD。创建HadoopRdd的inputFormatClass是org.apache.hadoop.mapred.SequenceFileInputFormat，有兴趣的话，可以看看这个类的getSplits。
```scala
/**
   * Create a HadoopRDD for every partition key specified in the query. Note that for on-disk Hive
   * tables, a data directory is created for each partition corresponding to keys specified using
   * 'PARTITION BY'.
   *
   * @param partitionToDeserializer Mapping from a Hive Partition metadata object to the SerDe
   *     class to use to deserialize input Writables from the corresponding partition.
   * @param filterOpt If defined, then the filter is used to reject files contained in the data
   *     subdirectory of each partition being read. If None, then all files are accepted.
   */
  def makeRDDForPartitionedTable(
      partitionToDeserializer: Map[HivePartition, Class[_ <: Deserializer]],
      filterOpt: Option[PathFilter]): RDD[InternalRow] = {
    ...

    val hivePartitionRDDs = verifyPartitionPath(partitionToDeserializer)
      .map { case (partition, partDeserializer) =>
      // Create local references so that the outer object isn't serialized.
      val localTableDesc = tableDesc
      //创建hadoopRdd
      createHadoopRdd(localTableDesc, inputPathStr, ifc).mapPartitions { iter =>
        val hconf = broadcastedHiveConf.value.value
        ...
      }
    }.toSeq
  }
```
5、RDD的parritions方法。  

类RDD里面有个partitions的方法，主要作用是获取这个RDD的分区，Array[Partition]。
```scala
  /**
   * Get the array of partitions of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   */
  final def partitions: Array[Partition] = {
    checkpointRDD.map(_.partitions).getOrElse {
      if (partitions_ == null) {
        stateLock.synchronized {
          if (partitions_ == null) {
            partitions_ = getPartitions
            partitions_.zipWithIndex.foreach { case (partition, index) =>
              require(partition.index == index,
                s"partitions($index).partition == ${partition.index}, but it should equal $index")
            }
          }
        }
      }
      partitions_
    }
  }
```
前面生成的RDD是UnionRDD，getPartitions方法就是UnionRDD内被重写的方法。如果partition的数量比较多，还会采用并发的方式去读。
```scala
 override def getPartitions: Array[Partition] = {
    val parRDDs = if (isPartitionListingParallel) {
      val parArray = rdds.par
      parArray.tasksupport = UnionRDD.partitionEvalTaskSupport
      parArray
    } else {
      rdds
    }
    val array = new Array[Partition](parRDDs.map(_.partitions.length).seq.sum)
    var pos = 0
    for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {
      array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
      pos += 1
    }
    array
  }
```
生成UnionRDD时传入的rdd是HadoopRDD。因此，getPartitions中的parRDDs.map(_.partitions.length)调用的是HadoopRDD中的getPartitions方法。_.partitions的调用就是RDD的partitions方法。
```scala
 override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    try {
      val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
      val array = new Array[Partition](inputSplits.size)
      for (i <- 0 until inputSplits.size) {
        array(i) = new HadoopPartition(id, i, inputSplits(i))
      }
      array
    } catch {
      case e: InvalidInputException if ignoreMissingFiles =>
        logWarning(s"${jobConf.get(FileInputFormat.INPUT_DIR)} doesn't exist and no" +
            s" partitions returned from this path.", e)
        Array.empty[Partition]
    }
  }
```
进行到这一步，就会通过getSplits(jobConf,minPartitions)读取本地或集群的文件。生成HadoopRDD的inputFormatClass是SequenceFileInputFormat，可以看下这个类的getSplits方法。这里就会扫描一个分区内的文件。整个流程就是读SequenceFile表的过程。
</ol>