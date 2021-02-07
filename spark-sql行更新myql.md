# spark行更新mysql表
## spark 自带的mode
<br/>

&ensp;在保存文件时，需要确认保存的格式。spark自带的四种模式，分别是：overwrite、append、ignore以及error|errorifexists|default。  
&ensp;代码如下，实际上是用枚举的方式。  
```scala
  /**
   * Specifies the behavior when data or table already exists. Options include:
   * <ul>
   * <li>`overwrite`: overwrite the existing data.</li>
   * <li>`append`: append the data.</li>
   * <li>`ignore`: ignore the operation (i.e. no-op).</li>
   * <li>`error` or `errorifexists`: default option, throw an exception at runtime.</li>
   * </ul>
   *
   * @since 1.4.0
   */
  def mode(saveMode: String): DataFrameWriter[T] = {
    this.mode = saveMode.toLowerCase(Locale.ROOT) match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" | "errorifexists" | "default" => SaveMode.ErrorIfExists
      case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode. " +
        "Accepted save modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists'.")
    }
    this
  }
```
  
## spark插入mysql的运行过程
<br/>

&ensp;在改造代码之前，需要了解spark插入mysql的运行流程。  
<ol>
1、在本地构建spark的运行环境，完成测试代码。在调试过程中，为了避免df的生成增加分析负担，可以先执行action操作并且persist DataFrame。

```java
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("mysql_test")
                .master("local[3]")
                .enableHiveSupport()
                .getOrCreate();

        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "root123");
        prop.put("driver", "com.mysql.jdbc.Driver");

        String url = "jdbc:mysql://localhost:3306/data";
        Dataset<Row> df = spark.read().jdbc(url, "table_name", prop).persist();
        df.show();

        // 调试处
        df.write().mode("append").jdbc(url, "table_name", prop);
    }
```
2、生成Logic plan SaveIntoDataSourceCommand，这个Command继承RunnableCommand。
```scala
 private def saveToV1Source(): Unit = {
    if (SparkSession.active.sessionState.conf.getConf(
      SQLConf.LEGACY_PASS_PARTITION_BY_AS_OPTIONS)) {
      partitioningColumns.foreach { columns =>
        extraOptions += (DataSourceUtils.PARTITIONING_COLUMNS_KEY ->
          DataSourceUtils.encodePartitioningColumns(columns))
      }
    }

    // Code path for data source v1.
    runCommand(df.sparkSession, "save") {
      DataSource(
        sparkSession = df.sparkSession,
        className = source,
        partitionColumns = partitioningColumns.getOrElse(Nil),
        options = extraOptions.toMap).planForWriting(mode, df.logicalPlan)
    }
  }

/**
   * Returns a logical plan to write the given [[LogicalPlan]] out to this [[DataSource]].
   */
  def planForWriting(mode: SaveMode, data: LogicalPlan): LogicalPlan = {
    if (data.schema.map(_.dataType).exists(_.isInstanceOf[CalendarIntervalType])) {
      throw new AnalysisException("Cannot save interval data type into external storage.")
    }

    // providintClass = "org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider"
    providingClass.newInstance() match {
      case dataSource: CreatableRelationProvider =>
        SaveIntoDataSourceCommand(data, dataSource, caseInsensitiveOptions, mode)
      case format: FileFormat =>
        DataSource.validateSchema(data.schema)
        planForWritingFileFormat(format, mode, data)
      case _ =>
        sys.error(s"${providingClass.getCanonicalName} does not allow create table as select.")
    }
  }

```
3、生成Physical plan, RunnableCommand生成的物理计划是ExecutedCommandExec。
```scala
object BasicOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case d: DataWritingCommand => DataWritingCommandExec(d, planLater(d.query)) :: Nil
      // 生成物理计划
      case r: RunnableCommand => ExecutedCommandExec(r) :: Nil

      case MemoryPlan(sink, output) =>
        val encoder = RowEncoder(sink.schema)
        LocalTableScanExec(output, sink.allData.map(r => encoder.toRow(r).copy())) :: Nil
      case MemoryPlanV2(sink, output) =>
        val encoder = RowEncoder(StructType.fromAttributes(output))
        LocalTableScanExec(output, sink.allData.map(r => encoder.toRow(r).copy())) :: Nil
    }
}
```
4、执行物理计划，实际执行的是ExecutedCommandExec.doExecute()，最终执行SaveIntoDataSourceCommand的run方法。
```scala
/**
 * A physical operator that executes the run method of a `RunnableCommand` and
 * saves the result to prevent multiple executions.
 *
 * @param cmd the `RunnableCommand` this operator will run.
 */
case class ExecutedCommandExec(cmd: RunnableCommand) extends LeafExecNode {

  override lazy val metrics: Map[String, SQLMetric] = cmd.metrics

  /**
   * A concrete command should override this lazy field to wrap up any side effects caused by the
   * command or any other computation that should be evaluated exactly once. The value of this field
   * can be used as the contents of the corresponding RDD generated from the physical plan of this
   * command.
   *
   * The `execute()` method of all the physical command classes should reference `sideEffectResult`
   * so that the command can be executed eagerly right after the command query is created.
   */
  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    cmd.run(sqlContext.sparkSession).map(converter(_).asInstanceOf[InternalRow])
  }
  // 忽略不相干的方法
  ...
  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
  }
}
/**
 * Saves the results of `query` in to a data source.
 *
 * Note that this command is different from [[InsertIntoDataSourceCommand]]. This command will call
 * `CreatableRelationProvider.createRelation` to write out the data, while
 * [[InsertIntoDataSourceCommand]] calls `InsertableRelation.insert`. Ideally these 2 data source
 * interfaces should do the same thing, but as we've already published these 2 interfaces and the
 * implementations may have different logic, we have to keep these 2 different commands.
 */
case class SaveIntoDataSourceCommand(
    query: LogicalPlan,
    dataSource: CreatableRelationProvider,
    options: Map[String, String],
    mode: SaveMode) extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    dataSource.createRelation(
      sparkSession.sqlContext, mode, options, Dataset.ofRows(sparkSession, query))

    Seq.empty[Row]
  }

  override def simpleString: String = {
    val redacted = SQLConf.get.redactOptions(options)
    s"SaveIntoDataSourceCommand ${dataSource}, ${redacted}, ${mode}"
  }
}

```
5、调用JdbcRelationProvider.createRelation方法，执行写表。这里会根据mode的类型进行具体的写表。
```scala
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}

class JdbcRelationProvider extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {

  override def shortName(): String = "jdbc"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val resolver = sqlContext.conf.resolver
    val timeZoneId = sqlContext.conf.sessionLocalTimeZone
    val schema = JDBCRelation.getSchema(resolver, jdbcOptions)
    val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
    JDBCRelation(schema, parts, jdbcOptions)(sqlContext.sparkSession)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    val options = new JdbcOptionsInWrite(parameters)
    val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis

    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      val tableExists = JdbcUtils.tableExists(conn, options)
      if (tableExists) {
        mode match {
          case SaveMode.Overwrite =>
            if (options.isTruncate && isCascadingTruncateTable(options.url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, options)
              val tableSchema = JdbcUtils.getSchemaOption(conn, options)
              saveTable(df, tableSchema, isCaseSensitive, options)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, options.table, options)
              createTable(conn, df, options)
              saveTable(df, Some(df.schema), isCaseSensitive, options)
            }

          case SaveMode.Append =>
            val tableSchema = JdbcUtils.getSchemaOption(conn, options)
            saveTable(df, tableSchema, isCaseSensitive, options)

          case SaveMode.ErrorIfExists =>
            throw new AnalysisException(
              s"Table or view '${options.table}' already exists. " +
                s"SaveMode: ErrorIfExists.")

          case SaveMode.Ignore =>
            // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
            // to not save the contents of the DataFrame and to not change the existing data.
            // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        createTable(conn, df, options)
        saveTable(df, Some(df.schema), isCaseSensitive, options)
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }
}
```
6、以mode等于append时为例，继续后续的环节。getInsertStatement会生成stmt，实际上就是insert into....
```scala
  /**
   * Saves the RDD to the database in a single transaction.
   */
  def saveTable(
      df: DataFrame,
      tableSchema: Option[StructType],
      isCaseSensitive: Boolean,
      options: JdbcOptionsInWrite): Unit = {
    val url = options.url
    val table = options.table
    val dialect = JdbcDialects.get(url)
    val rddSchema = df.schema
    val getConnection: () => Connection = createConnectionFactory(options)
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val insertStmt = getInsertStatement(table, rddSchema, tableSchema, isCaseSensitive, dialect)
    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw new IllegalArgumentException(
        s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
          "via JDBC. The minimum value is 1.")
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }
    repartitionedDF.rdd.foreachPartition(iterator => savePartition(
      getConnection, table, iterator, rddSchema, insertStmt, batchSize, dialect, isolationLevel,
      options)
    )
  }
```

</ol> 

## 改造spark代码 
### 使用insert ignore into方式插入mysql表。  
&ensp;对于mysql的主键表，当用spark的四种方式插入表时，会报错。分析完spark插入mysql表数据的流程，修改源代码就比较清晰。
&ensp;这里需要修改JdbcUtils.getInsertStatement方法就可以了。加一个判断，如果是ignore，换成insert ignore into。
```scala
  /**
   * Returns an Insert SQL statement for inserting a row into the target table via JDBC conn.
   */
  def getInsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      isCaseSensitive: Boolean,
      dialect: JdbcDialect): String = {
    val columns = if (tableSchema.isEmpty) {
      rddSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    } else {
      val columnNameEquality = if (isCaseSensitive) {
        org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
      } else {
        org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
      }
      // The generated insert statement needs to follow rddSchema's column sequence and
      // tableSchema's column names. When appending data into some case-sensitive DBMSs like
      // PostgreSQL/Oracle, we need to respect the existing case-sensitive column names instead of
      // RDD column names for user convenience.
      val tableColumnNames = tableSchema.get.fieldNames
      rddSchema.fields.map { col =>
        val normalizedName = tableColumnNames.find(f => columnNameEquality(f, col.name)).getOrElse {
          throw new AnalysisException(s"""Column "${col.name}" not found in schema $tableSchema""")
        }
        dialect.quoteIdentifier(normalizedName)
      }.mkString(",")
    }
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    s"INSERT INTO $table ($columns) VALUES ($placeholders)"
  }
```
### 使用insert ignore into方式插入mysql表。  