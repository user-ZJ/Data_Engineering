- [Hadoop-](https://hadoop.apache.org/)用于大数据存储和数据分析的工具生态系统。Hadoop是一个比Spark更旧的系统，但仍被许多公司使用。Spark和Hadoop之间的主要区别在于它们如何使用内存。Hadoop将中间结果写入磁盘，而Spark尝试尽可能将数据保留在内存中。这使得Spark在许多用例中都更快。
- **Hadoop MapReduce-**一种用于并行处理和分析大型数据集的系统。
- **Hadoop YARN-**资源管理器，可跨集群调度作业。管理器跟踪可用的计算机资源，然后将这些资源分配给特定任务。
- **Hadoop分布式文件系统（HDFS）** -一种大数据存储系统，可将数据拆分为多个块，并将这些块存储在计算机集群中。
- **Apache Pig-**一种类似于SQL的语言，在Hadoop MapReduce上运行
- **Apache Hive-**在Hadoop MapReduce上运行的另一个类似SQL的界面
- EC2 : Elastic Compute Cloud
- EMR: Elastic MapReduce是AWS提供的一项服务，它使您（用户）无需手动安装每台机器的Spark及其依赖项
- M family: multipurpose family 多用途组合
- R family:optimized for Ram
- C family:optimized for CPU

# Spark与Hadoop有何关系？

本课程的重点是Spark，它是另一个大数据框架。Spark包含用于数据分析，机器学习，图形分析和实时数据流的库。Spark通常比Hadoop快。这是因为Hadoop将中间结果写入磁盘，而Spark尝试将中间结果尽可能保留在内存中。

Hadoop生态系统包括一个称为HDFS（Hadoop分布式文件系统）的分布式文件存储系统。另一方面，Spark不包含文件存储系统。您可以在HDFS之上使用Spark，但不必这样做。Spark可以从其他来源（例如[Amazon S3）](https://aws.amazon.com/s3/)读取数据。

Spark具有一个称为[Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html)的流媒体库，尽管它不像其他某些流媒体库那样流行和快速。其他流行的流媒体库包括[Storm](http://storm.apache.org/)和 [Flink](https://flink.apache.org/)。本课程不会涉及流媒体，但是您可以通过以下链接来了解有关这些技术的更多信息。





MapReduce是一种用于处理大型数据集的编程技术。“ Hadoop MapReduce”是此编程技术的特定实现。

该技术的工作原理是首先分割大型数据集，然后将数据分布在整个群集中。在映射步骤中，将分析每个数据并将其转换为（键，值）对。然后，将这些键值对在整个群集中混排，以便所有键都在同一台计算机上。在reduce步骤中，将具有相同键的值组合在一起。

尽管Spark没有实现MapReduce，但是您可以编写行为与map-reduce范例相似的Spark程序。



spark适用场景 ：

- [数据分析](http://spark.apache.org/sql/)
- [机器学习](http://spark.apache.org/mllib/)
- [流媒体](http://spark.apache.org/streaming/)
- [图分析](http://spark.apache.org/graphx/)

# Spark的局限性

Spark有一些限制。

Spark Streaming的等待时间至少为500毫秒，因为它对记录的微批次进行操作，而不是一次处理一个记录。诸如[Storm](http://storm.apache.org/)，[Apex](https://apex.apache.org/)或[Flink之](https://flink.apache.org/)类的本地流工具可以降低此延迟值，并且可能更适合于低延迟应用程序。Flink和Apex也可以用于批处理计算，因此，如果您已经将它们用于流处理，则无需将Spark添加到您的技术堆栈中。

Spark的另一个限制是它选择了机器学习算法。当前，Spark仅支持与输入数据大小成线性比例的算法。通常，尽管有许多项目将Spark与Tensorflow和其他深度学习工具集成在一起，但深度学习也不可用。





请记住，Spark不是数据存储系统，除了Spark以外，还有许多其他工具可用于处理和分析大型数据集。您可能听说过较新的数据库存储系统，例如[HBase](https://hbase.apache.org/)或[Cassandra](http://cassandra.apache.org/)。还有分布式SQL引擎，例如[Impala](https://impala.apache.org/)和[Presto](https://prestodb.io/)，根据您在Python和SQL方面的经验，其中许多技术都使用您可能已经熟悉的查询语法。



spark DAG(Directed Acyclical Graph) 数据配方

```python
spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()
```

- `select()`：返回具有选定列的新DataFrame
- `filter()`：使用给定条件过滤行
- `where()`：只是它的别名 `filter()`
- `groupBy()`：使用指定的列对DataFrame进行分组，因此我们可以对它们进行聚合
- `sort()`：返回按指定列排序的新DataFrame。默认情况下，第二个参数“升序”为True。
- `dropDuplicates()`：返回一个具有基于所有列或仅列的子集的唯一行的新DataFrame
- `withColumn()`：通过添加列或替换具有相同名称的现有列来返回新的DataFrame。第一个参数是新列的名称，第二个参数是如何计算它的表达式。
- `agg({"salary": "avg", "age": "max"})`计算平均工资和最大年龄。

spark SQL提供了内置的方法最常见的聚合，例如`count()`，`countDistinct()`，`avg()`，`max()`，`min()`，等在pyspark.sql.functions模块

在Spark SQL中，我们可以使用pyspark.sql.functions模块中的udf方法定义自己的函数。UDF返回的变量的默认类型为字符串。如果我们想返回其他类型，则需要使用pyspark.sql.types模块中的不同类型来显式地返回。

RDD是数据的低层抽象。在Spark的第一个版本中，您直接使用RDD。您可以将RDD视为分布在各种计算机上的长列表。尽管数据框架和SQL更容易，但仍可以将RDD用作Spark代码的一部分。

# spark从S3中读数据

```python
df = spark.read.load(“s3://my_bucket/path/to/file/file.csv”)
# 如果我们使用的是spark，并且存储桶下面的所有对象都具有相同的架构，则可以执行以下操作
df = spark.read.load(“s3://my_bucket/”)
```



# HDFS和AWS S3之间的区别

- **AWS S3**是一个**对象存储系统**，它使用键值对（即存储区和键）存储数据，而**HDFS**是一种**实际的分布式文件系统**，可以保证容错能力。HDFS通过具有重复因素来实现容错能力，这意味着默认情况下，它将在集群中的3个不同节点上复制相同文件（可以将其配置为不同的重复次数）。
- HDFS通常**安装在本地系统中**，并且传统上让工程师在现场维护和诊断Hadoop生态系统，这**比在云上存储数据要花费更多**。由于**位置**的**灵活性**和**降低的维护成本**，云解决方案变得更加流行。借助您可以在AWS内使用的广泛服务，S3比HDFS更受欢迎。
- 由于**AWS S3是二进制对象存储库**，因此它可以**存储各种格式**，甚至图像和视频。HDFS将严格要求某种文件格式-流行的选择是**avro**和**parquet**，它们具有相对较高的压缩率，这对于存储大型数据集很有用。





* Spark Accumulators
* Spark Broadcast
* spark WebUI

# Spark

Apache Spark是一个在集群上运行的统一计算引擎以及一组并行数据处理软件库

![](images/spark1.png)  

**统一平台**：Spark通过统一计算引擎和利用一套统一的API，支持广泛的数据分析任务，从简单的数据加载，到SQL查询，再到机器学习和流式计算

**计算引擎**:在Spark致力于统一平台的同时，它也专注于计算引擎，Spark从存储系统加载数据并对其执行计算，加载结束时不负责永久存储，也不偏向于使用某一特定的存储系统，主要原因是大多数数据已经存在于混合存储系统
中，而移动这些数据的费用非常高，因此Spark专注于对数据执行计算，而不考虑数据存储于何处

**配套的软件库**：Spark包括SQL和处理结构化数据的库（Spark SQL）、机器学习库（MLlib）、流处理库（Spark Streaming和较新的结构化流式处理），以及图分析（GraphX）的库。除了这些库之外，还有数百种开源外部库，从用于各种存储系统的连接器到机器学习算法。spark-packages.org（https://spark-packages.org/）上提供了一个外部库的索引。



**Spark的基本架构**：Spark管理和协调跨多台计算机的计算任务。Spark用来执行计算任务的若干台机器由像Spark的集群管理器、YARN或Mesos这样的集群管理器管理，然后我们提交Spark应用程序给这些集群管理器，它们将计算资源分配给应用程序，以便完成我们的工作。

**Spark应用程序**：Spark应用程序由一个驱动器进程和一组执行器进程组成。**驱动进程**运行main()函数，位于集群中的一个节点上，它负责三件事：维护Spark应用程序的相关信息；回应用户的程序或输入；分析任务并分发给若干执行器进行处理。**执行器**负责执行驱动器分配给它的实际计算工作，这意味着每个执行器只负责两件事：执行由驱动器分配给它的代码，并将该执行器的计算状态报告给运行驱动器的节点。

![](images/spark2.png)



## SparkSession

```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
```

## DataFrame

DataFrame是最常见的结构化API，简单来说它是包含行和列的数据表。列和列类型的一些规则被称为模式（schema）。与电子表格不同的是：电子表格位于一台计算机上，而Spark DataFrame可以跨越数千台计算机。

我们可以非常容易地将Pandas（Python） DataFrame转换为Spark DataFrame或将R DataFrame转换为Spark DataFrame。

Spark中的DataFrame和Dataset代表不可变的数据集合，可以通过它指定对特定位置数据的操作，该操作将以惰性评估方式执行。当对DataFrame执行动作操作时，将触发Spark执行具体转换操作并返回结果，这些代表了如何操纵行和列来计算出用户期望结果的执行计划

```python
# spark is an existing SparkSession
df = spark.read.json("examples/src/main/resources/people.json")
# Displays the content of the DataFrame to stdout
df.show()
df.printSchema()  #表结构
df.select("name").show()
df.select(df['name'], df['age'] + 1).show()
df.filter(df['age'] > 21).show()   
df.groupBy("age").count().show()

# 创建dataframe
training = spark.createDataFrame([
    (0, "a b c d e spark", 1.0),
    (1, "b d", 0.0),
    (2, "spark f g h", 1.0),
    (3, "hadoop mapreduce", 0.0)
], ["id", "text", "label"])

flightData2015 = spark\
.read\
.option("inferSchema", "true")\
.option("header", "true")\
.csv("/data/flight-data/csv/2015-summary.csv")
```

### Schema

Schema定义了DataFrame的列名和类型，可以手动定义或者从数据源读取模式（通常定义为模式读取）。

### 数据类型

| 数据类型      | Python的值类型                                               | 获取或者创建数据类型的API                                    |
| ------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| ByteType      | int或long。注意：数字在运行时转换为1字节 <br/>的带符号整数。 确保数字在-128~127的范围内 | ByteType()                                                   |
| ShortType     | int或long。注意：数字在运行时将转换为2字 <br/>节带符号的整数。 确保数字在-32768到<br/>32767的范围内 | ShortType()                                                  |
| IntegerType   | int或long。注意：Python对“整数”有一个<br/>宽松的定义。如果使用IntegerType()，那么太<br/>大的数字将被Spark SQL拒绝。在这种情况下，<br/>最好使用LongType() | IntegerType()                                                |
| LongType      | long。注意：数字在运行时将转换为8字节有符 <br/>号整数。确保数字在-9223372036854775808~<br/>9223372036854775807范围内。否则，请将<br/>数据类型转换为decimal.Decimal，并使用<br/>DecimalType | IntegerType()                                                |
| FloatType     | float型。注意：在运行时，数字将被转换为4 <br/>字节的单精度浮点数 | FloatType()                                                  |
| DoubleType    | float型                                                      | DoubleType()                                                 |
| DecimalType   | decimal.Decimal                                              | DecimalType()                                                |
| StringType    | string                                                       | StringType()                                                 |
| BinaryType    | bytearray                                                    | BinaryType()                                                 |
| BooleanType   | Bool                                                         | BooleanType()                                                |
| TimestampType | datetime.datetime                                            | TimestampType()                                              |
| DateType      | datetime.date                                                | DateType()                                                   |
| ArrayType     | List，tuple或array                                           | ArrayType（elementType，<br/>[containsNull]）。注意：<br/>containsNull的默认值为<br/>True |
| MapType       | 字典                                                         | MapType（keyType，<br/>valueType，<br/>[valueContainsNull]）。<br/>注意：valueContainsNull<br/>的默认值为True |
| StructType    | 列表或元组                                                   | StructType（fields）。注<br/>意： fields是一个包含多<br/>个StructFiled的list，并且<br/>任意两个StructField不能<br/>同名 |
| StructField   | 该字段对应的Python数据类型（例如，int是<br/>IntegerType的StructField） | StructField（name，<br/>dataType，[nullable]）。<br/>注意：nullable指定该<br/>field是否可以为空值，默<br/>认值为True |



## 数据分区

为了让多个执行器并行地工作,S p a r k将数据分解成多个数据块,每个数据块叫做一个分区。分区是位于集群中的一台物理机上的多行数据的集合,DataFrame的分区也说明了在执行过程中,数据在集群中的物理分布。如果只有一个
分区,即使拥有数千个执行器,S p a r k也只有一个执行器在处理数据。类似地,如果有多个分区,但只有一个执行器,那么S p a r k仍然只有一个执行器在处理数据,就是因为只有一个计算资源单位
值得注意的是,当使用DataFrame时,(大部分时候)你不需要手动操作分区,只需指定数据的高级转换操作,然后Spark决定此工作如何在集群上执行

## Dataset

Dataset类似于RDD，但是，它们不使用Java序列化或Kryo，而是使用专用的[Encoder](http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Encoder.html)对对象进行序列化以进行处理或通过网络传输。虽然编码器和标准序列化都负责将对象转换为字节，但是编码器是动态生成的代码，并使用一种格式，该格式允许Spark执行许多操作，如过滤，排序和哈希处理，而无需将字节反序列化为对象

dataset只有java和scala接口

## SQL

使用Spark SQL，你可以将任何DataFrame注册为数据表或视图（临时表），并使用纯SQL对它进行查询。编写SQL查询或编写DataFrame代码并不会造成性能差异，它们都会被“编译”成相同的底层执行计划。

```python
# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()
```

## 操作

操作分为**转换操作**和**动作操作**

### 转换操作

要“ 更改”DataFrame，你需要告诉Spark如何修改它以执行你想要的操作，这个过程被称为转换。

如： divisBy2 = myRange.where("number % 2 = 0")

这些转换并没有实际输出，这是因为我们仅指定了一个抽象转换。在我们调用一个动作操作之前，Spark不会真的执行转换操作。

转换操作是使用Spark表达业务逻辑的核心，有两类转换操作：第一类是指定**窄依赖关系**的转换操作，第二类是指定**宽依赖关系**的转换操作。

窄依赖关系（narrow dependency）的转换操作（我们称之为窄转换）是每个输入分区仅决定一个输出分区的转换。在前面的代码片段中，where语句指定了一个窄依赖关系，其中一个分区最多只会对一个输出分区有影响。Spark将自动执行流水线处理，这意味着如果我们在DataFrame上指定了多个过滤操作，它们将全部在内存中执行。

宽依赖关系（wide dependency）的转换（或宽转换）是每个输入分区决定了多个输出分区。这种宽依赖关系的转换经常被称为洗牌（shuffle）操作，它会在整个集群中执行互相交换分区数据的功能。当我们执行shuffle操作时，Spark将结果写入磁盘

### 惰性评估

惰性评估（lazy evaluation）的意思就是等到绝对需要时才执行计算。

一个很好的例子就是DataFrame的谓词下推（predicate pushdown），假设我们构建一个含有多个转换操作的Spark作业，并在最后指定了一个过滤操作，假设这个过滤操作只需要数据源（输入数据）中的某一行数据，则最有效的方法是在最开始仅访问我们需要的单个记录，Spark会通过自动下推这个过滤操作来优化整个物理执行计划。

### 动作操作

转换操作使我们能够建立逻辑转换计划。为了触发计算，我们需要运行一个动作操作 （action）。

有三类动作：

* 在控制台中查看数据的动作。
* 在某个语言中将数据汇集为原生对象的动作。如collect操作
*  写入输出数据源的动作

**可以通过调用explain函数观察到Spark正在创建一个执行计划,并且可以看到这个计划将会怎样在集群上执行,调用某个DataFrame的explain操作会显示DataFrame的来源(即Spark是如何执行查询操作的)**
```python
flightData2015.sort("count").explain()
```

## 全局临时视图

Spark SQL中的临时视图是会话作用域的，如果创建它的会话终止，它将消失。如果要在所有会话之间共享一个临时视图并保持活动状态，直到Spark应用程序终止，则可以创建全局临时视图。全局临时视图与系统保留的`global_temp`数据库相关联，我们必须使用限定名称来引用它，例如`SELECT * FROM global_temp.view1`。

```python
# Register the DataFrame as a global temporary view
df.createGlobalTempView("people")

# Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show()
# Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.people").show()
```



## 标量函数

- [数组函数](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#array-functions)
- [Map功能](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#map-functions)
- [日期和时间戳功能](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#date-and-timestamp-functions)
- [JSON函数](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#json-functions)

## 类聚集函数

- [汇总功能](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#aggregate-functions)
- [视窗功能](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#window-functions)

## UDF（用户定义的函数）

- [标量用户定义函数（UDF）](http://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html)
- [用户定义的聚合函数（UDAF）](http://spark.apache.org/docs/latest/sql-ref-functions-udf-aggregate.html)
- [与Hive UDF / UDAF / UDTF集成](http://spark.apache.org/docs/latest/sql-ref-functions-udf-hive.html)

```python
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)
user_log = user_log.withColumn("hour", get_hour(user_log.ts))
user_log.head()
```

## 配置

默认情况下，shuffle操作会输出200个shuffle分区，我们将此值设置为5以减少shuffle输出分区的数量：

```python
spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2)
```



## 读CSV文件

```python
spark = SparkSession.builder.appName("myproject").getOrCreate()
flightData2015 = spark.read\
	.option("inferSchema", "true").option("header", "true")\
	.csv("/data/flight-data/csv/2015-summary.csv")
df.printSchema()
df.show(5)

staticDataFrame = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/data/retail-data/by-day/*.csv")
```

## spark-submit

spark-submit轻松地将测试级别的交互式程序转化为生产级别的应用程序。sparksubmit将你的应用程序代码发送到一个集群并在那里执行，应用程序将一直运行，直到它（完成任务后）正确退出或遇到错误。你的程序可以在集群管理器的支持下进行，包括Standalone，Mesos和YARN等。

spark-submit提供了若干控制选项，你可以指定应用程序需要的资源，以及应用程序的运行方式和运行参数等

你可以使用Spark支持的任何语言编写应用程序，然后提交它执行

```python
./bin/spark-submit \
--master local \
./examples/src/main/python/pi.py 10
```

## Dataset：类型安全的结构化API

Dataset，用于在Java和Scala中编写静态类型的代码。Dataset API在Python和R中不可用，因为这些语
言是动态类型的。

**暂时跳过**

